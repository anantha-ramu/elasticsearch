# Elasticsearch cluster coordination

Notes from a teaching session, 1–2 Sep 2026. Covers the **Cluster coordination and Discovery plugins**
topic of the ES-Distributed list, and sits on top of `docs/internal/DistributedArchitectureGuide.md`
lines 470–1468 (`# Cluster Coordination`) — which is the best-written section of that guide and worth
reading in full once.

Everything here was checked against source at the cited line. **Nothing was executed** — the *Lab*
section at the end lists what would turn reading into evidence. Line numbers are against Elasticsearch
commit `cd4f46d0e292`; if one looks off, grep for the quoted code.

## If you've forgotten everything

Read *The story in one page*. If that's enough, stop. If not, the twelve parts below are in the order
the story unfolds, each ending in one load-bearing sentence. *Failure scenarios* is where to go when
something is actually broken. *Settings you'll meet* is for when you're reading logs at 3am.

---

## The story in one page

Every node keeps a copy of one document — the **cluster state** — describing which nodes exist, where
every shard lives, and what every index looks like. Those copies must be identical, in the same order,
or two nodes end up both believing they own the same primary and data is lost.

So one node, the **elected master**, makes every change. It computes a new immutable state on a single
thread, batching many small changes into one, then **publishes** it in two phases: first every node
stores it and says "got it" (voting nodes write to disk before answering); only when a **majority of the
voting nodes** have said so does the
master say "now use it." That way, at any instant the master could die, either nobody has acted on the
new state yet, or enough nodes hold it that the next master is guaranteed to find it.

Followers ping the master every second; if it stops answering they become **candidates** and go looking.
The master pings followers; if one stops answering — or accepts states but never applies them — it's
removed and its shards are reallocated.

A candidate runs **discovery**: it asks every master-eligible node it knows about "who's master, who
else is there?", starting from its last cluster state plus configured seed hosts (which the discovery
plugins can supply from a cloud API). If it finds a master it joins. If it finds a quorum but no master,
it holds an **election**: first a **pre-vote** asking peers whether anyone already has a master — so a
flaky node can't disrupt a healthy cluster — then it bumps the **term**, collects votes, and becomes
leader once a majority of the **voting configuration** has voted. A node votes at most once per term, and
a candidate discards votes from anyone with fresher state than itself, so the winner always holds the
newest committed state.

A brand-new cluster has no voting configuration, so an operator names the first voters. A cluster that
has permanently lost its majority needs a deliberately frightening command to shrink the electorate to
one. Serverless swaps the quorum for a compare-and-swap on a blob in the object store. And every
master-bound request finds the master by reading the local state and retrying if it guessed wrong.

Two classes carry the whole thing: `CoordinationState` holds the safety rules (small, formally modelled,
never breaks), and `Coordinator` makes things actually happen (large, full of timers).

---

## 1. Why any of this exists

Without agreement, two nodes can both think they hold the primary for shard 0 and both accept writes.
That's not degraded service; it's divergent histories with no way back. The guide says it plainly at
lines 576–580: updates need linearizability or you get data loss.

The design shape is the familiar one — one leader changes, everyone copies — but Elasticsearch replicates
**whole states** (immutable, version-numbered), not a log of operations as Raft does. The algorithm is
Paxos-derived; the terms, quorum, and one-leader-per-term ideas will feel like Raft anyway.

The lens for everything that follows:

- `CoordinationState` = **safety**. Implements a TLA+ model line for line (`CoordinationState.java:29-33`).
  "Could this produce two masters?" — look here.
- `Coordinator` = **liveness**. Timeouts, retries, elections. "Why is my cluster stuck?" — look here.
- Every node is in one of three modes: `CANDIDATE`, `LEADER`, `FOLLOWER` (`Coordinator.java:1841-1845`).
  Everyone starts as a candidate.

**One sentence:** identical copies or lost data; one master changes, a quorum confirms; safety in
`CoordinationState`, liveness in `Coordinator`.

---

## 2. The cluster state — one immutable document

Open it up (`ClusterState.java:170-196`) and it answers five questions: who's here (`nodes`), where is
everything (`routingTable`), what exists (`metadata` — indices, mappings, settings, and the coordination
bookkeeping: term and voting configuration), what's forbidden (`blocks`), what's in flight (`customs`).

**Half is saved, half is thrown away.** Only `metadata` is persisted. The test: *can you rediscover it?*
Nodes reconnect, shards get re-found on disk, a running snapshot isn't running after a full restart — all
rebuildable. Which indices exist, with what mappings, and what term we're in — lose those and the shard
files are anonymous and a restarted node could vote twice. So they're saved. Note `transientSettings` sits
right beside `persistentSettings` in `Metadata` (`Metadata.java:239-240`) and is *not* saved: same class,
different lifetime, chosen per field.

**Where it goes:** a small Lucene index under each data path's `_state` directory. Three document types —
one `global`, one `index` per index, one `mapping` per *distinct* mapping (deduplicated by hash, so 300
data-stream backing indices share one) — and the current term plus last accepted version in the Lucene
**commit user data** (`PersistedClusterStateService.java:125-136`). Same commit-user-data mechanism you
found `history_uuid` in during the shard labs.

**Two identifiers, two jobs.** `version` goes up by one per *committed* state and is what "newer" means.
`stateUUID` is random and changes on *every* state, committed or not; its only job is to make sure a diff
is applied to the right base (`ClusterState.java:310-316`). `incrementVersion()` forgets the UUID so
`build()` must mint a new one (lines 1175–1179, 1210–1212) — you can't accidentally reuse an identity.

**One sentence:** one immutable document per version; only the "what exists" half is written to disk, into
a small Lucene index, because only it can't be rediscovered.

---

## 3. Roles — who gets a vote

`node.roles` is a node's job description. For this topic only two matter: **`master`** makes a node
master-eligible (can vote, can be elected); **`node.roles: []`** makes a coordinating-only node (receives
client requests, no data, no vote).

**Only master-eligible nodes count.** When later parts say "a quorum," the population is master-eligible
nodes only. Data nodes receive and apply every state, but their acknowledgement counts toward nothing.
That keeps the fault-tolerance arithmetic fixed: three dedicated masters means you need two and can lose
one, and nothing you do to the data tier changes it.

**`voting_only`** gives you a third vote without a third full master. It must be combined with `master`
(`DiscoveryNodeRole.java:228-232`), so it's in the voting configuration and its vote counts — it just never
ends up leading. The enforcement is characteristically indirect: the x-pack election strategy stands it
down if a full master with equally fresh state has voted (`VotingOnlyNodePlugin.java:132-143`), and if it
somehow wins anyway it publishes its state to the full masters *so they catch up*, then fails the
publication before committing and steps aside (`PublicationTransportHandler.java:393-396`). A doomed
leadership used to make sure nobody loses data. Term 0 is the exception, to avoid deadlock if only
voting-only nodes were bootstrapped.

**One sentence:** only master-eligible nodes vote; the production shape is three dedicated masters, and
`voting_only` is a cheap tiebreaker that votes but steps aside rather than lead.

---

## 4. `MasterService` — one thread computes every new state

Every change — index created, shard started, node joined — is computed on one thread,
`masterService#updateTask`, min 0 max 1 (`MasterService.java:285-296`). One thread makes "take the
current state, produce the next" trivially consistent: there's never a stale base. The cost is that
everything the master does is a queue. (Distinct from the `cluster_coordination` pool in the networking
notes — that's where the `Coordinator` handles protocol messages. Two single threads, two jobs.)

**Six priorities** (`Priority.java:23-46`): `IMMEDIATE` … `LANGUID`, drained highest first.

**Batching is what makes one thread survivable.** Publication costs hundreds of milliseconds. So
producers create a named queue with a priority and a `ClusterStateTaskExecutor`
(`createTaskQueue`, lines 1724–1735), and the executor's contract is: take a *batch* of tasks, return
*one* new state (`ClusterStateTaskExecutor.java:45`). 500 "shard started" events → one new routing table →
one publication. The busier the master, the bigger the batches. Self-regulating.

**The computation is a pure function, guarded three ways** (`executeAndPublishBatch`, lines 335–368):
before — still master? else fail every task with `NotMasterException`; during — executor throws? state
unchanged, every task's `onFailure` called; after — executor returned the *same object* (`==`)? skip
publication entirely. The rule this implies, for any code you ever write here: **the executor must have no
side effects**, because the publication might fail and the state be discarded. Side effects go in
listeners, after commit.

Each batch is a task (`publish_cluster_state_update`, line 110) — `GET _tasks?actions=publish_cluster_state_update`
on the master shows updates in flight.

**One sentence:** one thread, priority order, batches that each yield one immutable state; pure computation
guarded by "still master?" before and "anything changed?" after.

---

## 5. Publication — the two-phase commit

The master has state 42; everyone's on 41. The design question at every instant: *if I die right here, is
anything broken?*

**Phase 1, accept.** Send 42 to everyone: *write it to disk, don't use it yet.* Each node persists it as
"last accepted" before replying (`CoordinationState.handlePublishRequest`, write at line 405). Master
counts replies from master-eligible nodes until it has a strict majority (`isPublishQuorum`, line 112).

**Phase 2, commit.** *A majority has it — use it.* Each node marks it committed (`handleCommit`, line 517),
applies it (Part 6), acks. Master applies its own copy last.

**Why this survives death anywhere.** Before quorum: nobody has *applied* 42; it's harmlessly forgotten or
carried forward. After quorum, before commit: the next master must win a majority, any two majorities
overlap, and Part 9 makes the winner at least as fresh as anyone in its majority — so 42 survives.
Mid-commit: same. The invariant: **a node never applies a state that could later turn out not to exist.**

**No quorum → the master steps down** (`FailedToCommitClusterStateException`; `becomeCandidate` at
`Coordinator.java:1716, 1739, 1766`). A master that can't get a majority to take its states would only block
a real election. `cluster.publish.timeout` is 30s (line 131); at 10s you get the "publication took…" info
log (line 123). After commit is *sent*, the master waits politely for all acks but needs no quorum — slow
followers just miss this one.

**Diffs.** Nodes that had 41 get a diff carrying 41's `stateUUID`; new nodes get the full state. Base
mismatch → `IncompatibleClusterStateVersionException` → master resends full
(`PublicationTransportHandler.java:466-470`). Nothing to configure; a mismatch is just the slow path.

**One sentence:** send it, wait for a majority to write it to disk, then say "use it" — so at every moment
of possible death, either nobody has applied it or the next master is guaranteed to know about it.

---

## 6. Application — making the node match the state

State 42 arrives at a data node saying "shard 3 of `logs` lives on you." Right now that's false. Two kinds
of code care, for opposite reasons.

**Appliers make it true** — create the directory, open the Lucene index, start recovery.
`IndicesClusterStateService` is the big one. Three priority tiers (`ClusterApplierService.java:106-108`).

**Listeners react to it being true** — a metric updates, `PersistentTasksNodeService` notices it's been
assigned a job.

The node keeps a pointer to "the current state" that everyone reads. It flips from 41 to 42 *after* the
appliers (so nobody reads "shard 3 is here" and goes looking before it exists) and *before* the listeners
(which react to something now official). The full order, `applyChanges` at lines 538–572: connect to new
nodes (blocking, so appliers can reach them) → apply settings → appliers → disconnect from departed nodes
→ `state.set(new)` → listeners.

The bug class: an applier that calls `clusterService.state()` gets the **old** state — the javadoc says
so (`ClusterStateApplier.java:15-16`). Use the event you were handed. Writing new code? You almost always
want a listener.

**Slow appliers hurt everyone.** The commit ack waits for application; the master waits for acks. One node's
20-second applier delays the whole cluster's next state. Hence `cluster.service.slow_task_logging_threshold`
at 30s (lines 65–69) and the per-step stopwatch in that method.

**One sentence:** appliers make reality match the state, then the state becomes visible, then listeners
react — and because the master waits for every ack, a slow applier anywhere slows the cluster.

---

## 7. Persistence — when bytes hit disk

Three writes, all in `CoordinationState`: joining a new term (`handleStartJoin`, line 195), accepting a
published state (line 405), committing it (line 517). The second is the one the safety argument rests on:
the master counts that node's "got it" toward quorum, so the write must finish *before* the reply leaves.

**Who pays for that synchronously** is decided by role at startup (`GatewayMetaState.java:198-206`).
Master-eligible → `LucenePersistedState`, blocking on the coordination thread. Data-only →
`AsyncPersistedState`: memory now, disk on a background thread. Safe because a data node's reply counts
toward nothing. Coordinating-only (neither) → in-memory, writes **nothing** (line 147–160); restart it and
it starts from zero and asks the master.

Writes are incremental: new mappings added, unused ones deleted, an `index` doc rewritten only if that
index changed, `global` rewritten whole. Term and version go in the commit user data because they change
every time and a commit is happening anyway.

**One sentence:** write on join-term, accept, and commit; only master-eligible nodes do the accept-write
synchronously, because only their reply counts toward quorum.

---

## 8. Failure detection — three detectors

You documented the two checkers in the networking notes (1s interval, 10s timeout, 3 retries, PING
channel). Their meaning:

**Follower loses master → search.** `LeaderChecker` gives up (3 failures, master unhealthy, or
disconnected — `LeaderChecker.java:383-386`) → `Coordinator.onLeaderFailure` → `becomeCandidate`
(lines 387–398). The node blocks writes locally and starts discovery. It doesn't fire the master; it goes
looking. The master, for its part, refuses leader checks from nodes not in its cluster state
(`LeaderChecker.java:186-190`) — that's how a removed node learns it's out.

**Master loses follower → verdict.** `FollowersChecker` gives up for the same three reasons
(`FollowersChecker.java:323-330`) → `removeNode` → `node-left` queue at `IMMEDIATE` priority
(`Coordinator.java:312, 402-409`). New state without that node, published; its shards go unassigned and
reallocation begins.

**The follower check carries the term.** A check with a *higher* term means an election happened while you
weren't looking; the follower adopts the term and follows the sender (`onFollowerCheckRequest`, lines
411–437). The heartbeat doubles as the "there's a new boss" announcement.

**The third detector: lag.** A node that accepts states but never applies them is alive, pingable, and
running on an old map. `LagDetector` removes it after `cluster.follower_lag.timeout` (90s,
`LagDetector.java:50-63`) with reason `"lagging"` (`Coordinator.java:329`). Same removal path.

**One sentence:** can't reach master → search for a new one; can't reach follower → remove it; follower
can't keep up → remove it too.

---

## 9. Terms, quorum, voting configuration — the safety core

**Terms** are numbered mandates, a `long` that only goes up. A node votes at most once per term:
`handleStartJoin` rejects any term not *strictly greater* than what it knows (`CoordinationState.java:170`)
and persists the new term before voting (line 195). Two nodes *can* both believe they're master at the same
wall-clock moment — one in term 7 who hasn't heard, one in term 8 — but only the term-8 master can get a
quorum to accept anything. The other publishes, gets rejected, and steps down.

**Quorum** is a strict majority of an explicit list of node IDs, the **voting configuration**, stored in
the cluster state (`CoordinationMetadata.hasQuorum`, lines 326–334: `votedNodesCount * 2 > nodeIds.size()`).
Votes from nodes not on the list count for nothing. Three → need two. Four → still need three.

**Changing the list.** The `Reconfigurator` adjusts it as masters join and leave; its javadoc
(`Reconfigurator.java:34-47`) explains the dilemma and the answer: auto-shrink (default on) so that
losing one of five lets you then lose another — but **never below three**, because a two- or one-node
configuration means one failure takes the cluster down and no amount of restoring the others fixes it.
During a change there are two lists, and an election must win a majority of **both**
(`ElectionStrategy.java:79`) so two candidates can't each win under a different list.

**The winner is the freshest.** Each vote carries the voter's last accepted term and version. A candidate
*rejects* votes from anyone fresher than itself (`handleJoin`, lines 236–267). So it can only win with
votes from nodes at or behind it — meaning it holds the newest state in its majority — and combined with
"any two majorities overlap," an accepted state can't be lost across a master change. The code comment
notes the inversion: in a real election the voter is choosy; here the candidate is.

**One sentence:** vote once per term; quorum is a majority of an explicit list that shrinks but never below
three; a candidate discards fresher votes, so the winner holds the newest state.

---

## 10. Discovery — finding the others

A candidate starts from two lists: the master-eligible nodes in its last in-memory state, and the **seed
hosts** from `discovery.seed_hosts` (read once), `unicast_hosts.txt` (re-read each round — preferred), and
any **`DiscoveryPlugin`**. A plugin contributes exactly one thing here: another `SeedHostsProvider`
(`DiscoveryPlugin.java:64`). The EC2 one calls `describeInstances` filtered by your tags/groups
(`AwsEc2SeedHostsProvider.java:110`); GCE and Azure-classic do the same against their clouds. That's the
entire "Discovery plugins" story — a cloud query instead of a static IP list.

**The loop.** `PeerFinder` wakes every second (`discovery.find_peers_interval`, `PeerFinder.java:58-60`),
drops unreachable peers, and asks every known master-eligible node "who are you, are you master, who else
do you know?" Every node named in a reply gets probed too, and so does anyone who probes *you*. Gossip
flood; converges in roughly `⌈log₂(D)+1⌉` rounds.

**Two exits.** A peer says "I'm master" with a term at least yours → adopt the term, send a join, become a
follower (`onActiveMasterFound`, lines 257, 556). Enough peers but no master → election
(`onFoundPeersUpdated`, line 266).

Discovery connections are separate from the cluster's real connections and are dropped once a master is
found.

**One sentence:** a masterless node gossips "who's master, who else is there?" from its last state plus
seed hosts — which plugins can supply from a cloud API — until it finds a master to join or a quorum to
elect one.

---

## 11. The election — four messages

**Step 0, random delay.** So simultaneous candidates don't split the vote forever: up to 100ms, growing
100ms per failure, capped at 10s (`ElectionSchedulerFactory.java:62-80`).

**Step 1, pre-vote.** *Would you vote for me?* — a question that changes nothing. Receiver says yes only if
it has no master, or if the asker *is* its master (`StatefulPreVoteCollector.java:104-118`). The failure
this prevents: a node with a flaky cable loses its master, bumps its term to 8, cable comes back, and it
shouts "term 8!" at a healthy term-7 cluster, forcing a pointless election every time the cable flaps. With
pre-vote, four peers say "we have a master," it never bumps, and rejoins quietly. The guide links a
real-world Cloudflare outage from skipping this.

**Step 2, start-join.** Pre-vote quorum in hand, pick a term one higher than any seen
(`Coordinator.java:585`), broadcast `StartJoinRequest`. Every receiver runs Part 7's first write:
*I am now in term 8.*

**Step 3, join.** Each node sends a `JoinRequest` carrying its last accepted term/version. Candidate applies
Part 9's freshness rule; on quorum of the voting configuration(s), `electionWon` flips
(`CoordinationState.java:284`).

**Step 4, leader.** `becomeLeader` (`Coordinator.java:1021`): flush accumulated joins into one `JoinTask`
adding every voter, publish it — the first publication in term 8 is how everyone learns who won — and
start the `FollowersChecker`.

**A join is two things.** During an election it's a vote. Outside one, it's a membership request from a
node that found an existing master. Same message; the receiver's mode decides which.

**One sentence:** random delay, ask if anyone has a master, and only if a quorum says no: bump the term,
collect votes from nodes no fresher than you, become leader by publishing the state that adds them.

---

## 12. The edges

**First master.** Empty disks mean `EMPTY_CONFIG` — no electorate, so no election. The operator names it:
`cluster.initial_master_nodes` (`ClusterBootstrapService.java:49-52`). Once a strict majority of those
names are *discovered*, their IDs become the voting configuration, unfound names get placeholder entries
that count toward size but can't vote (line 61), and an election is scheduled. Read once, ever — remove
the setting afterwards. `discovery.type: single-node` bootstraps the local node instantly (what
`./gradlew run` does). Configure nothing at all and it waits `discovery.unconfigured_bootstrap_timeout`
(3s, lines 54–57) then bootstraps with whatever it found — dev only; two such nodes that don't see each
other form two clusters.

**Lost the majority for good.** `elasticsearch-node unsafe-bootstrap` on one survivor sets both voting
configurations to just itself and mints a new cluster UUID; anything the lost nodes committed that this one
never saw is gone. `elasticsearch-node detach-cluster` on the others sets their configuration to
`MUST_JOIN_ELECTED_MASTER` — a list containing one impossible node ID (`CoordinationMetadata.java:307-309`)
— so they can never form a majority and can only join whoever is master; term reset to 0, UUID
un-committed. If you're reaching for these, you should be restoring a snapshot.

**Serverless.** Same `Coordinator`, same `CoordinationState`, two strategies swapped. Liveness: the master
writes a heartbeat blob every 15s (`cluster.stateless.heartbeat_frequency`), candidates read it, two misses
means dead (`StoreHeartbeatService.java:31-46`). Quorum: the voting configuration is always just the
current master (`SingleNodeReconfigurator`), and two candidates racing for a term are settled by a
compare-and-swap on the term in the blob. The referee moved from "majority of nodes" to "one atomic
register."

**Finding the master.** `TransportMasterNodeAction` (javadoc at lines 55–99): read local state for who's
master, forward, on `NotMasterException` wait for the local state to show a new master and retry up to
`master_timeout`. (The implicit 30s default is deprecated for removal — `MasterNodeRequest.java:41` calls
it `TRAPPY_IMPLICIT_DEFAULT_MASTER_NODE_TIMEOUT`; REST callers are meant to pass `?master_timeout`
explicitly.) The forwarded request carries the routing `masterTerm`; a node with a lower term
knows it's stale and waits rather than bouncing the request. Terms doing routing duty.

**One sentence:** an operator names the first voters; a lost majority needs a scary command to shrink the
electorate to one; serverless replaces the quorum with a CAS on a blob; requests route by local state and
retry on `NotMasterException`.

---

## Failure scenarios

**Master partitioned from a minority, majority intact.** The majority's `LeaderChecker`s fail → they
become candidates → discovery finds each other → pre-vote passes (nobody has a master) → election in term
+1 → new master. The old master, on the minority side, can't get a publish quorum → steps down → becomes a
candidate that can't find a quorum → sits with a no-master block. When the partition heals it discovers
the new master and joins as a follower. Anything it "committed" alone was never a quorum and is discarded.

**Master partitioned with the majority, minority cut off.** Nothing happens on the majority side except the
`FollowersChecker` removing the cut-off nodes after 3 failures and reallocating their shards. The minority
nodes become candidates, fail pre-vote quorum (not enough peers), and wait. On heal they rediscover the
master and rejoin.

**Two nodes both believe they're master.** Possible for a moment across terms (7 and 8), never within one.
The term-7 master's next publication is rejected by nodes that moved to 8 → no quorum → it steps down.
Terms are why this is safe; Part 9.

**A follower stops applying but stays reachable.** Pings succeed, so neither checker fires. `LagDetector`
removes it after 90s with reason `lagging`. Search it before then and you get answers from an old map.

**A master-eligible node's disk is slow.** Its accept-write is synchronous (Part 7), so its "got it" reply
is late, so publication is slow for everyone. Watch for the 10s publication info log. A data node's slow
disk doesn't have this effect — its write is async and its reply doesn't count.

**Lost two of three masters permanently.** Stuck, correctly. Restore from snapshot if you can; otherwise
`unsafe-bootstrap` on the survivor and `detach-cluster` on everyone else, accepting whatever state the
survivor had.

**Only the voting-only node survives an election with stale full masters.** It wins, publishes its state so
the full masters catch up, fails the publication before commit, steps down; a now-fresh full master wins
the re-election. Deliberate (`PublicationTransportHandler.java:393-396`).

---

## Counterintuitive findings

1. **States, not a log.** Each state is a whole immutable object; nodes receive diffs of it, not commands.
2. **Only `metadata` survives a restart.** Node list, routing table, blocks — rebuilt from scratch.
3. **The persisted state is a Lucene index**, with the term in the commit user data.
4. **Two identifiers.** `version` for "newer," `stateUUID` only for "is this diff's base mine?"
5. **`transientSettings` and `persistentSettings` are neighbours in the same class** and one is never saved.
6. **Data nodes don't count.** Their accept of a state counts toward nothing; only master-eligible replies do.
7. **A data node's accept-write is async.** Master-eligible nodes block; nobody else needs to.
8. **Coordinating-only nodes persist nothing at all.**
9. **The executor must be pure.** Side effects during state computation describe a world the cluster may
   never agree to.
10. **Returning the same state skips publication** — and can serve a stale read across a master failover
    (javadoc admits it, `ClusterStateTaskExecutor.java:36-40`).
11. **Appliers see the old state via `clusterService.state()`.** Use the event.
12. **Publish needs a quorum; commit-ack doesn't.** Slow followers miss the boat and get caught by `LagDetector`.
13. **A master that can't get a quorum steps down** rather than waiting.
14. **Two masters can coexist briefly across terms.** Never within one; only the higher can commit.
15. **The candidate rejects votes, not the voter.** Inverted from real elections; it's the one that can enforce freshness.
16. **Auto-shrink never goes below three.** A two-node electorate is *less* resilient than three.
17. **Pre-vote is a question that changes nothing** — and skipping it has caused real outages.
18. **The first publication in a new term is the election announcement.** No separate "I won" message.
19. **A join is a vote or a membership request** depending only on whether the receiver is a candidate.
20. **`voting_only` is enforced by stepping down, not by refusal** — a doomed leadership used to sync state.
21. **`MUST_JOIN_ELECTED_MASTER` is a voting configuration containing an impossible node**, so the node can
    never win and must follow.
22. **Discovery plugins only supply seed addresses.** Everything after that is identical.
23. **Serverless quorum is one node plus a CAS on a blob.**
24. **Terms also route requests** (`masterTerm` on `TransportMasterNodeAction`).

---

## Settings you'll meet in logs

| setting | default | what it means when you see it |
| --- | --- | --- |
| `cluster.publish.timeout` | 30s | publication failed → master stepped down |
| `cluster.publish.info_timeout` | 10s | "publication of cluster state took…" — slow disk or slow applier somewhere |
| `cluster.service.slow_task_logging_threshold` | 30s | an applier on this node was slow; stopwatch shows which step |
| `cluster.service.slow_master_task_logging_threshold` | 10s | a state computation on the master was slow |
| `cluster.follower_lag.timeout` | 90s | node removed with reason `lagging` |
| `cluster.election.initial_timeout` / `back_off_time` / `max_timeout` | 100ms / 100ms / 10s | election randomised backoff |
| `cluster.auto_shrink_voting_configuration` | true | voting config follows master count, floor of 3 |
| `discovery.find_peers_interval` | 1s | discovery round frequency |
| `cluster.initial_master_nodes` | — | read once at bootstrap; remove afterwards |
| `discovery.unconfigured_bootstrap_timeout` | 3s | dev-only auto-bootstrap |
| `cluster.stateless.heartbeat_frequency` / `max_missed_heartbeats` | 15s / 2 | serverless liveness |

---

## ES file index

| File | Why |
| --- | --- |
| `docs/internal/DistributedArchitectureGuide.md` | `# Cluster Coordination`, lines 470–1468 — read it |
| `server/.../cluster/coordination/CoordinationState.java` | safety; TLA+ javadoc 29–33; `handleStartJoin` 169–197; `handleJoin` freshness 220–284; publish/commit 377–517 |
| `server/.../cluster/coordination/Coordinator.java` | liveness; modes 1841; timeouts 123–131; `onLeaderFailure` 387; `removeNode` 402; `onFollowerCheckRequest` 411; `getTermForNewElection` 583; `becomeLeader` 1021; step-down on publish failure 1716/1739/1766 |
| `server/.../cluster/ClusterState.java` | fields 170–196; version/uuid javadoc 166–175, 310–316; builder 1175–1212 |
| `server/.../cluster/metadata/Metadata.java` | persisted fields 232–240 |
| `server/.../gateway/PersistedClusterStateService.java` | doc types and commit user data 125–136; `_state` dir 172 |
| `server/.../gateway/GatewayMetaState.java` | sync vs async by role 147–206; `AsyncPersistedState` 391; `LucenePersistedState` 533 |
| `server/.../cluster/node/DiscoveryNodeRole.java` | `voting_only` 221–235 |
| `x-pack/plugin/voting-only-node/.../VotingOnlyNodePlugin.java` | stand-down rule 132–143 |
| `server/.../cluster/service/MasterService.java` | thread 108, 285–296; batch loop 335–368; `createTaskQueue` 1724; task name 110 |
| `server/.../cluster/ClusterStateTaskExecutor.java` | contract and stale-read caveat 24–45 |
| `server/.../common/Priority.java` | six levels 23–46 |
| `server/.../cluster/coordination/PublicationTransportHandler.java` | diff/full retry 466–470; voting-only fail-before-commit 393–396 |
| `server/.../cluster/coordination/Publication.java` | quorum failure 166 |
| `server/.../cluster/service/ClusterApplierService.java` | `applyChanges` 538–572; tiers 106–108; slow threshold 65 |
| `server/.../cluster/ClusterStateApplier.java` | "called before state is visible" 15–16 |
| `server/.../cluster/coordination/LeaderChecker.java` | master rejects checks from removed nodes 176–194; leader failure 383–386 |
| `server/.../cluster/coordination/FollowersChecker.java` | follower failure reasons 323–330 |
| `server/.../cluster/coordination/LagDetector.java` | why it exists 50–52; timeout 59–63 |
| `server/.../cluster/coordination/CoordinationMetadata.java` | `hasQuorum` 326–334; `MUST_JOIN_ELECTED_MASTER` 307 |
| `server/.../cluster/coordination/ElectionStrategy.java` | both-configs rule 79 |
| `server/.../cluster/coordination/Reconfigurator.java` | auto-shrink rationale 34–53 |
| `server/.../discovery/PeerFinder.java` | interval 58; exits 257, 266, 556 |
| `server/.../discovery/{Settings,File}BasedSeedHostsProvider.java` | `discovery.seed_hosts` 34; `unicast_hosts.txt` 40 |
| `server/.../plugins/DiscoveryPlugin.java` | `getSeedHostProviders` 64 |
| `plugins/discovery-ec2/.../AwsEc2SeedHostsProvider.java` | `describeInstances` 110 |
| `server/.../cluster/coordination/ElectionSchedulerFactory.java` | backoff settings 43–88 |
| `server/.../cluster/coordination/StatefulPreVoteCollector.java` | pre-vote rule 89–118 |
| `server/.../cluster/coordination/ClusterBootstrapService.java` | settings 49–61 |
| `server/.../cluster/coordination/stateless/StoreHeartbeatService.java` | heartbeat settings 31–46 |
| `server/.../action/support/master/TransportMasterNodeAction.java` | routing/retry javadoc 55–99 |
| `server/.../action/support/master/MasterNodeRequest.java` | deprecated implicit 30s `master_timeout` 41 |

---

## Lab — turning reading into evidence

Three-node cluster: `./gradlew run --nodes=3 -Dtests.es.xpack.security.enabled=false`. In rough order of
value:

1. **Read the persisted state.** Stop a node; `strings` on `segments_N` under its `_state/` directory;
   find `current_term` and `last_accepted_version`. Compare with `GET _cluster/state?filter_path=version,metadata.cluster_coordination`
   on a live node. (Part 2, 7.)
2. **Watch a publication.** `PUT _cluster/settings` with `logger.org.elasticsearch.cluster.coordination: TRACE`
   on all nodes; create an index; read the publish → accept → commit → apply sequence and the version
   bump. (Part 5.)
3. **See the voting configuration.** `GET _cluster/state?filter_path=metadata.cluster_coordination` —
   note `last_committed_config` has three IDs. Stop one node; watch it *not* shrink below three. Start a
   fourth master-eligible node, then stop one; watch it shrink from four to three. (Part 9.)
4. **Cause an election.** Kill the master (or `POST _cluster/voting_config_exclusions`); watch the
   `LeaderChecker` failures, pre-vote, term bump, and the new master's first publication in the logs.
   Confirm `GET _cluster/state` shows the new term. (Parts 8, 11.)
5. **Trigger the lag detector.** Pause a data node's JVM (`kill -STOP`), wait 90s, `kill -CONT`; look for
   `node-left … reason: lagging`. (Part 8.)
6. **Break pre-vote's assumption, safely.** With three masters, partition one using `iptables` or a
   firewall rule; observe it become a candidate and fail pre-vote quorum; heal it; observe it rejoin the
   *same* term as a follower. (Part 11.)
7. **`master_timeout` in action.** Kill the master and immediately `PUT /idx?master_timeout=5s`; observe
   the retry-then-`MasterNotDiscoveredException` behaviour. (Part 12.)

---

## Not covered

- **Allocation** — what happens to shards after `node-left`. Next topic.
- **Join validation** internals (`JoinValidationService`), cluster UUID handling on join.
- **`ClusterStateObserver`** and how actions wait for state changes.
- **Voting config exclusions API** in depth (`POST _cluster/voting_config_exclusions`).
- **Stateless internals** beyond the two swapped strategies — Stateless topic, done last.

---

## Appendix: code quotes, keyed by part

For self-verification when line numbers drift. Each is the exact text at the cited line on
`cd4f46d0e292`; grep for a distinctive phrase if the number no longer matches.

### Part 1

```java
// CoordinationState.java:29-33
/**
 * The core class of the cluster state coordination algorithm, directly implementing the
 * <a href="https://github.com/elastic/elasticsearch-formal-models/blob/master/ZenWithTerms/tla/ZenWithTerms.tla">formal model</a>
 */
public class CoordinationState {
```

```java
// Coordinator.java:1841-1845
public enum Mode {
    CANDIDATE,
    LEADER,
    FOLLOWER
}
```

### Part 2

```java
// ClusterState.java:166-175
/**
 * Monotonically increasing on (and therefore uniquely identifies) <i>committed</i> states. However sometimes a state is created/applied
 * without committing it, for instance to add a {@link NoMasterBlockService#getNoMasterBlock}.
 */
private final long version;

/**
 * Uniquely identifies this state, even if the state is not committed.
 */
private final String stateUUID;
```

```java
// ClusterState.java:1175-1179
public Builder incrementVersion() {
    this.version = version + 1;
    this.uuid = UNKNOWN_UUID;
    return this;
}
```

```text
// PersistedClusterStateService.java:125-127 and 135-136 (class javadoc tables)
 * | GLOBAL_TYPE_NAME  == "global"  | (none)            | Global metadata                              | large docs are       |
 * | INDEX_TYPE_NAME   == "index"   | "index_uuid"      | Index metadata                               | split into pages     |
 * | MAPPING_TYPE_NAME == "mapping" | "mapping_hash"    | Mapping metadata                             |                      |
 * | CURRENT_TERM_KEY          | "current_term"          | Node's "current" term (≥ last-accepted term and the terms of all sent joins)  |
 * | LAST_ACCEPTED_VERSION_KEY | "last_accepted_version" | The cluster state version corresponding with the persisted metadata           |
```

### Part 3

```java
// DiscoveryNodeRole.java:228-232
@Override
public void validateRoles(final List<DiscoveryNodeRole> roles) {
    if (roles.contains(MASTER_ROLE) == false) {
        throw new IllegalArgumentException("voting-only node must be master-eligible");
    }
}
```

```java
// PublicationTransportHandler.java:393-396
if (isVotingOnlyNode) {
    // Voting-only nodes publish their cluster state to other nodes in order to freshen the state held
    // on other full master nodes, but then fail the publication before committing. However there's no
    // need to freshen our local state so we can fail right away.
```

### Part 4

```java
// MasterService.java:285-296
protected ExecutorService createThreadPoolExecutor() {
    return EsExecutors.newScaling(
        nodeName + "/" + MASTER_UPDATE_THREAD_NAME,
        0,
        1,
        60,
        TimeUnit.SECONDS,
        true,
        daemonThreadFactory(nodeName, MASTER_UPDATE_THREAD_NAME),
        threadPool.getThreadContext()
    );
}
```

```java
// MasterService.java:350-357 and 368
if (previousClusterState.nodes().isLocalNodeElectedMaster() == false && executor.runOnlyOnMaster()) {
    logger.debug("failing [{}]: local node is no longer master", summary);
    for (ExecutionResult<T> executionResult : executionResults) {
        executionResult.onBatchFailure(new NotMasterException("no longer master"));
        executionResult.notifyFailure();
    }
    listener.onResponse(null);
    return;
}
// ...
if (previousClusterState == newClusterState) {
```

```java
// ClusterStateTaskExecutor.java:24-29
/**
 * Update the cluster state based on the current state and the given tasks. Return {@code batchExecutionContext.initialState()} to avoid
 * publishing any update.
 * <p>
 * If this method throws an exception then the cluster state is unchanged and every task's {@link ClusterStateTaskListener#onFailure}
 * method is called.
```

### Part 5

```java
// Coordinator.java:131-133
public static final Setting<TimeValue> PUBLISH_TIMEOUT_SETTING = Setting.timeSetting(
    "cluster.publish.timeout",
    TimeValue.timeValueMillis(30000),
```

```java
// Publication.java:166
Exception e = new FailedToCommitClusterStateException("non-failed nodes do not form a quorum");
```

```java
// PublicationTransportHandler.java:466-468
sendClusterState(connection, bytes, ActionListener.runAfter(listener.delegateResponse((delegate, e) -> {
    if (e instanceof final TransportException transportException) {
        if (transportException.unwrapCause() instanceof IncompatibleClusterStateVersionException) {
```

### Part 6

```java
// ClusterApplierService.java:538-572 (key lines)
private void applyChanges(ClusterState previousClusterState, ClusterState newClusterState, String source, Recorder stopWatch) {
    ClusterChangedEvent clusterChangedEvent = new ClusterChangedEvent(source, newClusterState, previousClusterState);
    // ...
        connectToNodesAndWait(newClusterState);
    // ...
            clusterSettings.applySettings(incomingSettings);
    // ...
    callClusterStateAppliers(clusterChangedEvent, stopWatch);

    nodeConnectionsService.disconnectFromNodesExcept(newClusterState.nodes());

    logger.debug("set locally applied cluster state to version {}", newClusterState.version());
    state.set(newClusterState);

    callClusterStateListeners(clusterChangedEvent, stopWatch);
}
```

```java
// ClusterStateApplier.java:15-16
 * A component that is in charge of applying an incoming cluster state to the node internal data structures. The {@link #applyClusterState}
 * method is called before the cluster state becomes visible via {@link ClusterService#state()}. See also {@link ClusterStateListener}.
```

### Part 7

```java
// GatewayMetaState.java:198-206
if (DiscoveryNode.isMasterNode(settings)) {
    persistedState = new LucenePersistedState(persistedClusterStateService, currentTerm, clusterState);
} else {
    persistedState = new AsyncPersistedState(
        settings,
        transportService.getThreadPool(),
        new LucenePersistedState(persistedClusterStateService, currentTerm, clusterState)
    );
}
```

```java
// GatewayMetaState.java:147 and 160
if (DiscoveryNode.isMasterNode(settings) || DiscoveryNode.canContainData(settings)) {
    return createOnDiskPersistedState(
// ...
return createInMemoryPersistedState(
```

```java
// CoordinationState.java:195
persistedState.setCurrentTerm(startJoinRequest.getTerm());
```

### Part 8

```java
// Coordinator.java:387-398 (start and end)
private void onLeaderFailure(Supplier<String> message, Exception e) {
    synchronized (mutex) {
        if (mode != Mode.CANDIDATE) {
        // ...
        becomeCandidate("onLeaderFailure");
    }
}
```

```java
// Coordinator.java:402-409
private void removeNode(DiscoveryNode discoveryNode, String reason) {
    synchronized (mutex) {
        if (mode == Mode.LEADER) {
            var task = new NodeLeftExecutor.Task(discoveryNode, reason, () -> joinReasonService.onNodeRemoved(discoveryNode, reason));
            nodeLeftQueue.submitTask("node-left", task, null);
        }
    }
}
```

```java
// LeaderChecker.java:186-190
} else if (discoveryNodes.nodeExists(request.getSender()) == false) {
    logger.debug("rejecting leader check from removed node: {}", request);
    throw new CoordinationStateRejectedException(
        "rejecting check since [" + request.getSender().descriptionWithoutAttributes() + "] has been removed from the cluster"
    );
```

```java
// LagDetector.java:50-52 and 59-62
 * A publication can succeed and complete before all nodes have applied the published state and acknowledged it; however we need every node
 * eventually either to apply the published state (or a later state) or be removed from the cluster. This component achieves this by
 * removing any lagging nodes from the cluster after a timeout.
public static final Setting<TimeValue> CLUSTER_FOLLOWER_LAG_TIMEOUT_SETTING = Setting.timeSetting(
    "cluster.follower_lag.timeout",
    TimeValue.timeValueMillis(90000),
```

### Part 9

```java
// CoordinationMetadata.java:326-334
public boolean hasQuorum(Collection<String> votes) {
    int votedNodesCount = 0;
    for (String nodeId : nodeIds) {
        if (votes.contains(nodeId)) {
            votedNodesCount++;
        }
    }
    return votedNodesCount * 2 > nodeIds.size();
}
```

```java
// ElectionStrategy.java:79
return voteCollection.isQuorum(lastCommittedConfiguration) && voteCollection.isQuorum(latestPublishedConfiguration);
```

```java
// CoordinationState.java:236-239
if (join.lastAcceptedTerm() > lastAcceptedTerm) {
    // Note that this is running on the receiving node, so it must reject joins from nodes with fresher state. This is unlike a
    // real-world election where candidates will accept every vote they receive and it's the voter's responsibility to be selective
    // about the votes they cast.
```

```java
// Reconfigurator.java:44-47
 * We offer two options: either we auto-shrink the voting configuration as long as it contains more than three nodes, or we don't and we
 * require the user to control the voting configuration manually using the retirement API. The former, default, option, guarantees that
 * as long as there have been at least three master-eligible nodes in the cluster and no more than one of them is currently unavailable,
 * then the cluster will still operate, which is what almost everyone wants. Manual control is for users who want different guarantees.
```

### Part 10

```java
// PeerFinder.java:58-60
public static final Setting<TimeValue> DISCOVERY_FIND_PEERS_INTERVAL_SETTING = Setting.timeSetting(
    "discovery.find_peers_interval",
    TimeValue.timeValueMillis(1000),
```

```java
// DiscoveryPlugin.java:64
default Map<String, Supplier<SeedHostsProvider>> getSeedHostProviders(
```

### Part 11

```java
// StatefulPreVoteCollector.java:104-118 (rare-case comment elided)
if (leader == null) {
    return response;
}

if (leader.equals(request.getSourceNode())) {
    // ...
    return response;
}

throw new CoordinationStateRejectedException("rejecting " + request + " as there is already a leader");
```

```java
// Coordinator.java:583-586
private long getTermForNewElection() {
    assert Thread.holdsLock(mutex);
    return Math.max(getCurrentTerm(), maxTermSeen) + 1;
}
```

```java
// ElectionSchedulerFactory.java:62-64 and 78-80
public static final Setting<TimeValue> ELECTION_INITIAL_TIMEOUT_SETTING = Setting.timeSetting(
    ELECTION_INITIAL_TIMEOUT_SETTING_KEY,
    TimeValue.timeValueMillis(100),
// ...
public static final Setting<TimeValue> ELECTION_MAX_TIMEOUT_SETTING = Setting.timeSetting(
    ELECTION_MAX_TIMEOUT_SETTING_KEY,
    TimeValue.timeValueSeconds(10),
```

### Part 12

```java
// ClusterBootstrapService.java:49-57
public static final Setting<List<String>> INITIAL_MASTER_NODES_SETTING = Setting.stringListSetting(
    "cluster.initial_master_nodes",
    Property.NodeScope
);

public static final Setting<TimeValue> UNCONFIGURED_BOOTSTRAP_TIMEOUT_SETTING = Setting.timeSetting(
    "discovery.unconfigured_bootstrap_timeout",
    TimeValue.timeValueSeconds(3),
```

```java
// CoordinationMetadata.java:307-309
public static final VotingConfiguration MUST_JOIN_ELECTED_MASTER = new VotingConfiguration(
    Collections.singleton("_must_join_elected_master_")
);
```

```java
// StoreHeartbeatService.java:31-33 and 43-45
public static final Setting<TimeValue> HEARTBEAT_FREQUENCY = Setting.timeSetting(
    "cluster.stateless.heartbeat_frequency",
    TimeValue.timeValueSeconds(15),
// ...
public static final Setting<Integer> MAX_MISSED_HEARTBEATS = Setting.intSetting(
    "cluster.stateless.max_missed_heartbeats",
    2,
```

```java
// MasterNodeRequest.java:40-41
@Deprecated(forRemoval = true)
public static final TimeValue TRAPPY_IMPLICIT_DEFAULT_MASTER_NODE_TIMEOUT = TimeValue.timeValueSeconds(30);
```
