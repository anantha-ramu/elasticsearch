# Elasticsearch shard allocation

Notes from a teaching session, 2–5 Sep 2026. Covers the **Allocation** topic of the ES-Distributed list.

The guide section (`docs/internal/DistributedArchitectureGuide.md` lines 2288–2356) is only ~70 lines —
a good overview, not a reference. So unlike the cluster-coordination notes, these are built from source.
Dianna's onboarding outline and Ievgen's brain dump are the companion documents; the recorded sessions are
in the team Drive folder.

Every claim was checked against source at the cited line. **Nothing was executed** — the *Lab* section
lists what would turn reading into evidence. Line numbers are against commit `cd4f46d0e292`; if one looks
wrong, grep for the quoted code in the appendix.

## If you've forgotten everything

Read *The story in one page*. If you're debugging, skip to *Failure scenarios* or Part 10. *Settings you'll
meet* is the 3am table. The appendix has the code quotes if a line number has drifted.

---

## The story in one page

**Allocation never touches a shard.** It edits a map — the routing table inside the cluster state. The
master writes "shard 3 should be on node B"; node B reads the new state, notices a shard it should have and
doesn't, builds it, and tells the master when it's done. Dianna's phrase: *the cluster state is a fancy work
queue.*

It does three things, in strict priority order: **assign** shards with no home, **move** shards off nodes
they can't stay on, **rebalance** so load is even. Primaries before replicas throughout.

Every decision funnels through `AllocationService.reroute`, which is three steps:

1. Clear expired delay markers.
2. **Census** — for unassigned shards, ask nodes what's on disk. A primary may only go on a node holding a
   copy from the *in-sync set*; a replica goes wherever the fewest bytes need copying. Fully asynchronous:
   the master fires requests and moves on, and the answers trigger the next reroute.
3. **Balancer** — gets whatever the census didn't claim.

The balancer is split in two. A **dreamer** on a background thread works out where everything should ideally
live and keeps that plan fresh. A **reconciler** on the master thread takes a few throttled steps toward the
current plan on every reroute. That split is why the master stays fast and why moves don't flip-flop.

Every proposed placement is checked by **24 deciders**, any one of which can veto (`NO`), defer
(`THROTTLE`), or discourage (`NOT_PREFERRED`) it. Deciders say where a shard *may* go; the **weight
function** — a separate layer — says where it *should*, by scoring each node's deviation from average on
shard count, per-index shard count, write load, and disk bytes.

Disk gets three thresholds: 85% stops new arrivals, 90% pushes shards off, 95% makes indices read-only.
A restart looks exactly like a failure unless you say otherwise, so replicas wait a minute before being
rebuilt — or five, if you used the **shutdown API** to declare intent.

When something is stuck, `GET _cluster/allocation/explain` names the decider and the reason, per node.
`GET _internal/desired_balance` tells you whether the *plan* is wrong or merely unrealised.

---

## 1. Vocabulary

**Shard copy** — one physical instance of one shard, primary or replica; described by a `ShardRouting`.
Four states (`ShardRoutingState.java:16-46`):

- `UNASSIGNED` — nowhere. Every copy starts here and returns here on failure.
- `INITIALIZING` — assigned, being built. Not serving.
- `STARTED` — the only state that does work.
- `RELOCATING` — the state on the **source** node while a new copy on the target is `INITIALIZING`. Two
  `ShardRouting`s for one logical copy; the source keeps serving until the target is ready.

**What a copy carries** (`ShardRouting.java:141-169`): `currentNodeId`, `relocatingNodeId` (the other end
during a move), `primary`, `recoverySource`, `unassignedInfo`, `allocationId`.

**`recoverySource`** — *how* a copy will come into existence: `EMPTY_STORE`, `EXISTING_STORE` (reuse local
files), `PEER` (copy from primary), `SNAPSHOT`, `LOCAL_SHARDS` (shrink/split). Allocation picks the node and
the source; **recovery** (separate topic) does the copying.

**`UnassignedInfo`** — why it's unassigned (`NODE_LEFT`, `ALLOCATION_FAILED`, `INDEX_CREATED`,
`PRIMARY_FAILED`, `NODE_RESTARTING`, … — `UnassignedInfo.java:102-190`) *and* what happened last time the
allocator looked (lines 220–241): `DECIDERS_NO`, `NO_VALID_SHARD_COPY`, `DECIDERS_THROTTLED`,
`FETCHING_SHARD_DATA`, `DELAYED_ALLOCATION`, `NO_ATTEMPT`. Every "why is my shard red" ends in one of those
six.

**`AllocationId`** — minted when a copy goes `UNASSIGNED → INITIALIZING`, lives as long as that copy. The
index metadata separately keeps the **in-sync set** — IDs of copies known to hold every acknowledged write.
Only an in-sync copy may become primary. Relocation wrinkle at `AllocationId.java:26-33`.

**Two views of the map** (`package-info.java:11-69`):

| | shape | where it lives | used for |
| --- | --- | --- | --- |
| `RoutingTable` | index → shard → copies → node | in the cluster state (in memory on every node, **not** on disk) | routing a search |
| `RoutingNodes` | node → copies on it | built on demand, cached (`ClusterState.java:199-200, 460-466`) | balancing |

Navigation one-liners (all verified to exist):

```java
routingTable.index("my-index").shard(0).primaryShard().currentNodeId()  // where's the primary?
routingNodes.node("node-1").iterator()                                  // what's on this node?
```

**One sentence:** allocation edits a map of shard copies, each carrying a state, a recovery source, a reason
if unassigned, and an ID that decides whether it's trusted to become primary.

---

## 2. The loop

Follow one shard. Node C dies holding shard 3.

1. **Master clears the map.** The `node-left` task calls `disassociateDeadNodes`
   (`AllocationService.java:301`): copies on C become `UNASSIGNED` with reason `NODE_LEFT`, and an in-sync
   replica is promoted to primary on the spot — no election, just a flag flipped.
2. **Reroute decides** (`AllocationService.java:461`, called after *anything* that could change placement —
   node join/leave, index create/delete, setting change, snapshot restore, shard started/failed, manual
   `POST _cluster/reroute`). Three steps at lines 600–602: clear delay markers → census (Part 4) → balancer
   (Parts 5–7). Census does all primaries first, then all replicas (lines 613–633).
3. **Skip if nothing changed** — same object returned, publication skipped (lines 482–484).
4. **Publish**, then on the data node the shard-management applier reacts
   (`IndicesClusterStateService.doApplyClusterState`, line 342): removals first (line 377, freeing space),
   then create/update (744, 817, 880). `createShard` kicks off recovery.
5. **Report back** — `internal:cluster/shard/started` or `.../failure` (`ShardStateAction.java:55-56`),
   handled by `applyStartedShards` (line 153) / `applyFailedShards` (line 205)… **which reroute again**,
   because a finished recovery frees a throttle slot.

Two consequences: everything master-side runs on the master's single thread inside a cluster state update
(so slow allocation = slow master), and **the master never knows what's physically on a node until the node
tells it** — the map is belief, `shardStarted`/`shardFailed` are corrections.

**One sentence:** master re-decides the map in one `reroute`, publishes, data nodes make reality match and
report back, and each report triggers the next reroute.

---

## 3. Deciders — the rules that say no

24 classes in `allocation/decider/`. Each answers three questions about one shard and one node
(`AllocationDeciders.java:76, 96/104, 113`):

- `canAllocate` — may it go here?
- `canRemain` — may it stay here? *(this is what makes shards leave nodes)*
- `canRebalance` — may optional moves happen at all right now?

Four answers (`Decision.java:132-137`), severity `NO` > `THROTTLE` > `NOT_PREFERRED` > `YES`. The combining
loop keeps the most negative and **breaks on the first `NO`** (lines 216–223) — no point asking the rest.

### The families

- **Correctness:** `SameShard` (two copies of one shard on one node makes redundancy a lie),
  `ReplicaAfterPrimaryActive`, `NodeVersion` / `IndexVersion` (a shard written by newer Lucene can't open on
  an older node — this is what makes rolling upgrades safe).
- **Operator intent:** `Filter` (`index/cluster.routing.allocation.require|include|exclude.*`; also how data
  tiers work), `Awareness` (spread across zones/racks), `ShardsLimit`, and the big switch
  `EnableAllocationDecider` — `cluster.routing.allocation.enable`, values `all`/`primaries`/`new_primaries`/
  `none` (`EnableAllocationDecider.java:56-59`). Most common operator action here; most common way to end up
  red is forgetting to set it back.
- **Resources:** `DiskThreshold` (Part 8), `WriteLoadConstraint`.
- **Node lifecycle:** `NodeShutdown`, `NodeReplacement` (Part 9).
- **Pacing:** `ThrottlingAllocationDecider` — the source of `THROTTLE`.
- **Giving up:** `MaxRetryAllocationDecider` — `index.allocation.max_retries`, default **5**
  (`MaxRetryAllocationDecider.java:30-32`). Shard stays unassigned until a human runs
  `POST _cluster/reroute?retry_failed=true`.

### Deciders vs weights

**Deciders veto. Weights rank.** A decider never expresses preference; the balancer never overrides a
decider. Two layers — rules, then taste.

Which makes `NOT_PREFERRED` interesting: a *rule* saying something that sounds like taste — "allowed, but
choose elsewhere if you can." Used by `WriteLoadConstraintDecider` and `IndexBalanceAllocationDecider`, both
recent. If every node is `NOT_PREFERRED` the shard is still placed. The wall between the layers has grown a
door.

**One sentence:** 24 independent rules, any one of which can veto or defer; rules say where a shard *may* go,
weights say where it *should*.

---

## 4. Census — don't copy what you already have

Runs **before** the balancer, because for primaries it's *safety* and for replicas it's *cost*.

### The census is asynchronous

`AsyncShardFetch`'s javadoc says the whole design (lines 44–51): fetch shard data from other nodes
*"without blocking the cluster update thread"*, and when results return, *"schedule a reroute"*.

- On the master thread, `fetchData` is a **cached map lookup**. Cache hit → decide. Miss → fire transport
  requests (line 112) and return immediately with `hasData() == false`.
- Allocator marks the shard `FETCHING_SHARD_DATA` and moves on (`PrimaryShardAllocator.java:90-98`).
- Answers arrive on transport threads → schedule another reroute (abstract hook, line 244).
- Results are cached **per shard per node**; it only re-asks nodes it has no answer from (lines 288–294). So
  the census happens once per shard, not once per reroute. On a healthy cluster this step costs ~nothing.

This is why a full cluster restart pauses briefly with everything in `FETCHING_SHARD_DATA` — the census
completing, not a fault.

### Primaries: only a trusted copy

```java
final Set<String> inSyncAllocationIds = indexMetadata.inSyncAllocationIds(unassignedShard.id());
```
(`PrimaryShardAllocator.java:103`)

A copy may become primary **only if its allocation ID is in the in-sync set** — file freshness is not
evidence. The set is maintained by `IndexMetadataUpdater` (javadoc at lines 39–45): IDs added when a copy
becomes active, removed when it stops being active, *before* the write is acknowledged. If no node reports an
in-sync copy → `NO_VALID_SHARD_COPY` (line 147), red, correctly. That's what `allocate_stale_primary` with
`accept_data_loss: true` overrides.

### Replicas: whichever is cheapest

No trust needed — it syncs from the primary anyway. Ranked three tiers, best first
(`ReplicaShardAllocator.java:471-479`):

1. **No-op** — a peer-recovery retention lease covers this exact copy; replay a few operations, copy nothing.
2. **Operation-based** — lease exists, further behind; still no files.
3. **File-based partial** — some segment files match byte-for-byte; copy only the difference.

Pure cost optimisation. It will even **cancel an in-progress recovery** if a better node appears (lines
96–100) — throwing away partial work because the alternative is nearly free.

**One sentence:** the master asks every node what's on disk (asynchronously, cached); a primary goes only on
a node holding an in-sync copy or stays red, while a replica goes wherever the fewest bytes need copying.

---

## 5. Desired balance — dreamer and mover

**The problem:** solving the ideal layout is an iterative search, and allocation runs on the master's single
thread. The old balancer re-solved the whole puzzle on every reroute.

**The split:**

- **Dreamer** (`DesiredBalanceComputer`, background thread) — *ignoring reality, where should everything
  ideally be?* Output is a plan: shard → node IDs (`DesiredBalance.java:29-32`). Nothing moves.
- **Mover** (`DesiredBalanceReconciler`, master thread, inside reroute) — solves nothing; picks the next few
  throttled moves toward the current plan and writes them on the map (`submitReconcileTask`,
  `DesiredBalanceShardsAllocator.java:404`).

**The dreamer never falls behind** (`ContinuousComputation.java:22-26`): one thread, always the newest
snapshot; if a fresher one arrives before an older starts, the older is **skipped**. Mid-search it re-checks
`isFresh` (`DesiredBalanceComputer.java:446`) and stops early rather than perfecting a stale answer.

**How they meet** — two lines in `allocate` (`DesiredBalanceShardsAllocator.java:325, 334`): hand the dreamer
a fresh snapshot *asynchronously*, then reconcile against the plan we already have. The comment explains why
stale is fine: *"balance should have incremental rather than radical changes."*

### What the dreamer does and doesn't know

It does **not** do the disk census — it's explicit:

```java
// we are not responsible for allocating unassigned primaries of existing shards, and we're only responsible for allocating
// unassigned replicas if the ReplicaShardAllocator gives up, so we must respect these ignored shards
```
(`DesiredBalanceComputer.java:167-168`)

Shards the census claimed arrive in an "ignored" list — `DesiredBalanceInput.java:24` defines it as *"shards
for which earlier allocators have claimed responsibility"* — and are skipped (line 180).

But it isn't naive either. Two anti-churn hints:

- starts from the **previous plan** (line 202), so it drifts rather than lurches;
- adds each shard's **`lastAllocatedNodeId`** as a candidate (lines 208–213), *"preserving last known shard
  location as a starting point to avoid unnecessary relocations."*

Keep the distinction: `lastAllocatedNodeId` is **metadata** (a note saying where it used to be, often wrong);
the census is **evidence** (the node was asked and answered). The dreamer uses the cheap hint; it never asks.

It also plans against the **near future**, not the present: in-flight recoveries are simulated as finished
(lines 157–165) and disk usage simulated forward with a `ClusterInfoSimulator` (line 147).

**Payoff:** master stays fast; cluster converges instead of oscillating; and `GET _internal/desired_balance`
lets you see whether the plan is wrong or merely unrealised — two different fixes the old design couldn't
distinguish.

**One sentence:** a background thread keeps a plan fresh; the master takes a few throttled steps toward it
each reroute.

---

## 6. The weight function

Balance means: **for each thing we care about, a node's share should equal the cluster average.** A node's
weight is its deviation.

```java
final float weightShard = nodeNumShards - avgShardsPerNode;
final float ingestLoad = (float) (nodeWriteLoad - avgWriteLoadPerNode);
final float diskUsage = (float) (diskUsageInBytes - avgDiskUsageInBytesPerNode);
return theta0 * weightShard + theta2 * ingestLoad + theta3 * diskUsage;
```
(`WeightFunction.java:89-92`)

Positive = over its share, move things off. Negative = good destination. The loop: heaviest → lightest,
repeat until nothing improves.

| Factor | Setting | Default | Prevents |
| --- | --- | --- | --- |
| Shard count | `cluster.routing.allocation.balance.shard` | 0.45 | one node babysitting far more shards |
| Index spread | `...balance.index` | 0.55 | all shards of one index on one node |
| Write load | `...balance.write_load` | 10.0 | the hot new index piling onto one node |
| Disk usage | `...balance.disk_usage` | 2e-11 | one disk filling while others idle |
| Deadband | `...balance.threshold` | 1.0 (min 1.0) | endless fidgeting |

(`BalancedShardsAllocator.java:110-144`, all dynamic.)

**Index spread** is the one people miss — a per-index count added on top of the global one
(`WeightFunction.java:69-78`). Without it a node could hold an average *total* that happens to be all of
`logs-2026.09.03`. Balanced on paper, hot in practice. Note it outweighs the global count, 0.55 vs 0.45.

**Don't read the numbers as importance.** They're normalised — divided by their sum (lines 52–60) — so only
ratios matter. And the units differ wildly: shard count is single digits, disk is *bytes*. `2e-11` converts
bytes into shard-comparable units (~50 GB ≈ one shard). **Don't hand-tune these**; an unbalanced cluster is
almost always a decider, a filter, or throttling.

**Threshold** is why balancing stops: a move must improve weight by >1.0, i.e. roughly one whole shard's worth
of imbalance. Minimum enforced value is also 1.0 — you can't make it twitchier.

**Sizes it doesn't know yet:** forecast from the index (data streams predict from previous backing indices),
else extrapolate from known shards (`getIndexDiskUsageInBytes`, lines 141–166). This is why data streams
balance better than ad-hoc indices. Partial searchable snapshots count as zero bytes (lines 142–146).

**One sentence:** weight = deviation from average on four axes, normalised into one number; move heaviest to
lightest until no move gains a whole shard's worth.

---

## 7. Reconciliation — order and throttles

Three passes, strictly in order (`DesiredBalanceReconciler.java:164-177`):

1. **`allocateUnassigned()`** — data currently unavailable. Nothing matters more.
2. **`moveShards()`** — a `canRemain` decider said no (disk over high watermark, node shutting down, filter
   just applied). Obligations, not emergencies.
3. **`balance()`** — optional. Everything works; it could work more evenly.

Since all three compete for the same throttle budget, the order is also a rationing rule.

**The dreamer does these in the opposite order internally**, and says why
(`DesiredBalanceComputer.java:226-228`): it prefers not to leave *immovable* shards in undesirable places.
The dreamer is imagining (nothing at risk, place the hard ones first); the reconciler is acting (real
unavailable data, fix availability first).

**Fairness within a pass:** an ordered iterator (`OrderedShardsIterator.java:21-26`) puts a node that just
received a shard at the *back* of the queue. The reason (lines 488–489): *"in the presence of throttling…
achieve a fairer movement of shards from the nodes that are offloading."* Otherwise one overloaded node hogs
every slot.

**The throttles:**

| Setting | Default | Limits |
| --- | --- | --- |
| `cluster.routing.allocation.node_concurrent_recoveries` | **2** | incoming recoveries per node |
| `cluster.routing.allocation.node_initial_primaries_recoveries` | **4** | per node, primaries from local disk |
| `cluster.routing.allocation.cluster_concurrent_rebalance` | **2** | pass-3 moves, cluster-wide |

(`ThrottlingAllocationDecider.java:48-49`, `ConcurrentRebalanceAllocationDecider.java:41-47`.)

Note the asymmetry: local-disk primary recovery is cheap (4), network copy is expensive (2), and optional
rebalancing gets its own separate budget so it can never crowd out recoveries. Throttled moves are **not
queued** — the next reroute simply tries again.

**When the gap won't close:** if >**10%** of shards sit somewhere the plan doesn't want
(`undesired_allocations.threshold`, lines 74–80), a warning is logged at most hourly (line 62). That line is
the fingerprint of "plan is right, reality can't get there."

**One sentence:** unavailable shards first, then must-move, then optional — round-robin across nodes, capped
by small budgets, everything unfinished retried next reroute.

---

## 8. Disk watermarks

| Watermark | Default | Effect |
| --- | --- | --- |
| low | **85%** | no *new* shards allocated here |
| high | **90%** | shards start being *moved off* |
| flood stage | **95%** | every index with a shard here goes **read-only** |

(`DiskThresholdSettings.java:38-89`, all dynamic.)

**Low and high map onto the two decider questions:** low → `canAllocate` says NO
(`DiskThresholdDecider.java:241-243`); high → `canRemain` says NO (line 397 onward). And Part 7's pass 2 is
"move shards that cannot remain" — so crossing the high watermark automatically enrols that node's shards for
the next round of moves. Two thresholds, two questions, one decider.

**Flood stage is different in kind** — not an allocation decision but a cluster state change, made by
`DiskThresholdMonitor`, not the decider. Every index with *any* shard on the flooded node gets
`index.blocks.read_only_allow_delete` (lines 218–223). Blast radius is deliberate: a node that fills
completely doesn't degrade gracefully, and blocking writes is the only reliable lever. The block name is the
escape hatch — you can still delete.

**It un-blocks itself** (lines 420–426, look for *"releasing read-only block on indices"*). Safety catch: if a
node's disk usage is *unknown*, its indices are never auto-released (`markNodesMissingUsageIneligibleForRelease`,
line 461). Silence isn't good news.

**It counts shards in flight.** A node with 100 GB free and two 60 GB shards recovering onto it is already
oversubscribed, and there's a distinct verdict for it (lines 208–213). Same reasoning as the dreamer
simulating recoveries complete: decide against the near future.

**Headroom caps.** 85% of 20 TB leaves 3 TB idle, so each watermark has a `max_headroom` companion (lines 46,
68, 90). Effective threshold is whichever is stricter.

**The 60-second clock.** `cluster.routing.allocation.disk.reroute_interval` (lines 125–130) floors
disk-driven reroutes. Explains "I freed space and nothing happened for a minute."

**One sentence:** 85% stops arrivals, 90% pushes shards off, 95% goes read-only — all counting shards still in
flight.

---

## 9. Delayed allocation and node shutdown

**The problem:** a restart is indistinguishable from a death. Node leaves → shards `UNASSIGNED` → rebuild
elsewhere → node returns 90 s later with the data intact → undo. Ten nodes rolling-restarted = ten rounds of
pointless copying.

**Fix one — wait.** `index.unassigned.node_left.delayed_timeout`, default **1 minute** (10 s in stateless,
where rebuilding is cheap) (`UnassignedInfo.java:81-89`). Shard sits with status `DELAYED_ALLOCATION`;
nothing copies. **Replicas only** — an unassigned primary is unavailable data and never worth waiting on. The
master sets an alarm rather than polling (`findNextDelayedAllocation`, line 434), which is the
`removeDelayMarkers` step of `reroute`.

**Fix two — declare intent.** `PUT _nodes/{id}/shutdown` with a type (`SingleNodeShutdownMetadata.java:483-487`):

| Type | Meaning | Behaviour |
| --- | --- | --- |
| `RESTART` | back shortly | hold shards; default delay **5 min** (line 96) |
| `REMOVE` | gone for good | move everything off now; poll for "safe to stop" |
| `REPLACE` | node B takes over from A | move A's shards to B, skip the usual churn |
| `SIGTERM` | local `REMOVE` | with a mandatory grace period |

The code enforces semantics: delay only valid for `RESTART` (135–137), target node required for `REPLACE` and
forbidden otherwise (139–149), grace period mandatory for `SIGTERM` (151–154). `isRemovalType()` (line 502) is
the one-line summary the rest of the code branches on — `RESTART` is the only type where the node is expected
back.

**How intent reaches the allocator:** through `NodeShutdownAllocationDecider` / `NodeReplacementAllocationDecider`
(Part 3's machinery, no new mechanism) and through `remainingDelay` (line 402), which uses the shutdown's delay
instead of the index default. The dreamer also resets its plan when a *non-restart* shutdown appears
(`DesiredBalanceShardsAllocator.java:346-350`).

**Practice:** mark `RESTART` before a rolling restart; mark `REMOVE` and poll `GET _nodes/{id}/shutdown` when
decommissioning. The old `cluster.routing.allocation.enable: none` trick still works but is cluster-wide,
blocks genuine recoveries too, and is the setting people forget to unset.

**One sentence:** replicas wait a minute before rebuild because a restart looks like a failure — the shutdown
API replaces that guess with a declaration the deciders honour.

---

## 10. Debugging

### `GET _cluster/allocation/explain`

No body → finds an unassigned shard. None to find → says so plainly and invites you to name an assigned one
(`TransportClusterAllocationExplainAction.java:166-168`), because it answers two questions: *why can't this be
allocated?* and (with `"current_node"`) *why isn't this somewhere better?*

Read the response in three parts:

1. **`unassigned_info`** — reason + last allocation status. The status alone usually names the problem:

| Status | Meaning | Part |
| --- | --- | --- |
| `FETCHING_SHARD_DATA` | census running — **wait** | 4 |
| `DELAYED_ALLOCATION` | deliberately waiting — **wait** | 9 |
| `DECIDERS_THROTTLED` | allowed, budget full — **wait** | 7 |
| `NO_VALID_SHARD_COPY` | no in-sync copy anywhere. Serious. | 4 |
| `DECIDERS_NO` | every node vetoed — read the list | 3 |
| `NO_ATTEMPT` | not looked at this round | — |

Three of six mean "wait." Most "stuck" clusters aren't.

2. **Per-node decisions** — which decider said no, in prose. Add `"include_yes_decisions": true` to see what
   passed.
3. **Store info** (replicas) — matching bytes per node and in-sync flags: Part 4's ranking, shown.

### `GET _internal/desired_balance`

Plan vs reality. **The diagnostic fork:**

- Plan sensible, reality lags → throttled or blocked. Check concurrency settings and the undesired-allocations
  warning.
- Plan itself wrong → a decider forbidding the nodes you expected, or a tuned weight factor.

`DELETE _internal/desired_balance` discards and recomputes; rarely needed.

### Manual overrides — `POST _cluster/reroute`

**Safe:** `move`, `allocate_replica`, `cancel` — only things the allocator could have done anyway.

**Dangerous**, both requiring `accept_data_loss: true` (`BasePrimaryAllocationCommand.java:26-38`):

- `allocate_stale_primary` — promote a copy *not* in the in-sync set. Overrides Part 4's safety rule.
- `allocate_empty_primary` — create it empty. Everything in it is gone.

Only after `NO_VALID_SHARD_COPY`, and only with no snapshot available.

**The one you'll actually use:** `POST _cluster/reroute?retry_failed=true` (`ClusterRerouteRequest.java:83-100`)
— resets the counters `MaxRetryAllocationDecider` blocks on. Workflow is always: explain says `max_retry` →
fix the real cause → run this. The counter doesn't clear itself.

### Supporting

`GET _cat/shards?v` (state + node), `GET _cat/allocation?v` (disk per node — fastest Part 8 check),
`GET _cluster/health?level=indices` (which index).

**One sentence:** explain never says the cluster is broken — it says which rule can't currently be satisfied,
and by whom.

---

## Failure scenarios

**Node dies and comes back after its shards were rebuilt elsewhere.** Its on-disk copies are now stale — their
allocation IDs were removed from the in-sync set when they stopped being active. The census still reports them,
and they're still useful as *replica* targets (file-based partial recovery, Part 4), but they can never be
promoted to primary. Eventually the balancer may place a replica back on that node cheaply. Nothing is lost;
nothing is trusted that shouldn't be.

**Master changes mid-recovery.** The in-flight recovery is between two data nodes and doesn't stop. But the new
master's `RoutingNodes` comes from the published cluster state, so it sees the shard as `INITIALIZING` and
waits for the `shardStarted` message like the old one would. If the recovery *fails*, `shardFailed` goes to
whoever is master then. The dreamer's plan, however, is in-memory on the master and is recomputed from scratch
by the new master.

**Explain says `DECIDERS_NO` on every node, different reasons each.** Not a bug — the configuration can't be
satisfied. Worked example: node-1 `same_shard` (copy already there), node-2 `disk_threshold` (over 85%),
node-3 `awareness` (both zones already have a copy). Fix is capacity or topology, not a reroute command.

**Cluster relocates constantly and never settles.** Nodes hovering near 90%: moving shards off one pushes
another over, which triggers more moves. Add capacity; tuning watermarks moves the cliff rather than removing it.

**Writes fail with `read_only_allow_delete`.** Flood stage, Part 8. Delete data or add disk; the block lifts
itself when the monitor next runs and usage has dropped — unless the node's disk usage is unknown, in which
case it never auto-releases.

**Shard stuck after you fixed the underlying problem.** `MaxRetryAllocationDecider` counted five failures and
won't try again. `POST _cluster/reroute?retry_failed=true`.

**Rolling restart leaves shards unassigned.** Either `cluster.routing.allocation.enable` was left at `none`/
`primaries` (Part 3), or nodes took longer than the 1-minute delay to return and rebuilds started (Part 9).
Use the shutdown API with `RESTART`.

**Cluster looks unbalanced but explain shows nothing wrong.** Check `_internal/desired_balance`. If the plan
matches what you'd expect, it's throttling — and look for the hourly undesired-allocations warning.

---

## Counterintuitive findings

1. **Allocation never touches a shard.** It edits a map; data nodes react to the map.
2. **The routing table is in the cluster state but not on disk.** Ephemeral, rebuilt on full restart.
3. **`RoutingNodes` isn't in the cluster state at all** — built on demand and cached.
4. **`RELOCATING` is the state of the *source* copy.** The target is `INITIALIZING`. Two routings, one copy.
5. **A reroute that changes nothing returns the same object** and skips publication entirely.
6. **The disk census is fully async.** The master thread only does a cached map lookup.
7. **The census runs once per shard, not per reroute** — results are cached per shard per node.
8. **Fresh files don't qualify a primary.** Only membership of the in-sync set does.
9. **A replica recovery in progress can be cancelled** if a better-matching node appears.
10. **The dreamer never does the census.** It uses `lastAllocatedNodeId` — metadata, not evidence.
11. **The dreamer plans against a simulated near-future**, with in-flight recoveries treated as complete.
12. **The dreamer and reconciler use opposite priority orders**, deliberately.
13. **Deciders break on the first `NO`** — later deciders aren't consulted.
14. **`NOT_PREFERRED` is a rule expressing a preference** — the layer boundary has a door in it.
15. **Balance factors are normalised**, so only their ratios matter; the disk factor is `2e-11` because its
    unit is bytes.
16. **Index spread outweighs global shard count** (0.55 vs 0.45).
17. **Balancing stops at a whole shard's worth of improvement**, and you can't lower the threshold below 1.0.
18. **Local-disk primary recovery gets a higher throttle (4) than network recovery (2).**
19. **Throttled moves aren't queued** — just retried next reroute.
20. **The high watermark works via `canRemain`**, which is what makes shards drain off a filling node.
21. **Flood stage blocks the whole index**, including shards on healthy nodes.
22. **Unknown disk usage never auto-releases a read-only block.**
23. **Delayed allocation applies to replicas only.**
24. **`RESTART` is the only shutdown type where the node is expected back** (`isRemovalType()`).
25. **Half the "stuck" statuses mean wait**, not intervene.

---

## Settings you'll meet

| Setting | Default | When you'll care |
| --- | --- | --- |
| `cluster.routing.allocation.enable` | `all` | left at `none`/`primaries` after a restart |
| `index.allocation.max_retries` | 5 | shard stuck after you fixed the cause |
| `cluster.routing.allocation.node_concurrent_recoveries` | 2 | recovery feels slow after a restart |
| `...node_initial_primaries_recoveries` | 4 | same, local-disk primaries |
| `cluster.routing.allocation.cluster_concurrent_rebalance` | 2 | rebalancing feels slow |
| `cluster.routing.allocation.disk.watermark.low` | 85% | "above the low watermark" in explain |
| `...watermark.high` | 90% | shards constantly relocating |
| `...watermark.flood_stage` | 95% | `read_only_allow_delete` write failures |
| `...disk.reroute_interval` | 60s | freed space, nothing happened yet |
| `index.unassigned.node_left.delayed_timeout` | 1 min (10 s stateless) | rebuilds started during a restart |
| shutdown `RESTART` delay | 5 min | rolling restarts |
| `cluster.routing.allocation.balance.{shard,index,write_load,disk_usage}` | .45 / .55 / 10 / 2e-11 | don't tune these |
| `cluster.routing.allocation.balance.threshold` | 1.0 | why balancing stopped |
| `...desired_balance.undesired_allocations.threshold` | 0.1 | the hourly "plan unrealised" warning |

---

## ES file index

| File | Why |
| --- | --- |
| `docs/internal/DistributedArchitectureGuide.md` | Allocation section, lines 2288–2356 |
| `cluster/routing/ShardRoutingState.java` | four states + transitions, 16–46 |
| `cluster/routing/ShardRouting.java` | fields, 141–169 |
| `cluster/routing/UnassignedInfo.java` | reasons 102–190; statuses 220–241; delay setting 81–89; `findNextDelayedAllocation` 434 |
| `cluster/routing/RecoverySource.java` | five recovery types, 79–305 |
| `cluster/routing/AllocationId.java` | relocation semantics, 26–33 |
| `cluster/routing/package-info.java` | RoutingTable vs RoutingNodes, 11–69 |
| `cluster/routing/allocation/AllocationService.java` | `disassociateDeadNodes` 301; `reroute` 461; three steps 600–602; census loop 606–634; no-change 482–484; `applyStartedShards` 153; `applyFailedShards` 205 |
| `cluster/action/shard/ShardStateAction.java` | action names 55–56; `shardStarted` 275 |
| `indices/cluster/IndicesClusterStateService.java` | `doApplyClusterState` 342; removals 377; create/update 744, 817, 880 |
| `cluster/routing/allocation/decider/AllocationDeciders.java` | three questions 76–116; combining loop 216–223 |
| `.../decider/Decision.java` | four types + ordering 132–137, 216–218 |
| `.../decider/ThrottlingAllocationDecider.java` | throttle defaults 48–49 |
| `.../decider/MaxRetryAllocationDecider.java` | max_retries 30–32 |
| `.../decider/EnableAllocationDecider.java` | the big switch 56–59 |
| `.../decider/ConcurrentRebalanceAllocationDecider.java` | rebalance budget 41–47 |
| `.../decider/DiskThresholdDecider.java` | canAllocate 169; in-flight 208–213; low 241–243; high 276–278; canRemain 397 |
| `cluster/routing/allocation/DiskThresholdSettings.java` | three watermarks + headroom + interval, 32–130 |
| `cluster/routing/allocation/DiskThresholdMonitor.java` | read-only marking 218–223; auto-release 404–426; unknown-usage catch 461 |
| `gateway/AsyncShardFetch.java` | async javadoc 44–51; cache 288–294 |
| `gateway/PrimaryShardAllocator.java` | in-sync gate 102–107; NO_VALID_SHARD_COPY 145–149; ranking 241–310 |
| `gateway/ReplicaShardAllocator.java` | three-tier comparator 471–479; cancel-for-better 96–100 |
| `cluster/routing/allocation/IndexMetadataUpdater.java` | who maintains the in-sync set, 39–45 |
| `.../allocator/DesiredBalanceShardsAllocator.java` | class javadoc 52–55; `allocate` 312–334; `submitReconcileTask` 404; shutdown reset 346–350 |
| `.../allocator/DesiredBalanceComputer.java` | `compute` 123; simulate-future 143–165; not-responsible comment 167–169; hints 202–213; order comment 226–228; `isFresh` 446 |
| `.../allocator/DesiredBalanceInput.java` | what "ignored" means, 24–34 |
| `.../allocator/DesiredBalance.java` | the plan record, 29–32 |
| `.../allocator/ContinuousComputation.java` | one-thread-newest-input, 22–26 |
| `.../allocator/DesiredBalanceReconciler.java` | three passes 164–177; fairness comment 488–489; undesired warnings 62–80 |
| `.../allocator/OrderedShardsIterator.java` | ordering rationale, 21–26 |
| `.../allocator/WeightFunction.java` | formula 89–92; index term 69–78; normalisation 52–60; size forecasting 141–166 |
| `.../allocator/BalancedShardsAllocator.java` | five balance settings, 110–144 |
| `cluster/metadata/SingleNodeShutdownMetadata.java` | types 483–487; validation 135–154; restart delay 96; `isRemovalType` 502 |
| `action/admin/cluster/allocation/TransportClusterAllocationExplainAction.java` | explain entry 99–143; no-shards message 166–168 |
| `action/admin/cluster/reroute/ClusterRerouteRequest.java` | `retry_failed` 83–100 |
| `cluster/routing/allocation/command/` | the five commands; `accept_data_loss` in `BasePrimaryAllocationCommand.java:26-38` |

---

## Lab

Three-node cluster: `./gradlew run --nodes=3 -Dtests.es.xpack.security.enabled=false`. Roughly by value:

1. **Watch the loop.** `logger.org.elasticsearch.cluster.routing.allocation: DEBUG`. Create an index with 3
   shards / 1 replica and read the reroute → INITIALIZING → shardStarted → reroute sequence. (Part 2.)
2. **Force `DECIDERS_NO` and read the explanation.** Set
   `index.routing.allocation.exclude._name` to all three nodes on a test index, then
   `GET _cluster/allocation/explain`. Confirm every node names `filter`. (Parts 3, 10.)
3. **See the census.** Stop and restart a data node; immediately poll `_cluster/allocation/explain` and catch
   `FETCHING_SHARD_DATA`, then watch it flip to allocated-from-`EXISTING_STORE` rather than a peer copy.
   (Part 4.)
4. **Prove delayed allocation.** Stop a node holding replicas; explain shows `DELAYED_ALLOCATION` with a
   remaining delay. Restart within the minute → no rebuild. Repeat waiting >1 min → rebuild starts. Then set
   `index.unassigned.node_left.delayed_timeout: 0` and see the difference. (Part 9.)
5. **Inspect the plan.** `GET _internal/desired_balance` on an idle cluster, then add a node and poll it —
   watch the plan change before reality does. (Parts 5, 7.)
6. **Hit a watermark safely.** Set `cluster.routing.allocation.disk.watermark.low` to something just below
   current usage (e.g. `10%`); explain should report "above the low watermark". Revert. Do **not** experiment
   with flood stage on anything you care about. (Part 8.)
7. **Trip `max_retry`.** Hard to force cleanly; easier to read `MaxRetryAllocationDecider` and confirm the
   counter lives in `UnassignedInfo`, then verify `retry_failed=true` resets it in
   `TransportClusterRerouteAction`. (Part 3.)
8. **Confirm the replica-write hypothesis from the task-management notes** while you're in this code —
   unrelated but adjacent.

---

## Questions asked during the session

| Question | Where the answer lives |
| --- | --- |
| Does the dreamer find existing copies, or is it pure distribution? | Part 5, *What the dreamer does and doesn't know* |
| Why are expensive disk checks on the master thread? | Part 4, *The census is asynchronous* — they aren't |
| What's the overall flow? | *The story in one page*; Part 2 |

---

## Not covered

- **Recovery** — what happens *after* allocation says "copy this shard." Own topic.
- **Data tiers / ILM** in depth — `DataTierAllocationDecider` is a `Filter` variant.
- **Autoscaling**, which consumes allocation's "can't place this" signal. Own topic.
- **Stateless allocation** — different roles (`index`/`search`) and much cheaper rebuilds. Stateless topic.
- **`BalancedShardsAllocator`'s inner loop** in detail — read it if you need to reason about convergence.
- **Resharding** (`RESHARD_ADDED` in `UnassignedInfo.Reason`) — new, and adjacent to your ES-14306 work.

---

## Appendix: code quotes, keyed by part

For self-verification when line numbers drift.

### Part 1

```java
// ShardRoutingState.java:23-46 (abridged)
UNASSIGNED((byte) 1),
INITIALIZING((byte) 2),
STARTED((byte) 3),
RELOCATING((byte) 4);
```

```java
// ShardRouting.java:141-165 (abridged)
private final ShardId shardId;
private final String currentNodeId;
@Nullable private final String relocatingNodeId;
private final boolean primary;
private final ShardRoutingState state;
@Nullable private final RecoverySource recoverySource;
@Nullable private final UnassignedInfo unassignedInfo;
private final AllocationId allocationId;
```

```java
// AllocationId.java:27-33
 * Uniquely identifies an allocation. An allocation is a shard moving from unassigned to initializing,
 * or relocation.
 * Relocation is a special case, where the origin shard is relocating with a relocationId and same id, and
 * the target shard (only materialized in RoutingNodes) is initializing with the id set to the origin shard
 * relocationId. Once relocation is done, the new allocation id is set to the relocationId.
```

### Part 2

```java
// AllocationService.java:600-602
rerouteStrategy.removeDelayMarkers(allocation);
allocateExistingUnassignedShards(allocation); // try to allocate existing shard copies first
rerouteStrategy.execute(allocation);
```

```java
// AllocationService.java:482-484
if (fixedClusterState == clusterState && allocation.routingNodesChanged() == false) {
    return clusterState;
}
```

```java
// ShardStateAction.java:55-56
public static final String SHARD_STARTED_ACTION_NAME = "internal:cluster/shard/started";
public static final String SHARD_FAILED_ACTION_NAME = "internal:cluster/shard/failure";
```

### Part 3

```java
// AllocationDeciders.java:216-223
for (AllocationDecider decider : deciders) {
    var decision = deciderAction.apply(decider);
    if (mostNegativeDecision.type().compareToBetweenDecisions(decision.type()) > 0) {
        mostNegativeDecision = decision;
        if (mostNegativeDecision.type() == Decision.Type.NO) {
            traceNoDecisions(decider, decision, logMessageCreator);
            break;
        }
    }
}
```

```java
// Decision.java:132-137
enum Type implements Writeable {
    // order matters only for serialization, do NOT use for comparison
    NO(0, 0),
    NOT_PREFERRED(1, 2),
    THROTTLE(2, 1),
    YES(3, 3);
```

```java
// ThrottlingAllocationDecider.java:48-49
public static final int DEFAULT_CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES = 2;
public static final int DEFAULT_CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_PRIMARIES_RECOVERIES = 4;
```

### Part 4

```java
// AsyncShardFetch.java:44-51
/**
 * Allows to asynchronously fetch shard related data from other nodes for allocation, without blocking
 * the cluster update thread.
 * <p>
 * The async fetch logic maintains a map of which nodes are being fetched from in an async manner,
 * and once the results are back, it makes sure to schedule a reroute to make sure those results will
 * be taken into account.
 */
```

```java
// AsyncShardFetch.java:288-294
private List<NodeEntry<T>> findNodesToFetch(Map<String, NodeEntry<T>> shardCache) {
    List<NodeEntry<T>> nodesToFetch = new ArrayList<>();
    for (NodeEntry<T> nodeEntry : shardCache.values()) {
        if (nodeEntry.hasData() == false && nodeEntry.isFetching() == false) {
            nodesToFetch.add(nodeEntry);
        }
    }
```

```java
// PrimaryShardAllocator.java:102-107
final IndexMetadata indexMetadata = allocation.metadata().indexMetadata(unassignedShard.index());
final Set<String> inSyncAllocationIds = indexMetadata.inSyncAllocationIds(unassignedShard.id());
final boolean snapshotRestore = unassignedShard.recoverySource().getType() == RecoverySource.Type.SNAPSHOT;

assert inSyncAllocationIds.isEmpty() == false;
// use in-sync allocation ids to select nodes
```

```java
// ReplicaShardAllocator.java:471-479
private record MatchingNode(long matchingBytes, long retainingSeqNo, boolean isNoopRecovery) {
    static final Comparator<MatchingNode> COMPARATOR = Comparator.<MatchingNode, Boolean>comparing(m -> m.isNoopRecovery)
        .thenComparing(m -> m.retainingSeqNo)
        .thenComparing(m -> m.matchingBytes);

    boolean anyMatch() {
        return isNoopRecovery || retainingSeqNo >= 0 || matchingBytes > 0;
    }
}
```

### Part 5

```java
// ContinuousComputation.java:22-26
/**
 * Asynchronously runs some computation using at most one thread but expects the input value changes over time as it's running. Newer input
 * values are assumed to be fresher and trigger a recomputation. If a computation never starts before a fresher value arrives then it is
 * skipped.
 */
```

```java
// DesiredBalanceComputer.java:167-168
// we are not responsible for allocating unassigned primaries of existing shards, and we're only responsible for allocating
// unassigned replicas if the ReplicaShardAllocator gives up, so we must respect these ignored shards
```

```java
// DesiredBalanceComputer.java:208-213
// preserving last known shard location as a starting point to avoid unnecessary relocations
for (ShardRouting shardRouting : routings.unassigned()) {
    var lastAllocatedNodeId = shardRouting.unassignedInfo().lastAllocatedNodeId();
    if (knownNodeIds.contains(lastAllocatedNodeId)) {
        targetNodes.add(lastAllocatedNodeId);
    }
}
```

```java
// DesiredBalanceShardsAllocator.java:331-334
// Starts reconciliation towards desired balance that might have not been updated with a recent calculation yet.
// This is fine as balance should have incremental rather than radical changes.
// This should speed up achieving the desired balance in cases current state is still different from it (due to THROTTLING).
reconcile(currentDesiredBalanceRef.get(), allocation);
```

```java
// DesiredBalanceInput.java:24
 * @param ignoredShards     a list of the shards for which earlier allocators have claimed responsibility
```

### Part 6

```java
// WeightFunction.java:89-92
final float weightShard = nodeNumShards - avgShardsPerNode;
final float ingestLoad = (float) (nodeWriteLoad - avgWriteLoadPerNode);
final float diskUsage = (float) (diskUsageInBytes - avgDiskUsageInBytesPerNode);
return theta0 * weightShard + theta2 * ingestLoad + theta3 * diskUsage;
```

```java
// WeightFunction.java:52-60
public WeightFunction(float shardBalance, float indexBalance, float writeLoadBalance, float diskUsageBalance) {
    float sum = shardBalance + indexBalance + writeLoadBalance + diskUsageBalance;
    if (sum <= 0.0f) {
        throw new IllegalArgumentException("Balance factors must sum to a value > 0 but was: " + sum);
    }
    theta0 = shardBalance / sum;
```

```java
// BalancedShardsAllocator.java:110-144 (defaults only)
"cluster.routing.allocation.balance.shard",      0.45f
"cluster.routing.allocation.balance.index",      0.55f
"cluster.routing.allocation.balance.write_load", 10.0f
"cluster.routing.allocation.balance.disk_usage", 2e-11f
"cluster.routing.allocation.balance.threshold",  1.0f   // min 1.0f
```

### Part 7

```java
// DesiredBalanceReconciler.java:164-177
// 1. allocate unassigned shards first
allocateUnassigned();
// 2. move any shards that cannot remain where they are
moveShards();
// 3. move any other shards that are desired elsewhere
// This is the rebalancing work. The previous calls were necessary, to assign unassigned shard copies, and move shards that
// violate resource thresholds. Now we run moves to improve the relative node resource loads.
DesiredBalanceMetrics.AllocationStats allocationStats = balance();
```

```java
// DesiredBalanceComputer.java:226-228
// Here existing shards are moved to desired locations before initializing unassigned shards because we prefer not to leave
// immovable shards allocated to undesirable locations (e.g. a node that is shutting down or an allocation filter which was
// only recently applied). In contrast, reconciliation prefers to initialize the unassigned shards first.
```

```java
// OrderedShardsIterator.java:21-26
/**
 * This class iterates all shards from all nodes.
 * The shard order is defined by
 * (1) allocation recency: shards from the node that had a new shard allocation would appear in the end of iteration.
 * (2) shard priority: dictated by the provided {@link ShardRelocationOrder}.
 */
```

```java
// DesiredBalanceReconciler.java:74-80
public static final Setting<Double> UNDESIRED_ALLOCATIONS_LOG_THRESHOLD_SETTING = Setting.doubleSetting(
    "cluster.routing.allocation.desired_balance.undesired_allocations.threshold",
    0.1,
```

### Part 8

```java
// DiskThresholdSettings.java:39-40, 61-62, 83-84
"cluster.routing.allocation.disk.watermark.low",         "85%"
"cluster.routing.allocation.disk.watermark.high",        "90%"
"cluster.routing.allocation.disk.watermark.flood_stage", "95%"
```

```java
// DiskThresholdMonitor.java:218-223
if (routingNode != null) { // might be temporarily null if the ClusterInfoService and the ClusterService are out of step
    for (ShardRouting routing : routingNode) {
        indicesToMarkReadOnly.add(routing.index());
        indicesNotToAutoRelease.add(routing.index());
    }
}
```

```java
// DiskThresholdDecider.java:208-213
"the node has fewer free bytes remaining than the total size of all incoming shards: "
    + "free space [%sB], relocating shards [%sB]",
freeBytes + sizeOfRelocatingShards,
sizeOfRelocatingShards
```

### Part 9

```java
// UnassignedInfo.java:81-89
public static final Setting<TimeValue> INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING = Setting.timeSetting(
    "index.unassigned.node_left.delayed_timeout",
    settings -> EXISTING_SHARDS_ALLOCATOR_SETTING.get(settings).equals("stateless")
        ? TimeValue.timeValueSeconds(10)
        : TimeValue.timeValueMinutes(1),
```

```java
// SingleNodeShutdownMetadata.java:96, 135-137, 483-487, 502-507
public static final TimeValue DEFAULT_RESTART_SHARD_ALLOCATION_DELAY = TimeValue.timeValueMinutes(5);

if (allocationDelay != null && Type.RESTART.equals(type) == false) {
    throw new IllegalArgumentException("shard allocation delay is only valid for RESTART-type shutdowns");
}

public enum Type {
    REMOVE,
    RESTART,
    REPLACE,
    SIGTERM; // locally-initiated version of REMOVE

    public boolean isRemovalType() {
        return switch (this) {
            case REMOVE, SIGTERM, REPLACE -> true;
            case RESTART -> false;
        };
    }
```

### Part 10

```java
// TransportClusterAllocationExplainAction.java:166-168
throw new IllegalArgumentException(Strings.format("""
    There are no unassigned shards in this cluster. Specify an assigned shard in the request body to explain its \
    allocation. See %s for more information.""", ReferenceDocs.ALLOCATION_EXPLAIN_API));
```

```java
// AllocateEmptyPrimaryAllocationCommand.java:142-148
if (shardRouting.recoverySource().getType() != RecoverySource.Type.EMPTY_STORE && acceptDataLoss == false) {
    String dataLossWarning = "allocating an empty primary for ["
        + index + "][" + shardId
        + "] can result in data loss. Please confirm by setting the accept_data_loss parameter to true";
    return explainOrThrowRejectedCommand(explain, allocation, dataLossWarning);
}
```

```java
// ClusterRerouteRequest.java:80-85
/**
 * Sets the retry failed flag (defaults to {@code false}). If true, the
 * request will retry allocating shards that can't currently be allocated due to too many allocation failures.
 */
public ClusterRerouteRequest setRetryFailed(boolean retryFailed) {
```
