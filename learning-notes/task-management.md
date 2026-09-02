# Elasticsearch task management

Notes from a teaching session, 1 Sep 2026. This covers the **Task Management** topic of the
ES-Distributed knowledge-sharing list and supports `docs/internal/DistributedArchitectureGuide.md`
lines 2778–3085 (`# Task Management / Tracking`).

Every claim here was verified by reading source at the cited line. **Unlike the networking
notes, nothing here has been executed yet** — there is no captured output. The *Lab* section at
the end lists the experiments that would turn reading into evidence; run them before trusting
any claim that matters.

Line references are against Elasticsearch commit `cd4f46d0e292` (the upstream baseline of branch
`es-learning`). Line numbers drift — if one looks wrong, grep for the quoted code instead.

## How to use this if you've forgotten everything

- **Need a jog?** Read *The one-paragraph model* below, then *Counterintuitive findings* near the
  end.
- **Debugging a cancellation that didn't work?** Go straight to section 8 (cooperative
  cancellation) and then *Failure scenarios*. The answer is almost always "where does the loop
  check."
- **Need to find code?** The *ES file index* maps every file mentioned to why you'd open it.
- **Need to understand it again?** Sections 1–14 are in dependency order. Each ends with a
  one-sentence summary; those are the load-bearing ideas.
- **Someone (or some AI) is explaining this to you?** Check their claims against *Counterintuitive
  findings* first. Two of those are things a confident summary got wrong during this session.

---

## The one-paragraph model

A `Task` is an inert description of work happening somewhere else — it holds no thread and no
result. Every node keeps its own in-memory registry (`TaskManager`) of the tasks running on it.
Every inbound transport request becomes a task automatically, so a single user request becomes a
tree of tasks across nodes, linked by a parent `TaskId` stamped on each request. Cancelling a
task writes a string into a field; the running code must periodically look at that field and
choose to stop. To cancel a tree, the parent first *bans* new children on every node it sent work
to, then sweeps the children that already exist, and each of those does the same one level down.
Finished tasks vanish unless they explicitly opted to write their result to the `.tasks` index.
Persistent tasks are a different mechanism that shares the word: a durable record in cluster
state that keeps spawning ordinary tasks until someone removes it.

---

## 1. Why tasks exist

Elasticsearch hands work to thread pools and to other nodes, and loses the handle on it the moment
it does. Java's `Future` doesn't help: it lives in one JVM, can't say *what* the work is, and can't
cancel across the network.

Three things you fundamentally can't do without a tracker, and every part of the framework serves
one of them:

- **Visibility** — what is running right now, since when, on whose behalf.
- **Control** — make a specific piece of work stop.
- **Correlation** — know that this work on node B belongs to that request on node A.

**One sentence:** the tasks framework exists to see, stop, and relate work that has already been
handed off.

---

## 2. What a Task is — and isn't

**A `Task` does not contain the work.** No thread, no future, no callback, no result. It is a
description:

```java
// Task.java:91-112
private final long id;
private final String type;
private final String action;
private final String description;
private final TaskId parentTask;
private final Map<String, String> headers;
protected final long startTime;        // wall clock, for display
private final long startTimeNanos;     // monotonic, for duration
```

Two timestamps because wall-clock can jump when NTP adjusts; `startTimeNanos` never goes
backwards, so running time is computed from it (`Task.java:171`).

`CancellableTask` adds exactly one piece of mutable state:

```java
// CancellableTask.java:35-37
private volatile String reason;
private final SubscribableListener<Void> listeners = new SubscribableListener<>();
```

`isCancelled()` is `reason != null` (line 69–71). Cancelling a task means writing a string.
Nothing is interrupted. See section 8.

The write is a compare-and-set (line 48): cancel is idempotent, and the **first reason wins**. In
practice cancellation arrives from several directions at once (section 10), and the recorded
reason tells you which got there first.

`type` is a loose category string whose javadoc is wrong — see section 5. `action` is the field
that carries meaning (e.g. `indices:data/read/search`).

**One sentence:** a task is metadata about work plus, if cancellable, a flag that running code
may or may not look at.

---

## 3. Identity

Local IDs come from a plain counter, fresh on every process start:

```java
// TaskManager.java:78
private final AtomicLong taskIdGenerator = new AtomicLong();
```

Unique on one node for one process lifetime. Not unique across nodes (every node has its own
counter) and not across restarts (back to zero). Same collision as transport request IDs.

`TaskId` pairs it with the **persistent** node ID (`TaskId.java:26-34`), serialized as
`{nodeId}:{localId}`, e.g. `oTUltX4IQMOUUVeiohTt8A:124`. The node ID comes from
`nodeEnvironment.nodeId()` (`NodeConstruction.java:773`), stored on disk, so it survives restarts.

Consequence: a `TaskId` is unique across the cluster *at any instant*, and **repeats over time**.
After a restart the same node will eventually mint `…:124` again for something unrelated. Safe for
"cancel the thing running now"; unsafe as a historical key. Section 12 and *Failure scenarios*
show where that bites.

Wire micro-optimisation worth knowing exists: `EMPTY_TASK_ID` (no parent) is by far the most
common value and is encoded as an empty string with no long following (`TaskId.java:80-87`),
saving 8 bytes on every transport request.

**One sentence:** `TaskId` = persistent node ID + a per-process counter, unique now and not
forever.

---

## 4. The registry: `TaskManager`

One per node, entirely in memory. If the node dies, every task on it is gone — correctly, because
the work is gone too.

### Two maps that don't overlap

```java
// TaskManager.java:74-76
private final Map<Long, Task> tasks = ...;
private final CancellableTasksTracker<CancellableTaskHolder> cancellableTasks = ...;
```

`tasks` is **not** "all tasks." It holds only non-cancellable ones. Registration is an if/else
(`register`, lines 168–176), deregistration is an if/else (`unregister`, lines 338–354), and
`getTasks()` has to union the two by hand (lines 478–484). If they overlapped, that union loop
would be dead code.

Why two structures: `tasks` only answers "list everything," so a hash map suffices.
`cancellableTasks` must answer "all children of parent X" and "the child of parent X that came
from request Y" quickly, concurrently, on the cancellation path — so it maintains secondary
indexes, and you only pay for those on tasks that could actually be cancelled.

### The request builds its own task

```java
// TaskManager.java:155-162
Task task = request.createTask(new TaskId(nodeId, taskIdGenerator.incrementAndGet()), type, action,
    request.getParentTask(), headers);
```

`createTask` is on `TaskAwareRequest` with a default returning a plain `Task`
(`TaskAwareRequest.java:51-53`). A request that needs progress reporting returns something with a
`getStatus()`; a request that needs to be cancellable returns a `CancellableTask`. There is no
flag and no config: **a task is cancellable because its request's `createTask` said so.** Example:
`BulkRequest.createTask` returns `new CancellableTask(...)` (`BulkRequest.java:628-630`).

### Registration is paired, and can throw

Every `register` needs an `unregister`, done via the `Releasable` idiom wired into both success
and failure paths. And `register` **can throw**: if the parent is already banned, the new task is
cancelled, unregistered, and `TaskCancelledException` is thrown (lines 259–270). "Start work" and
"check whether this work is already unwanted" are the same operation. Scope caveat: that check is
inside `registerCancellableTask`, so a *non-cancellable* child of a banned parent runs anyway.

**One sentence:** two disjoint maps, the request decides what kind of task it gets, and
registering a doomed child fails immediately.

---

## 5. Where tasks come from

About a dozen production call sites in the whole codebase; four doors matter.

| door | code | type string | what |
| --- | --- | --- | --- |
| 1 | `RequestHandlerRegistry.java:76-77` | `"transport"` | every inbound transport request, unconditionally |
| 2 | `NodeClient.java:107-113` (`registerAndExecute`) | `"transport"` | work this node starts itself — REST handlers, internal action calls |
| 3 | `MasterService.java:387` | `"master"` | cluster state updates (constructs an anonymous `TaskAwareRequest` just to satisfy the API) |
| 4 | `PersistentTasksNodeService.java:267` | `"persistent"` | the running execution of a persistent task (section 13) |

Doors 1 and 2 together make the tree: one user search → one door-2 task on the coordinator →
one door-1 task on every shard's node.

Tail of hand-registered special cases: `PrimaryReplicaSyncer.java:195`,
`RetentionLeaseSyncAction.java:119`, `RetentionLeaseBackgroundSyncAction.java:104`; in x-pack the
async-search wrapper and `InternalExecutePolicyAction.java:145` (`"enrich"`).

### The `type` field's javadoc is wrong

`Task.getType()` javadoc (`Task.java:188-191`) says `(netty, transport, direct)`. Grepping every
`taskManager.register(` in `server`, `modules`, `x-pack`: production values are `"transport"`,
`"master"`, `"persistent"`, `"enrich"`. Neither `"netty"` nor `"direct"` appears anywhere. And:

```java
// PrimaryReplicaSyncer.java:195
taskManager.register("transport", "resync", request); // it's not transport :-)
```

`type` is vestigial. Nothing branches on it. Read `action`. (Fixing that javadoc is a two-line
first PR.)

**One sentence:** tasks are born at a handful of doors, almost all of them typed `"transport"`,
and the `type` field means little.

---

## 6. How context travels with the work

The carrier is `ThreadContext`: a map of headers attached to the current thread. On its own it is
lost at the first thread hop, so there are three propagation steps.

1. **Thread → thread.** Submitting to an ES `ThreadPool` executor wraps the runnable so the
   submitter's context is captured and restored on the worker. Automatic if you use the ES pool.
2. **Node → node.** Headers are serialized into the transport message's variable header (the same
   frame section that carries the action name) and restored into the handler thread's context on
   arrival. Sending to a *remote cluster* deliberately strips `X-Elastic-Project-Id`
   (`ThreadContext.java:331-335`).
3. **Context → task.** At `register`, a whitelisted subset is copied onto the `Task`
   (`TaskManager.java:145-154`), with a total size cap.

The whitelist is built once at startup: plugin-contributed headers ∪ `Task.HEADERS_TO_COPY`
(`NodeConstruction.java:768-771`). The five built-ins: `X-Opaque-Id`, `traceparent`, `trace.id`,
`X-elastic-product-origin`, `X-Elastic-Project-Id` (`Task.java:83-89`).

**Why copy onto the task when the context has them:** the context belongs to a thread at an
instant; the task outlives the thread. Three seconds later a `_tasks` call arrives on a different
thread asking "everything for `X-Opaque-Id: kibana-dashboard-7`." Only the copy on the task can
answer. The context is the transport; the task's header map is the record.

Why the whitelist is small and capped: headers are copied onto *every task in the tree*, so a fat
header costs memory once per node per shard, not once per request.

**One sentence:** `ThreadContext` carries headers across threads and the wire; a small whitelist
is stamped onto each task so the record outlives the thread.

---

## 7. The tree — parent/child

### Building the link is one line

```java
// TransportService.java:1082-1083 (sendChildRequest)
request.setParentTask(localNode.getId(), parentTask.getId());
sendRequest(connection, action, request, options, handler);

// ParentTaskAssigningClient.java:64-66 — wrap a client once, every request gets the parent
request.setParentTask(parentTask);
super.doExecute(action, request, listener);
```

The child carries a pointer *upward*. Nothing points downward — deliberately.

### The parent can't know its children, so it tracks connections

At send time the parent knows *where* it sent the request but not the child's task ID — that is
minted on the remote node later. So the parent keeps a **count of outstanding children per
connection** (`Transport.Connection` = the logical link to one peer, the 13-channel bundle):

```java
// TaskManager.java:378-390 registerChildConnection — increments; returned Releasable decrements
// TaskManager.java:750-755 unregisterChildConnection — removes the connection at zero
```

Only *cancellable* parents do this (`cancellableTasks.get(taskId)` returns null otherwise), because
the per-connection set has exactly one consumer: cancellation.

### Knowledge is split

- Parent knows **which nodes** currently have ≥1 of its children.
- Each node knows **which of its own tasks** have a given parent (the `cancellableTasks` index).

Neither side has the whole tree; neither needs it. Section 9 uses exactly this split.

### The parent is whoever's ID is on the request, not whoever sent it

Usually the same. Not for replication: the shard request's parent is stamped once at the
coordinator (`TransportReplicationAction.java:927-929`), the primary forwards the *same request
object* to replicas wrapped in `ConcreteReplicaRequest`, whose `getParentTask()` passes it through
(lines 1518–1521). So `bulk[s][p]` and `bulk[s][r]` are **siblings** under `bulk[s]`, even though
the primary sends the replica request.

### Depth is a property of the operation

```
bulk (coordinator)                        reindex (coordinator)
└── bulk[s]  (coordinator, per shard)     ├── search (scroll)
    ├── bulk[s][p]  (primary node)        │   └── search[phase/query]  (each source shard)
    └── bulk[s][r]  (replica node)        └── bulk
                                              └── bulk[s]
                                                  ├── bulk[s][p]
                                                  └── bulk[s][r]
```

Reindex wraps its clients in `ParentTaskAssigningClient` (`Reindexer.java:294-295`), so the write
branch is four deep and the read branch three. A cross-cluster hop inserts a proxy task in the
middle (see *Proxy* below).

### Proxy connections (cross-cluster)

Inside one cluster every node connects to every other. Across clusters you connect to a few
gateway nodes only. To reach a remote node you're not connected to, you hand the request to one you
are, wrapped with "forward this to node X." `RemoteConnectionManager.ProxyConnection`
(`RemoteConnectionManager.java:307-330`) pretends to be a connection to the target but sends over
the real connection with the action renamed `internal:transport/proxy/<action>`
(`TransportActionProxy.java:205`) and the request wrapped.

The intermediate node registers an ordinary door-1 task for the proxy action, then stamps *its
own* task as parent of the forwarded request (`TransportActionProxy.java:61-62`). So:

```
T1 (your search) ──► T2 (proxy task, gateway node) ──► T3 (real shard search)
```

T3's parent is T2, not T1. A cancel of T1 must go to the *gateway*, which is why
`registerChildConnection` records the *unwrapped* connection (`TransportService.java:936-948`).
Record the wrong one and cross-cluster cancellation silently does nothing.

**One sentence:** children point up via a `TaskId` on the request; parents remember only which
nodes hold children; the parent is whoever's ID is on the request.

---

## 8. Cooperative cancellation — what "cancel" actually does

The complete list of effects:

```java
// CancellableTask.java:46-53
final void cancel(String reason) {
    if (REASON_HANDLE.compareAndSet(this, null, reason) == false) return;
    listeners.onResponse(null);
    onCancelled();
}
```

Write the string, fire listeners, call a hook. **No thread is interrupted.** The running code
carries on until it chooses to look. `Thread.interrupt()` is deliberately not used: interrupting
mid-Lucene-write or mid-file-copy leaves things half-done; only the code doing the work knows where
its safe stopping points are.

### The contract has two sides

Framework: set the flag, fire listeners, exactly once, first reason wins. Always kept.

Task: **periodically check, and stop if set.** If it never checks, cancellation is a no-op and the
framework is not at fault.

Three ways to check:

- **Poll.** `ensureNotCancelled()` throws `TaskCancelledException` (lines 102–106) — for
  synchronous loops. `notifyIfCancelled(listener)` fails the listener and returns `true` (lines
  112–118) — for callback-style code with nothing to throw into.
- **Push.** `addListener(CancellationListener)` (lines 84–86) — get called the instant it happens;
  for code waiting on I/O it can't poll from.
- **Hook.** Override `onCancelled()` (line 97) in a `CancellableTask` subclass.

### Example: search checks every 2,048 documents

```java
// ContextIndexSearcher.java:611-624 (interval at line 72: 1 << 11)
if (++seen % CHECK_CANCELLED_SCORER_INTERVAL == 0) checkCancelled.run();

// SearchService.java:1058-1062 — the check is just the poll
context.searcher().addQueryCancellation(() -> { if (task != null) task.ensureNotCancelled(); });
```

Controlled by `search.low_level_cancellation`, default `true`, dynamic (`SearchService.java:227`).
Every cancellable operation has made this tradeoff: how often to look versus what looking costs.

### The debugging rule

A cancel that "succeeded" while CPU stays hot is almost never the machinery. Find the loop doing
the work and ask **where does it check.**

**One sentence:** cancel writes a string; the work stops only when its own code reads that string
and decides to.

---

## 9. The ban protocol — crossing nodes

### The race the naive version leaks

"Send cancel-all-children to each node in my connection set" misses anything **in flight**: a
child request sent a millisecond before the cancel, arriving a millisecond after, registers
normally and runs to completion. You cannot fix this by being faster.

### Shut the door first, then clear the room

1. **Latch locally and snapshot.** `startBan` (`TaskManager.java:765-790`) sets
   `banChildrenReason`, then copies the connection set — both under one `synchronized`. From now
   on the parent's own `registerChildConnection` throws (lines 740–743), so no node can be added
   after the snapshot.
2. **Send the ban** — action `internal:admin/tasks/ban` (`TaskCancellationService.java:49`) — to
   every connection in the snapshot (lines 192–199). It is an ordinary transport request and
   therefore gets its own door-1 task.
3. **On each child node: latch, then sweep.** `BanParentRequestHandler` calls `setBan`
   (`TaskManager.java:568-583`), which records the parent in `bannedParents` *and* returns the
   children that already exist; each is then cancelled (`TaskCancellationService.java:360-368`).
   Late arrivals die at registration (`TaskManager.java:259-270`).
4. **It recurses.** Each child is cancelled via the same `cancelTaskAndDescendants`, so it latches
   itself and bans *its* children. One level per hop; nobody holds the whole tree.

### A latch must be lifted

Left forever, `bannedParents` grows without bound and — since `TaskId`s repeat after restart — an
unrelated future task could be killed at birth. Two removal paths:

- **Tidy:** once all children on all nodes finish, the parent sends remove-ban to the same
  connections (`removeBanOnChildConnections`, `TaskCancellationService.java:242`).
- **Safety net:** the ban remembers the channel it arrived on; if that channel closes, the child
  node drops it (`TaskManager.java:881-884`). Local evidence, no message needed.

### `waitForCompletion`

`false` (default): return once bans are *in place* — no new work can start, existing work may still
be winding down. `true`: wait until every descendant has finished, bounded by the slowest child's
next cancellation check (`TaskCancellationService.java:160-166`).

**One sentence:** ban first so nothing new can start, then cancel what exists, recursively; lift
the ban when done or when the requester's channel dies.

---

## 10. The four triggers

| # | trigger | code | what gets cancelled | notes |
| --- | --- | --- | --- | --- |
| 1 | `POST _tasks/{id}/_cancel` | `TransportCancelTasksAction.java:264-278` | the task and its tree | calls `ensureCancellable()` first (can be refused); caller picks `waitForCompletion`; runs on `GENERIC` (lines 60–62) |
| 2 | HTTP client disconnects | `RestCancellableNodeClient.java:101-111` | the coordinating task and its tree | **opt-in per REST handler**; implemented *as* trigger 1 via the cancel API as system user |
| 3 | transport connection to requester dies | `TaskManager.java:863-875` | tasks that arrived on that channel, and their trees | fire-and-forget; reason is always `"channel was closed"` |
| 4 | a child request fails on the parent's side | `TransportService.java:1883-1885` → `internal:admin/tasks/cancel_child` → `cancelChildLocal` (`TaskCancellationService.java:411-413`) | **one specific child** | keyed by parent `TaskId` **and** transport request ID — surgical, does not cascade |

Triggers 2 and 3 are the same idea at two layers: each layer cancels what it was asked over the
connection that just died. Trigger 4 is why `cancellableTasks` stores the request ID alongside
each task (section 4) — purely so this lookup can be exact.

Handlers that call `client.execute(...)` directly instead of wrapping in
`RestCancellableNodeClient` get **no** disconnect cancellation. Search, msearch, kNN, most `_cat`
and many admin GETs are wrapped. If you write a heavy REST handler, reach for the wrapper.

**One sentence:** four doors — explicit, HTTP-disconnect, transport-disconnect, child-failure —
all ending in a `reason` string and, if there are children, the ban protocol.

---

## 11. The `_tasks` API

Three endpoints, two shapes. **List** and **cancel** fan out to nodes and merge
(`TransportTasksAction`). **Get** goes to one node and falls back to disk.

### What travels: `TaskInfo`

A `Task` never leaves its node; `Task.taskInfo(localNodeId, detailed)` (`Task.java:147-179`)
builds a snapshot. `?detailed=true` additionally calls `getDescription()` and `getStatus()`, and
the `getStatus()` javadoc warns it "might be a costly operation" (line 230–235). Detailed status of
everything on a busy cluster is not free.

### List

Per node, filter the local registry (`getTasks()` — the union from section 4), snapshot each match
(`TransportListTasksAction.java:117-119`). Filters: `actions` (glob), `parent_task_id`, `nodes`,
`group_by=parents`. `wait_for_completion` waits for matched tasks to be unregistered, excluding
list-tasks itself (lines 401–406) — because the list request is a task and its fan-outs are its
children, so it would otherwise wait for itself forever.

### Get: memory first, then `.tasks`

```java
// TransportGetTaskAction.java:172-176
Task runningTask = taskManager.getTask(request.getTaskId().getId());
if (runningTask == null) getFinishedTaskFromIndex(...);
```

Also falls through to `.tasks` if the node named in the `TaskId` is gone from the cluster (lines
137–140) or unreachable (lines 146–150). This is the only seam where the framework switches from
memory to disk.

### Cancel fans out everywhere

`resolveNodes` returns all nodes (`TransportCancelTasksAction.java:89-93`), because a relocated
task (section 14) may not be where its ID says. Double-broadcasts for relocatable actions.

**One sentence:** live tasks are spread across nodes so list/cancel ask everyone; a finished task
exists only in `.tasks`, and only if it asked to be stored.

---

## 12. Storing results — the `.tasks` index

### Problem

Tasks vanish on completion. For "start this, give me a ticket, I'll come back tomorrow"
(`wait_for_completion=false`) the result needs somewhere to live.

### Where

A system index, `.tasks` (`TaskResultsService.java:57`), one document per stored task, **document
ID = the `TaskId` string** (`TaskResultsService.java:121`). Not cluster state: task results are
potentially large, numerous, and read once by one client — data, not coordination.

### Opt-in

```java
// ActionRequest.java:36-40
public boolean getShouldStoreResult() { return false; }
```

Set `true` by REST handlers when the user passes `wait_for_completion=false`, e.g.
`RestForceMergeAction.java:59`. If every door-1 task stored a result, `.tasks` would be the busiest
index in the cluster.

### Mechanics

`TransportAction.java:86-88` wraps the listener in `TaskResultStoringActionListener` →
`TaskManager.storeResult` (lines 404–465) → `task.result(localNode, response)` → indexed with
backoff. The response **must implement `ToXContent`** or you get `IllegalStateException`
(`Task.java:307`). Success and failure are both stored.

### Footnotes

- The `TaskId` caveat (section 3) returns: a post-restart task with the same ID that also stores
  its result **overwrites** the old document (plain index op). `storeResultIfAbsent` (create-only,
  conflict = success; `TaskResultsService.java:106-117`) exists, but for relocation (section 14).
- Reindex slices are created with `setShouldStoreResult(false)` — "Parent task will store result"
  (`AbstractBulkByPaginatedSearchRequest.java:514-515`). One document per user operation.

**One sentence:** `TaskManager` remembers now; `.tasks` remembers then, but only for tasks whose
request set `getShouldStoreResult()`.

---

## 13. Persistent tasks — same word, different animal

Everything above shares one property: **a task exists because work is already happening**, and it
dies with the node. Some work must *always* be running somewhere regardless of what dies — CCR
followers, the health node, system index migration, ML jobs.

### The record is the truth

A persistent task is a **record in cluster state** (`PersistentTasksCustomMetadata`): "this job
should run, with these params, currently assigned to node N." Cluster state is replicated and
survives restarts.

```
// PersistentTasksExecutor.java:28-31
An executor of tasks that can survive restart of requesting or executing node.
These tasks are using cluster state rather than only transport service to send requests and responses.
```

Three components:

- **Master assigns** — `PersistentTasksClusterService`, default: least-loaded data node
  (`PersistentTasksExecutor.java:66-67`).
- **Every node watches** — `PersistentTasksNodeService` is a `ClusterStateListener`; "assigned to
  me but not running → start" (lines 126–129), "running but no longer assigned → stop."
- **Starting = registering an ordinary task** — door 4, type `"persistent"`, action
  `<name>[c]` (line 267). `AllocatedPersistentTask extends CancellableTask`
  (`AllocatedPersistentTask.java:32`), so it appears in `_tasks` and everything in sections 2–10
  applies.

### Node dies → reassigned → new regular task

The execution on N is gone; the record still says "assigned to N"; the master sees N left and
reassigns to M; M starts a **brand-new regular task with a brand-new `TaskId`**. Continuous from the
cluster's view, two executions in reality.

**Two identities:** the record's own string ID (stable for the life of the job; used by
`PersistentTasksService` and e.g. CCR's follow API) versus the current execution's `TaskId`
(changes every move; what `_tasks` shows). Cancelling via `_tasks` cancels *this execution*; unless
the record is removed, the master may start another.

### What the framework does NOT do

- **Retry on reported failure.** `markAsFailed` → completion notification → master **removes the
  record** (`PersistentTasksClusterService.java:239-241`). Retry is the creator's job.
- **Detect a silent death.** No heartbeat, no liveness probe. `PeriodicRechecker`
  (`PersistentTasksClusterService.java:830`) only reassigns *unassigned* tasks. A task that's stuck
  on a healthy node is a zombie until monitoring or a human notices. See *Failure scenarios*.

### CCR in one paragraph

Cross-Cluster Replication: a **leader** index in cluster A, a **follower** in cluster B that
continuously pulls the leader's sequence-numbered operations and applies them. Uses: DR across
regions, reads close to users, fan-in from edge clusters. Each follower shard is a persistent
task — the canonical "must resume elsewhere on its own" job. Separate topic on the list.

### The two durable stores, side by side

- `.tasks` stores **results of finished work** — the past.
- Persistent-task metadata stores **intent for work that must not finish** — the future.
- `TaskManager` is only ever **now**.

**One sentence:** a persistent task is a durable cluster-state record that keeps spawning regular
tasks until someone removes it — and it trusts the executor's code to report its own failure.

---

## 14. Relocatable tasks — brief

Reindex can run for hours; under the normal model a node shutdown loses it. Relocation lets it
hand off to another node. Only reindex today:

```java
// TransportListTasksAction.java:68
public static final Set<String> RELOCATABLE_ACTIONS = Set.of(ReindexAction.NAME);
```

After handoff the work has a new `TaskId`, so `TaskInfo` carries `originalTaskId` /
`originalStartTimeMillis` (`Task.java:316-331`; for normal tasks it's just their own ID, line 176).

Things it explains: cancel fanning out to all nodes and double-broadcasting
(`TransportCancelTasksAction.java:43-44`); `ensureCancellable()` (refuse a cancel mid-handoff,
503); `storeResultIfAbsent` (source and destination may both store; first wins);
`GET _tasks/{id}?follow_relocations` (`TransportGetTaskAction.java:106-108`). Mechanics belong to
the Reindex topic.

---

## Failure scenarios (from questions asked in the session)

### The proxy (gateway) node dies mid-request

```
your node  ──conn A──►  gateway (T2)  ──conn B──►  target (T3)
```

Three uncoordinated local cleanups:

- **Your node:** conn A closes → outstanding handler fails with `NodeDisconnectedException` →
  `UnregisterChildTransportResponseHandler.handleException` (`TransportService.java:1883-1889`)
  tries `cancelChildRemote` to the dead gateway (fails harmlessly), decrements the connection
  count, hands the exception to the action. The action decides fail-vs-partial; the framework only
  reports.
- **Gateway:** dead. T2 was in memory; no record it existed.
- **Target:** conn B closes → `TaskManager.onChannelClosed` → T3 cancelled with
  `"channel was closed"` (trigger 3). Nobody told the target anything; the socket its request
  arrived on closed, and that is enough.

Caveat: "cancelled" = flag set. T3 *stops* only if its code checks (section 8).

### Healthy node, persistent task execution silently dead

Record says "assigned to N"; N is healthy; the `AllocatedPersistentTask` is `STARTED` in
`TaskManager`; no work is happening. Nothing in the framework notices (section 13). Signals:
ever-growing running time in `_tasks`; a status that stops changing if the executor publishes one.
Recovery: cancel via `_tasks` (executor must cooperate) or restart the node. Same contract as
cancellation — the framework can't distinguish "quiet and working" from "quiet and dead."

### Node restarts; does anyone read stale state from `.tasks`?

**No node reads `.tasks` on restart.** It's not a recovery journal; its only reader is
`GET _tasks/{id}` as a fallback. The counter does reset (field initialiser) and the node ID does
not (on disk), so `{nodeId}:{localId}` repeats. Two things can go wrong, both for a **client
holding an old ID**:

- **Read side, wrong not stale:** `GET _tasks/{old-id}` → node is up → memory lookup by local ID
  (`TransportGetTaskAction.java:173`) → if a *new* unrelated task holds that ID, the client gets
  its info with no error.
- **Write side:** the new task storing its result overwrites the old document.

Window is narrow (exact counter match while the old ID is still held) but real. `TaskInfo` carries
`startTime`, so a careful client could detect the mismatch; the API won't.

### Replica-write edge noticed while verifying — unconfirmed

`registerChildConnection` is keyed by **local task ID only** (`TransportService.java:948`). When
the primary forwards a replica request, that request's parent is `bulk[s]` on the *coordinator*,
so the primary's lookup finds nothing (or an unrelated local task with the same ID) and registers
no child connection. Reading suggests a cancel of `bulk[s]` would therefore ban the primary's node
but never the replica's. Possibly deliberate (a replica write shouldn't be cancellable once the
primary committed, or copies diverge). **Not tested. Treat as a hypothesis, not a finding.**

---

## Counterintuitive findings worth rereading

1. **A `Task` contains no work.** No thread, no future, no result. It is a label.
2. **Cancel writes a string.** Nothing is interrupted. The work stops only where its own code
   checks. Search checks every 2,048 docs.
3. **`TaskManager.tasks` is not all tasks.** It's the non-cancellable ones. Cancellable tasks
   live only in `cancellableTasks`; `getTasks()` unions them.
4. **The request builds its own task.** Cancellability is decided by `createTask`, not by a flag.
5. **`register` can throw.** A child of a banned parent dies at the door — if it's cancellable.
6. **`type`'s javadoc is wrong.** `netty` and `direct` never appear; `transport`, `master`,
   `persistent`, `enrich` do. Read `action`.
7. **Parents don't know their children's IDs.** They count children per *connection*.
8. **The parent is whoever's ID is on the request, not whoever sent it.** `bulk[s][p]` and
   `bulk[s][r]` are siblings.
9. **Cross-cluster inserts a task you didn't create.** T3's parent is the proxy task T2, so
   cancellation must go through the gateway.
10. **Ban first, sweep second.** Reversing the order leaks in-flight children.
11. **The ban request is itself a task.** Cancellation is tracked by the thing it cancels.
12. **HTTP-disconnect cancellation is opt-in per REST handler** (`RestCancellableNodeClient`).
13. **Trigger 4 is surgical.** Keyed by parent + transport request ID; cancels one child, no
    cascade.
14. **List-tasks excludes itself from `wait_for_completion`** or it would wait forever.
15. **`.tasks` is written only on opt-in** (`getShouldStoreResult()`), else every shard request
    would write a document.
16. **A stored response must implement `ToXContent`.**
17. **Persistent tasks don't retry on reported failure** — the record is removed.
18. **Persistent tasks have no liveness check.** A stuck executor on a healthy node is invisible.
19. **`TaskId` repeats after restart.** `GET _tasks/{old-id}` can return the *wrong* task, not a
    stale one.
20. **Cancel fans out to every node** because one action (reindex) can move.

---

## Questions asked during the session, and where the answers live

| question | section |
| --- | --- |
| What is a "proxy" here, in plain terms? | 7, *Proxy connections* |
| What if the proxy node dies? Does it track the task? | *Failure scenarios* §1; 7 |
| How deep is the tree? Multi-level example? | 7, *Depth is a property of the operation* |
| Healthy node but the persistent task silently failed? | 13, *What the framework does NOT do*; *Failure scenarios* §2 |
| What is CCR? | 13, *CCR in one paragraph* |
| Node restarts and reads stale `.tasks` state? | *Failure scenarios* §3; 3; 12 |

---

## ES file index

| File | Why |
| --- | --- |
| `docs/internal/DistributedArchitectureGuide.md` | Task Management section, lines 2778–3085 |
| `server/.../tasks/Task.java` | fields 91–112; `taskInfo` 147–179; `HEADERS_TO_COPY` 83–89; stale `getType` javadoc 188–191; `OriginalTaskInfo` 316–331 |
| `server/.../tasks/CancellableTask.java` | `reason` 35–37; `cancel` CAS 46–53; `ensureNotCancelled` 102; `notifyIfCancelled` 112; `addListener` 84 |
| `server/.../tasks/TaskId.java` | node+local pair 26–34; empty-ID wire shortcut 80–87 |
| `server/.../tasks/TaskAwareRequest.java` | `createTask` default 51–53 |
| `server/.../tasks/TaskManager.java` | two maps 74–76; `register` 128–177; ban check 259–270; `unregister` 338–361; `registerChildConnection` 378–390; `getTasks` 478–484; `setBan` 568–583; `Ban` 605–628; holder `startBan` 765–790; channel tracking 799–821; `onChannelClosed` 863–885 |
| `server/.../tasks/TaskCancellationService.java` | action names 49–52; `cancelTaskAndDescendants` ~120–176; ban send 178–240; unban 242–260; `BanParentRequestHandler` 350–375; `CancelChildRequestHandler` 409–415 |
| `server/.../tasks/TaskResultsService.java` | `.tasks` 57; doc ID = TaskId 121; `storeResultIfAbsent` 106–117 |
| `server/.../transport/RequestHandlerRegistry.java` | door 1, 76–85 |
| `server/.../client/internal/node/NodeClient.java` | door 2, `executeLocally` 102–114 |
| `server/.../cluster/service/MasterService.java` | door 3, line 387 |
| `server/.../persistent/PersistentTasksNodeService.java` | door 4, line 267; start-on-assignment 126–129 |
| `server/.../node/NodeConstruction.java` | task-header whitelist assembly 768–771 |
| `server/.../common/util/concurrent/ThreadContext.java` | remote-cluster header stripping 327–335 |
| `server/.../transport/TransportService.java` | child-connection registration 936–960; `sendChildRequest` 1074–1084; `UnregisterChildTransportResponseHandler` 1867–1900 |
| `server/.../client/internal/ParentTaskAssigningClient.java` | 60–67 |
| `server/.../transport/RemoteConnectionManager.java` | `ProxyConnection` 307–330 |
| `server/.../transport/TransportActionProxy.java` | proxy stamps itself as parent 61–63; prefix 205 |
| `server/.../rest/action/RestCancellableNodeClient.java` | trigger 2, 40–111 |
| `server/.../action/admin/cluster/node/tasks/cancel/TransportCancelTasksAction.java` | trigger 1, 264–278; fan-out 89–93 |
| `server/.../action/admin/cluster/node/tasks/list/TransportListTasksAction.java` | `RELOCATABLE_ACTIONS` 68; self-exclusion 401–406 |
| `server/.../action/admin/cluster/node/tasks/get/TransportGetTaskAction.java` | memory-then-`.tasks` 172–176; node-gone fallbacks 122–166 |
| `server/.../action/support/tasks/TransportTasksAction.java` | fan-out base, 51–153 |
| `server/.../action/support/TransportAction.java` | result-storing listener wrap 86–88 |
| `server/.../action/ActionRequest.java` | `getShouldStoreResult` default false 36–40 |
| `server/.../rest/action/admin/indices/RestForceMergeAction.java` | `wait_for_completion=false` → store result, 56–72 |
| `server/.../search/internal/ContextIndexSearcher.java` | 2,048-doc check interval 72; loop 611–624 |
| `server/.../search/SearchService.java` | `search.low_level_cancellation` 227; check wiring 1058–1062 |
| `server/.../action/support/replication/TransportReplicationAction.java` | parent stamped once 927–929; replica passes parent through 1518–1521 |
| `server/.../action/bulk/BulkOperation.java` | `bulk` → `bulk[s]` parent, 454–456 |
| `modules/reindex/.../Reindexer.java` | `ParentTaskAssigningClient` use 294–295 |
| `server/.../persistent/PersistentTasksExecutor.java` | javadoc 28–31; default assignment 66–67 |
| `server/.../persistent/PersistentTasksClusterService.java` | record removed on completion 239–241; `PeriodicRechecker` 830 |
| `server/.../persistent/AllocatedPersistentTask.java` | extends `CancellableTask` 32; `markAsFailed` 153 |

---

## Lab: experiments to convert reading into evidence

Nothing above has been run. These would close the gap, roughly in order of value. Two-node
cluster via `./gradlew run --nodes=2 -Dtests.es.xpack.security.enabled=false`.

1. **See the tree.** Start a slow search (large index, `?pre_filter_shard_size=1`, or a script
   query with a sleep), then `GET _tasks?detailed&group_by=parents`. Confirm coordinator task →
   per-shard children, with `parent_task_id` set. Then a bulk with `?refresh=wait_for` against an
   index with a replica and confirm `bulk[s][p]` and `bulk[s][r]` share the same parent (finding 8).
2. **`X-Opaque-Id` propagation.** Send a search with `-H 'X-Opaque-Id: lab-1'`, list tasks, confirm
   every task in the tree carries `headers.X-Opaque-Id: lab-1` on both nodes (section 6).
3. **Cooperative cancellation.** Run a long script-query search, `POST _tasks/{id}/_cancel`, then
   watch `_tasks`: the coordinator and children should disappear within roughly one 2,048-doc
   check interval. Set `search.low_level_cancellation: false` and repeat — cancellation should
   become noticeably slower or only take effect between phases (section 8).
4. **Trigger 2.** Start a long search with `curl` and Ctrl-C it. Confirm the tasks vanish and the
   node log shows reason `http channel [...] closed` (section 10).
5. **Watch the ban.** Enable the transport tracer for `internal:admin/tasks/*`
   (`transport.tracer.include`), cancel a fanned-out search, and read the `ban` and `cancel_child`
   messages. Confirm bans go to every node that had a child and the unban follows once children
   finish (section 9).
6. **`.tasks` opt-in.** `POST idx/_forcemerge?wait_for_completion=false`, note the returned
   `task`, wait, then `GET _tasks/{task}` — should come from `.tasks` (`GET .tasks/_doc/{task}` to
   confirm). Then a search with `wait_for_completion` semantics not available — confirm nothing is
   written (section 12).
7. **Trigger 3 via a test.** `kill -9` under `./gradlew run` tears down the whole task; instead
   read `TaskManagerTests` / `CancellableTasksTests` for the channel-closed assertions, same
   habit as the networking notes.
8. **Verify or refute the replica-write hypothesis.** Add a `logger.trace` (or breakpoint) in
   `TaskManager.registerChildConnection` and run a bulk against a replicated index; check whether
   the primary node ever registers a child connection for the replica request. This is the one
   claim in these notes I'd most want tested before repeating.

---

## Not covered

- **Reindex internals and relocation mechanics** — Reindex topic.
- **CCR internals** — CCR topic.
- **`CancellableTasksTracker`'s data structure** — read it when you need the indexes' complexity.
- **APM tracing hooks** (`maybeStartTrace`, `Traceable`) — orthogonal to the task model.
- **Async search's task wrapper** (`AsyncTaskManagementService`) — x-pack, builds on section 12.
