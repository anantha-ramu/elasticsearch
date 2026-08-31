# Netty fundamentals, and how Elasticsearch uses them

Notes from a hands-on lab session, 30 Aug 2026. Every claim here was verified against
source and most were demonstrated with a running program; the captured output is
included so the evidence survives even if the reasoning is forgotten.

Elasticsearch line references are against commit `cd4f46d0e292` on branch `es-learning`.
Line numbers drift — if one looks wrong, grep for the quoted code instead.
Netty references are against 4.1.135.Final.

Context for why this exists: this was a detour from the **Network** topic of the
ES-Distributed knowledge-sharing list. The guide section it supports is
`docs/internal/DistributedArchitectureGuide.md` lines 13–240.

---

## 0. Recreating the lab

The Netty jars are already in the Gradle cache from ES builds, so no new project is
needed. Compiling directly against them gives a two-second edit-run loop.

```bash
export NETTY_CP=$(find ~/.gradle/caches/modules-2/files-2.1/io.netty \
  -name "*4.1.135.Final.jar" ! -name "*sources*" ! -name "*javadoc*" | tr '\n' ':')

mkdir -p ~/netty-lab && cd ~/netty-lab
# save EchoServer.java and Tap.java from the appendix below
javac -cp "$NETTY_CP" *.java && java -cp "$NETTY_CP:." EchoServer
```

Connect with `nc localhost 9999`. Full source is in the appendix.

Netty source is worth having open too — the sources jars are cached:

```bash
mkdir -p /tmp/nettysrc && cd /tmp/nettysrc
for j in netty-common netty-transport netty-codec netty-handler netty-codec-http; do
  unzip -o -q "$(find ~/.gradle/caches/modules-2/files-2.1/io.netty/$j/4.1.135.Final \
    -name '*sources.jar' | head -1)"
done
```

---

## 1. Event loops

**The idea.** An `EventLoop` is one thread running a loop: ask the OS which registered
sockets are ready, dispatch an event for each, drain a queue of submitted tasks, repeat.
An `EventLoopGroup` is a fixed pool of these. A server bootstrap takes two: the **boss**
group, which only accepts new connections, and the **worker** group, which handles all
traffic afterwards.

A `Channel` (one connection) is assigned to exactly one `EventLoop` **for its entire
life**. Every event for it — active, read, write, close — runs on that same thread, in
order. That is why handler code needs no locks. The flip side is that many channels
share each loop, so blocking one thread freezes every connection on it.

**The experiment.** Two worker threads, three connections, log the thread name on every
event.

```
ACTIVE  conn=0ddf0116  thread=nioEventLoopGroup-3-1
ACTIVE  conn=210a471b  thread=nioEventLoopGroup-3-2
ACTIVE  conn=c376631b  thread=nioEventLoopGroup-3-1
READ    conn=0ddf0116  thread=nioEventLoopGroup-3-1
READ    conn=210a471b  thread=nioEventLoopGroup-3-2
READ    conn=c376631b  thread=nioEventLoopGroup-3-1
CLOSED  conn=c376631b  thread=nioEventLoopGroup-3-1
CLOSED  conn=210a471b  thread=nioEventLoopGroup-3-2
CLOSED  conn=0ddf0116  thread=nioEventLoopGroup-3-1
```

Three connections, full lifecycle, never hopping threads. Two of them share `3-1`.
**The boss thread never appears** — `.childHandler(...)` installs handlers on *accepted*
channels only, and those live on workers.

**The cost of the pin.** Same setup with a 10-second `Thread.sleep` in `channelRead`:

```
20740ms  READ  8de4c2b7  (3-1)
23265ms  READ  2db8f091  (3-2)   <- the loner, unaffected
30744ms  READ  e99e309d  (3-1)
```

The two reads on `3-1` finished exactly 10004 ms apart even though both were typed
within a few seconds. The second one's bytes sat readable in the kernel the whole time:
a thread inside `Thread.sleep()` is neither in `select()` nor dispatching, so nobody was
home to collect them. The loner on `3-2` finishing *in between* is what proves the stall
is per-event-loop rather than server-wide.

This is the origin of ES's `ThreadWatchdog`, which logs a warning when a single task on a
transport thread runs longer than five seconds.

### Channels: parent vs child, boss vs worker

These are two different axes and conflating them makes `Netty4HttpServerTransport`
unreadable.

|                  | listening socket         | connection socket    |
| ---------------- | ------------------------ | -------------------- |
| Netty class      | `NioServerSocketChannel` | `NioSocketChannel`   |
| how many         | one per bound port       | one per client       |
| carries data     | no                       | yes                  |
| registered to    | boss group               | worker group         |
| pipeline set by  | `.handler(...)`          | `.childHandler(...)` |

A `Channel` is just a socket. A TCP server always has two kinds: one listener that never
carries data and only produces new sockets, and one connection socket per client.
Parent/child names *which socket*; boss/worker names *which thread*.

Verified live: all three child channels printed the same
`parent=[id: 0x2316d4e0, L:/[0:0:0:0:0:0:0:0]:9999]`.

**One sentence:** a channel is a socket pinned for life to one event loop thread, which
is why handler code needs no locks and why blocking that thread freezes every other
socket sharing it.

---

## 2. The pipeline

**The idea.** A `ChannelPipeline` is an ordered list of handlers belonging to one
channel. Each does one small job — strip TLS, parse HTTP bytes, decompress, enforce a
size limit — and the message changes type as it travels. That is how a TCP byte stream
becomes a `RestRequest`.

Inbound events (data arriving, active, close) flow **head → tail**. Outbound operations
(write, flush, close, read) flow **tail → head**. So `addLast(A)` then `addLast(B)` means
A sees incoming data first, but B sees outgoing data first.

**The experiment.** Pipeline `head → A → B → C → O → E → tail`, where A/B/C are duplex
taps, O is an outbound-only logger and E is the echo handler:

```
IN   A
IN   B
IN   C
READ    conn=15309b74
OUT WRITE       (that's O)
OUT  C
OUT  B
OUT  A
OUT FLUSH
```

Inbound A→B→C, outbound C→B→A. `OUT WRITE` precedes `OUT C` because `E` called
`ctx.writeAndFlush(msg)`, which starts travelling from `E`'s position toward the head,
and `O` is the first outbound handler it meets.

Two rules that fall out of this:

- A handler only participates in the direction it implements. `E` (inbound-only) is
  invisible to writes; `O` (outbound-only) is invisible to reads. In the leak report from
  lesson 3, the inbound handoff went straight from `Tap#2` to the echo handler, skipping
  the outbound logger entirely.
- A handler that forgets `ctx.fireChannelRead(msg)` or `ctx.write(msg, promise)` silently
  swallows the event.

**One sentence:** a pipeline is an ordered chain of small handlers where inbound runs
head-to-tail and outbound runs tail-to-head, and each handler only sees the directions it
implements.

---

## 3. ByteBuf and reference counting

**Why `ByteBuf` exists** rather than `byte[]` or `java.nio.ByteBuffer`: separate reader
and writer indices (no `flip()`); pooling, because allocating a buffer per socket read
would generate crushing garbage; and off-heap storage, so the kernel can read straight
into it.

**Why pooling forces reference counting.** A pooled buffer is *lent*, not given, so the
pool must know when you are done. GC cannot answer that — it fires far too late and does
not manage off-heap memory at all. So every `ByteBuf` carries a `refCnt`, starting at 1.
`retain()` increments, `release()` decrements, and at zero the memory returns to the pool.

**The ownership convention:** passing a message along the pipeline transfers ownership
*without changing the count*. Whoever stops passing it on must release it. On the
outbound side Netty releases the buffer after the socket write, which is why a plain echo
handler never calls `release()` and never leaks.

Verified: `refCnt=1` at every one of the nine hops, in both directions. Also
`cap=2048` for a 5-byte payload — you get a slice the pool had lying around, not a buffer
sized to your data. And `ridx=0` even at `OUT A`, because handlers only forward the write
*request*; the socket write happens at the head, after the last handler.

### Failure mode 1: leak

Consume a buffer and neither forward nor release it. Nothing fails immediately; the node
slowly starves of memory. Because the memory is off-heap (`newDirectBuffer` in the
report), **no heap metric shows anything wrong.**

Reproduced by deleting `ctx.writeAndFlush(msg)`. With
`ResourceLeakDetector.setLevel(PARANOID)` the report is a chain of custody, printed
newest-first:

```
Created at:  AdaptiveByteBufAllocator.newDirectBuffer   <- born, off-heap
      #4     NioSocketChannel.doReadBytes               <- kernel bytes written in
      #3     Hint: 'Tap#0' will handle...               <- head handed to A
      #2     Hint: 'Tap#1' will handle...               <- A handed to B
   (dropped)                                            <- B handed to C
      #1     Hint: 'EchoServer$1$2#0' will handle...    <- C handed to the echo handler
                                                        <- trail ends, never released
```

**The last `Hint` names the culprit.** To read the report, count the
`org.example.Tap.channelRead` frames in each record: zero frames means head→A, one means
A→B, three means C→echo.

On the "1 leak records were discarded" footer: `TARGET_RECORDS` defaults to 4
(`ResourceLeakDetector.java:49`, overridable via
`-Dio.netty.leakDetection.targetRecords`). The drop logic at `ResourceLeakDetector.java:477-487`
only ever splices out the **current head** (`prevHead = oldHead.next`), and the drop
probability is `nextInt(1 << (numElements - TARGET_RECORDS)) != 0` — which is *exactly
zero* at the fourth record and one-half at the fifth. So with five accesses the only
droppable record is the fourth, and whether it is dropped at all is a coin flip. Setting
`targetRecords=10` makes all five appear.

The design keeps the oldest records permanently (origin) and the newest always (where the
trail died), thinning the middle increasingly aggressively.

### Failure mode 2: use-after-free — the dangerous one

The realistic version of this bug is not calling `release()` too early. It is **handing a
buffer downstream, which transfers ownership, and then keeping your own reference.**

Reproduced by stashing the previous message's buffer in a field and printing it on the
next message. Typed `123`, then `456`:

```
  STALE  refCnt=1  content=[456]
```

Not `123`. Not an exception. `refCnt=1`.

The pool had recycled that exact `AdaptiveByteBuf` instance for the next socket read, so
the stale reference and the live buffer were **the same object**. Because it had been
legitimately reissued, `refCnt` was 1, `ensureAccessible()` passed, and the paranoid leak
detector said nothing — there was no leak; the buffer was released exactly on time. The
bug lives entirely in application code holding a reference it gave away, and no runtime
check in Netty can see it.

**Why this is worse than a leak.** A leak is loud and bounded. This is silent, and on a
real node one event loop's allocator pool serves hundreds of connections — so the
recycled buffer plausibly carries *a different client's request*. That is data
disclosure, not just incorrectness.

### What ES does about both

- Paranoid leak detection in **all** tests: `ESTestCase.java:464`, and test clusters at
  `ElasticsearchNode.java:841-843`. Log appenders watch for leak messages
  (`ESTestCase.java:319`) and the build **fails** on them (`ElasticsearchNode.java:1163`).
  Tests also force the pooled allocator deliberately, to maximise the chance of catching
  these.
- `Netty4LeakDetectionHandler.java:30-38` enriches the `Hint` via `content.touch(info)`
  with `method`, `uri` and `x-opaque-id`, so a CI leak report names the failing request
  and (because integ tests put the test name in the opaque ID) the failing test.
- `TrashingByteBuf.java:31-38` zero-fills a buffer's contents on final release, via
  `NettyAllocator.java:386-394`. Assertion-gated at `NettyAllocator.java:135-139`. With
  this in place, the `STALE` experiment above would print zeros — obviously broken output
  that an assertion trips on — instead of plausible data from another request.

Note the symmetry: the leak detector catches *released too late*; trashing catches *used
too late*. Both are runtime substitutes for a type system that cannot express ownership.

**One sentence:** buffers are borrowed from a pool, `refCnt` tracks the loan, passing a
buffer downstream hands over the loan, and keeping your own reference afterwards gives
you a window onto whatever the pool lends out next.

---

## 4. Futures and promises

**The idea.** An async call hands you a *receipt*, not a result. `ctx.writeAndFlush(msg)`
returns a `ChannelFuture`; when it returns, nothing has been written. Because blocking on
an event loop is forbidden, you attach a listener rather than calling `get()`. A
`Promise` is the writable side — whoever does the work completes it.

The `promise` parameter threaded through a handler's `write` method is the *original
caller's* promise. A handler that accepts one and never passes it on leaves the caller
waiting forever, with no error.

**Failure is delivered to the future, not thrown.** It does not reach `exceptionCaught`
and is not logged. If nobody listens, the outcome is discarded.

**The experiment.** `ctx.close()` followed by `ctx.writeAndFlush(msg)`. Without a
listener: the write travels the whole pipeline (`OUT WRITE`, `OUT C/B/A`, `OUT FLUSH`),
the connection drops, and there is **no evidence anywhere** that anything failed. With
`.addListener(...)`:

```
  write done: success=false  cause=io.netty.channel.StacklessClosedChannelException
 handler returned normally
```

Two things to notice. The pipeline forwarded the write happily — handlers do not check
whether a write can succeed; only the head knows. And the listener line printed *before*
`handler returned normally`, meaning it ran synchronously inside the `writeAndFlush`
call: the channel was already closed, so the promise was already complete when
`addListener` was called, and `addListener` on a settled future invokes immediately.

> **"Asynchronous" does not mean "later."** A listener can run re-entrantly inside the
> call you are making. Code that assumes otherwise is wrong.

### What ES does about it

- A forbidden-apis rule bans the raw call —
  `modules/transport-netty4/forbidden/netty-signatures.txt:8-9` bans
  `io.netty.channel.ChannelFuture#addListener(GenericFutureListener)`, so `precommit`
  fails if you use it. The only permitted caller is `Netty4Utils.java:297-300`, marked
  `@SuppressForbidden`.
- The choke point earns its keep at `Netty4Utils.java:281-295`: it bridges Netty futures
  onto ES's `ActionListener`, and calls `ExceptionsHelper.maybeDieOnAnotherThread(cause)`
  so a fatal error arriving through a future kills the node instead of being reported as
  a failed write.
- `Netty4Utils.safeWriteAndFlush` (lines 172-191) handles the worst case. Per
  [netty#8007](https://github.com/netty/netty/issues/8007), a write issued while the
  event loop is shutting down may "just vanish without a trace" — the promise is never
  completed *at all*, so the listener never fires. ES attaches a second listener to the
  event loop's own `terminationFuture()` as a backstop, and builds the promise on
  `ImmediateEventExecutor.INSTANCE` so it can still be completed after that loop is dead.

**One sentence:** async operations hand you a receipt instead of a result, failures are
delivered only to that receipt, and a receipt you discard is a failure that never
happened as far as your program is concerned.

---

## 5. Write-side flow control

**The idea.** Writes do not go to the wire; they go to a per-channel in-memory queue
(`ChannelOutboundBuffer`). If the client is slow, the queue grows. Netty tracks its size
against two marks: above `WRITE_BUFFER_HIGH_WATER_MARK` (default 64 KB)
`channel.isWritable()` goes false and `channelWritabilityChanged` fires; below
`WRITE_BUFFER_LOW_WATER_MARK` (default 32 KB) it flips back. Two marks for hysteresis,
the same shape as `IndexingPressure`'s low/high split.

**`isWritable()` is advisory. Netty will not stop you.**

**The experiment.** Forty 8 KB writes with no flush, marks left at the defaults:

```
  queued 8KB   writable=true   bytesBeforeUnwritable=57249
  queued 16KB  writable=true   bytesBeforeUnwritable=48961
  ...
  >>> WRITABILITY -> false
  queued 64KB  writable=false  bytesBeforeUnwritable=0
  ...
  queued 320KB writable=false  bytesBeforeUnwritable=0
  flushing...
  >>> WRITABILITY -> true
```

Three findings:

- **The queue charges more than you write.** The deltas are 8288, not 8192 — 96 bytes of
  per-message bookkeeping (`ChannelOutboundBuffer.java:63-64`, applied at line 854). So a
  thousand 100-byte messages consume 196 KB of budget, not 100 KB.
- **`bytesBeforeUnwritable` is off by one on purpose:**
  `highWaterMark - totalPendingSize + 1`, because "writability doesn't change until the
  threshold is crossed (not equal to)" — `ChannelOutboundBuffer.java:770-772`.
- **Writes 9 through 40 were all accepted.** 320 KB queued on a channel that declared
  itself unwritable at 64 KB. Five times over, no exception, no rejection.

`>>> WRITABILITY -> false` also printed *before* the `queued 64KB` line — the event fired
synchronously inside `ctx.write()`, the same re-entrancy as lesson 4.

### What ES does about it

`Netty4WriteThrottlingHandler` — the `chunked_writer` slot in the pipeline — is the code
that actually obeys `isWritable()`. It keeps its own queue and only passes writes through
when the channel is writable and nothing is already backed up (lines 90-99), slicing
anything over `MAX_BYTES_PER_WRITE = 1 << 18` (256 KB). It drains on
`channelWritabilityChanged` (lines 132-137) via a `while (channel.isWritable())` loop
(lines 182-188).

Why a second queue rather than Netty's? From the class javadoc: to stop handlers like
`SslHandler` buffering large amounts themselves. Outbound flows tail→head and `ssl` sits
nearer the head than `chunked_writer`, so throttling here means TLS never receives more
than it can encrypt and push out — otherwise a large response gets encrypted in full into
a second set of buffers, doubling memory.

This one file uses all five lessons: `assert ctx.executor().inEventLoop()` (177) and
`Transports.assertTransportThread()` (89) with `ThreadWatchdog.ActivityTracker` on every
entry point; a `ChannelDuplexHandler` in one pipeline slot; `retainedSlice` (115, 196) and
`buf.release()` in `failAsClosedChannel`; `PromiseCombiner` (67) plus
`forwardResultListener`/`forwardFailureListener` attached through `Netty4Utils.addListener`;
and `isWritable()` gating throughout. Good file to reread as a refresher.

**One sentence:** written bytes queue in memory, `isWritable()` and two water marks tell
you the queue is too big, and it is entirely up to the application to care.

---

## The ES HTTP pipeline

From `Netty4HttpServerTransport.initChannel`, lines 329-490. Which handlers are present
depends on config — TLS, compression, security and assertions each add or remove stages.

```
                         ┌────────────────────────────────┐
                         │           TCP SOCKET           │
                         │  setAutoRead(false)      L331  │
                         │  ch.read()  <- primes it L490  │
                         └────────────────────────────────┘
                              |                        ^
                    raw bytes |                        | raw bytes
                              v                        |
 HEAD =====================================================================
   accept_channel_handler            [in]      optional - IP filtering
   initial-tls-handshake-throttle    [in]      optional - TLS
   ssl                               [duplex]  optional - TLS
   initial-tls-handshake-watcher     [in]      optional - TLS
   chunked_writer                    [duplex]  Netty4WriteThrottlingHandler
   byte_buf_sizer                    [in]      NettyByteBufSizer
   read_timeout                      [duplex]  optional
   decoder                           [in]      HttpRequestDecoder
 ..........................................................................
   L403   bytes above  ·  HTTP parts below
          "every handler must call ctx or channel #read()
           when ready to process next HTTP part"
 ..........................................................................
   (missing read detector)           [duplex]  assertions only
   header_validator                  [in]      optional - security
   decoder_compress                  [in]      HttpContentDecompressor
   encoder                           [out]     HttpResponseEncoder
   (content size)                    [in]      Netty4HttpContentSizeHandler
   encoder_compress                  [duplex]  optional - HttpContentCompressor
   (leak detection)                  [in]      optional
   (empty chunk)                     [in]      Netty4EmptyChunkHandler
   (flow control)                    [duplex]  FlowControlHandler
   pipelining                        [duplex]  Netty4HttpPipeliningHandler
 TAIL =====================================================================
                              |                        ^
        HttpRequest / HttpContent                      | HttpResponse
                              v                        |
                    ┌──────────────────────────────────────┐
                    │  AbstractHttpServerTransport         │
                    │     -> RestController                │
                    │        -> Rest*Action                │
                    └──────────────────────────────────────┘
```

`encoder` sitting between two inbound handlers looks wrong until you remember direction:
as an outbound-only handler it is invisible on the request path.

### Why the read() contract begins exactly at the decoder

`Netty4HttpServerTransport` sets `autoRead(false)` (L331), so bytes only arrive when
someone calls `read()`. L490 fires the first one; after that every read must be requested
explicitly. The comment at L403 marks where responsibility passes from Netty to ES.

Above the decoder, Netty self-heals. `ByteToMessageDecoder.channelReadComplete`
(netty-codec, lines 366-375) does:

```java
if (selfFiredChannelRead && !firedChannelRead && !ctx.channel().config().isAutoRead()) {
    ctx.read();
}
```

*If I received data, passed nothing downstream, and auto-read is off — ask for more
myself.* Feed it half a header line and it silently re-reads. But the moment it emits a
complete HTTP part, `firedChannelRead` is true, the condition fails, and it deliberately
does **not** re-read. That handoff is the line the comment sits on.

Conceptually it is the right boundary because the two halves are paced by different
things: above the decoder the pace is the network, below it the pace is Elasticsearch.
You want the brake where the slow consumer is. If Netty kept auto-reading past the
decoder, HTTP parts would pile up ahead of a slow indexer without limit.

Breaking the contract produces **nothing** — no exception, no timeout, just a hang. Hence
`MissingReadDetector`, added immediately after the decoder and only under assertions
(L404-408, comment: "missing reads are hard to catch"). For a sense of how subtle it is,
L473 adds a *second* `FlowControlHandler` to work around
[netty#15053](https://github.com/netty/netty/issues/15053), where the decompressor could
emit several chunks per read while the downstream stream API needs exactly one.

---

## The read-demand chain (bulk backpressure)

Stripped to five steps — the client can only send as fast as the server asks:

```
  1.  ES says "send me some bytes"                    ch.read()
        |
        v
  2.  bytes arrive from the client
        |
        v
  3.  do we have a complete HTTP chunk yet?
        no  --> the decoder quietly asks for more --> back to 2
        yes --> hand it up to the bulk code
        |
        v
  4.  bulk code adds those documents to the current batch
        |
        v
  5.  is the node's indexing memory over the watermark?
        no  --> ask for more bytes right now       --> back to 2
        yes --> send this batch off to be indexed
                wait for it to come back
                then ask for more bytes            --> back to 2
```

Step 3 is pure Netty and automatic. Step 5 is pure Elasticsearch. With class names:

- `Netty4HttpServerTransport.java:490` — `ch.read()` primes the pump.
- `Netty4HttpRequestBodyStream.java:63-72` — `next()` calls `read()`, which is
  `ctx.channel().eventLoop().execute(ctx::read)`.
- `RestBulkAction.java:270-273` — passes `() -> request.contentStream().next()` as the
  `nextItems` callback.
- `IncrementalBulkService.java:336-346` — the decision. No split: `nextItems.run()`
  immediately (line 343). Split: `nextItems.run()` inside `ActionListener.runAfter`
  (line 340), so it only fires once the sub-bulk has been routed, replicated and acked.

**There is no pause API.** While a split sub-bulk is in flight nobody calls `ctx.read()`,
so Netty pulls nothing, the kernel receive buffer fills, the TCP window closes and the
client's `write()` blocks. Flow control is expressed as *withholding demand*.

### The watermarks are split thresholds, not reject thresholds

The guide's wording here is misleading. `IndexingPressure.java:61-83` defines
`SPLIT_BULK_LOW_WATERMARK` (5% of heap, chunk size 4 MB) and `SPLIT_BULK_HIGH_WATERMARK`
(7.5%, chunk size 1 MB). `maybeSplit()` (lines 215-226) requires **both** halves — node
memory pressure *and* enough of this bulk accumulated to be worth flushing.

Rejection with 429 is a different, higher setting: `MAX_COORDINATING_BYTES` /
`MAX_PRIMARY_BYTES` at 10% of heap, `MAX_REPLICA_BYTES` at 1.5× that
(`IndexingPressure.java:43-58`).

So the ladder is: below 5%, no splitting at all; past 5%, split every 4 MB; past 7.5%,
split every 1 MB; past 10%, reject. The watermarks are graceful degradation; the limit is
the wall.

What a smaller chunk size buys is a **lower memory ceiling**, not harder throttling — the
threshold is the most one connection accumulates before it stops reading, so 4 MB across
a hundred connections is 400 MB resident whereas 1 MB is 100 MB.

Why the percentages look small: heap is auto-sized and capped at 31 GB
(`MachineDependentHeap.java:37`), a 64 GB-RAM data node gets ~11 GB heap, and the
accounting is held for the whole replication round trip — coordinating bytes are not
released until primary and replica stages complete. The public docs
(`docs/reference/elasticsearch/configuration-reference/indexing-pressure-settings.md`)
call the 10% default "generously sized", and explain the 1.5× replica limit: nodes
"naturally stop accepting coordinating and primary work in favor of outstanding replica
work."

---

## Counterintuitive findings worth rereading

1. **The boss thread handles almost nothing.** It accepts a connection, registers the new
   channel with a worker, and is never involved again.
2. **"Asynchronous" does not mean "later."** A listener attached to an already-settled
   future runs synchronously, inside your call.
3. **`isWritable()` is advisory.** An unwritable channel accepts every write you give it.
4. **A released buffer usually does not contain your old data** — it contains whatever
   the pool lent it out for next, with a healthy `refCnt` and no exception.
5. **`refCnt` does not change as a message moves down a pipeline.** Ownership transfers;
   the count stays at 1.
6. **Netty charges 96 bytes per queued message** on top of the payload.
7. **The two HTTP settings are named on different axes:** `http.port` versus
   `transport.port` (`HttpTransportSettings.java:77` / `TransportSettings.java:57`).

---

## ES file index

Everything referenced above, for jumping straight back in.

| File | Why |
| --- | --- |
| `docs/internal/DistributedArchitectureGuide.md` | Networking section, lines 13-240 |
| `modules/transport-netty4/.../http/netty4/Netty4HttpServerTransport.java` | the pipeline, `initChannel` 329-490 |
| `modules/transport-netty4/.../http/netty4/Netty4HttpRequestBodyStream.java` | `next()` -> `ctx.read()` |
| `modules/transport-netty4/.../http/netty4/Netty4LeakDetectionHandler.java` | enriches leak hints with uri / x-opaque-id |
| `modules/transport-netty4/.../transport/netty4/Netty4WriteThrottlingHandler.java` | write-side throttling; uses all five lessons |
| `modules/transport-netty4/.../transport/netty4/TrashingByteBuf.java` | zero-fills on final release |
| `modules/transport-netty4/.../transport/netty4/NettyAllocator.java` | allocator wiring, `trashBuffer` |
| `modules/transport-netty4/.../transport/netty4/Netty4Utils.java` | `addListener` choke point, `safeWriteAndFlush` |
| `modules/transport-netty4/forbidden/netty-signatures.txt` | the build rule banning raw `addListener` |
| `server/.../rest/action/document/RestBulkAction.java` | `ChunkHandler`, the `nextItems` lambda |
| `server/.../action/bulk/IncrementalBulkService.java` | the split-or-continue decision |
| `server/.../index/IndexingPressure.java` | watermarks and limits |
| `server/.../rest/RestController.java` | `dispatchRequest`, the HTTP/transport seam |
| `test/framework/.../ESTestCase.java` | paranoid leak detection, line 464 |

---

## Not yet covered

- **Transport (port 9300)** — the node-to-node binary protocol, `Connection` as a pool of
  `Channel`s (~13, split into sub-pools by `ConnectionProfile`), no retries at the
  transport layer, the 2 GB / 30%-of-heap message cap. This is the layer allocation,
  replication, recovery, snapshots and CCR are all built on. Guide lines 105-153.
- **Thread pools and when to fork** — the ES side of "never block the event loop".
- **Wire serialization** — `Writeable`, `StreamInput`/`StreamOutput`, `TransportVersion`
  gating. Guide lines 240+. Its own topic.
- **The two empty guide stubs** at lines 236-238, `### Chunk Encoding` and
  `#### XContent`. Nobody has written them; a candidate first contribution.

---

## Appendix: lab source

`Tap.java` — announces itself in both directions and describes the buffer.

```java
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;

public class Tap extends ChannelDuplexHandler {

    private final String name;

    public Tap(String name) {
        this.name = name;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        System.out.println("IN   " + name + "  " + describe(msg));
        ctx.fireChannelRead(msg);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
        System.out.println("OUT  " + name + "  " + describe(msg));
        ctx.write(msg, promise);
    }

    private static String describe(Object msg) {
        if (msg instanceof ByteBuf buf) {
            return "refCnt=" + buf.refCnt()
                 + "  ridx=" + buf.readerIndex()
                 + "  widx=" + buf.writerIndex()
                 + "  cap=" + buf.capacity();
        }
        return msg.getClass().getSimpleName();
    }
}
```

`EchoServer.java` — base version. Deliberately two worker threads so three connections
cannot each get their own.

```java
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.ResourceLeakDetector;

public class EchoServer {

    static final long T0 = System.nanoTime();

    public static void main(String[] args) throws Exception {
        // must precede any other use of ResourceLeakDetector: TARGET_RECORDS is read
        // in a static initialiser, so setting the property afterwards silently does nothing
        System.setProperty("io.netty.leakDetection.targetRecords", "10");
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);

        EventLoopGroup boss = new NioEventLoopGroup(1);
        EventLoopGroup workers = new NioEventLoopGroup(2);
        try {
            new ServerBootstrap().group(boss, workers)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(new Tap("A"));
                        ch.pipeline().addLast(new Tap("B"));
                        ch.pipeline().addLast(new Tap("C"));
                        ch.pipeline().addLast(new ChannelInboundHandlerAdapter() {

                            @Override
                            public void channelActive(ChannelHandlerContext ctx) {
                                log("ACTIVE", ctx);
                            }

                            @Override
                            public void channelRead(ChannelHandlerContext ctx, Object msg) {
                                log("READ  ", ctx);
                                ctx.writeAndFlush(msg);
                            }

                            @Override
                            public void channelInactive(ChannelHandlerContext ctx) {
                                log("CLOSED", ctx);
                            }

                            private void log(String event, ChannelHandlerContext ctx) {
                                long ms = (System.nanoTime() - T0) / 1_000_000;
                                System.out.println(
                                    ms + "ms  " + event + "  conn=" + ctx.channel().id().asShortText()
                                       + "  thread=" + Thread.currentThread().getName()
                                );
                            }
                        });
                    }
                })
                .bind(9999).sync().channel().closeFuture().sync();
        } finally {
            boss.shutdownGracefully();
            workers.shutdownGracefully();
        }
    }
}
```

### Per-lesson modifications

**Lesson 1, blocking.** Add `Thread.sleep(10_000)` at the top of `channelRead`. Connect
three clients, note the thread assignment from the `ACTIVE` lines, then type into two
that share a thread followed by the loner.

**Lesson 3, leak.** Delete `ctx.writeAndFlush(msg)` so the message is neither forwarded
nor released. Leaks are only reported once the buffer is garbage collected, so on a toy
server add a lab-only GC nudge to `main`:

```java
Executors.newSingleThreadScheduledExecutor()
    .scheduleAtFixedRate(System::gc, 2, 2, TimeUnit.SECONDS);
```

**Lesson 3, use-after-free.** Restore the echo, add a `private ByteBuf stale;` field, and
print `stale` at the top of `channelRead` (in a try/catch) before doing
`stale = buf; ctx.writeAndFlush(buf);`.

**Lesson 4, silent failure.** `ctx.close();` then `ctx.writeAndFlush(msg);`. Run once
bare, once with `.addListener(f -> System.out.println("success=" + f.isSuccess()
+ " cause=" + f.cause()))`. The difference between the two logs is the lesson.

**Lesson 5, water marks.** Comment out the Taps (forty writes would bury the output), add
`ch.config().setWriteBufferWaterMark(new WriteBufferWaterMark(32 * 1024, 64 * 1024))` to
`initChannel`, override `channelWritabilityChanged` to log flips, and in `channelRead`
release the incoming message then loop forty times allocating and writing 8 KB with **no
flush**, logging `isWritable()` and `bytesBeforeUnwritable()`. Flush at the end. Connect
with `nc localhost 9999 > /dev/null`.
