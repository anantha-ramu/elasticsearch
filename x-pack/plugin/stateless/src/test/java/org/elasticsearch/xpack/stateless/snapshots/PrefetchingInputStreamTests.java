/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.CRC32;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class PrefetchingInputStreamTests extends ESTestCase {

    private static final String POOL = "test_prefetch";
    private static final int THREADS = 8;
    private static final int PAGE = 64;
    private static final int PAGE_FOR_BIG = 16 * 1024;

    private ThreadPool threadPool;

    @Before
    public void createThreadPool() {
        threadPool = new TestThreadPool(
            getTestName(),
            new FixedExecutorBuilder(
                Settings.EMPTY,
                POOL,
                THREADS,
                4 * THREADS,
                "test.prefetch",
                EsExecutors.TaskTrackingConfig.DO_NOT_TRACK
            )
        );
    }

    @After
    public void terminateThreadPool() {
        // a task that was left running would make the termination, or the thread leak check of the test, fail
        terminate(threadPool);
    }

    private Executor executor() {
        return threadPool.executor(POOL);
    }

    private void assertIdle(SourceReadBufferPool pool) throws Exception {
        assertBusy(() -> {
            assertThat(((ThreadPoolExecutor) threadPool.executor(POOL)).getActiveCount(), equalTo(0));
            assertThat("all the buffers were given back", pool.getBytesInUse(), equalTo(0L));
        });
    }

    private static long crc(byte[] bytes) {
        final var crc = new CRC32();
        crc.update(bytes, 0, bytes.length);
        return crc.getValue();
    }

    /** One connection for the whole stream that the ranges are read from in turn, as a read ahead does. */
    private static PrefetchingInputStream.RangeOpener sequential(InputStream source) {
        return (offset, length) -> new java.io.FilterInputStream(source) {
            @Override
            public void close() {}
        };
    }

    private static PrefetchingInputStream.RangeOpener ranges(RateLimitedFakeStore store) {
        return store::get;
    }

    private PrefetchingInputStream newStream(
        RateLimitedFakeStore store,
        boolean sequential,
        int rangeSize,
        int maxInFlight,
        SourceReadBufferPool pool,
        Executor executor,
        Runnable abortCheck
    ) {
        if (sequential) {
            final var source = store.get(0, store.data.length);
            return new PrefetchingInputStream(
                "test",
                store.data.length,
                rangeSize,
                maxInFlight,
                sequential(source),
                executor,
                pool,
                abortCheck,
                null,
                source
            );
        }
        return new PrefetchingInputStream(
            "test",
            store.data.length,
            rangeSize,
            maxInFlight,
            ranges(store),
            executor,
            pool,
            abortCheck,
            null,
            null
        );
    }

    private static byte[] readSomeHow(InputStream in, int expectedLength) throws IOException {
        final var out = new byte[expectedLength + 1]; // room to see more bytes than expected
        int pos = 0;
        while (true) {
            final int n;
            switch (between(0, 2)) {
                case 0 -> {
                    final int b = in.read();
                    if (b == -1) {
                        n = -1;
                    } else {
                        out[pos] = (byte) b;
                        n = 1;
                    }
                }
                case 1 -> n = in.read(out, pos, between(1, out.length - pos));
                case 2 -> {
                    // a zero-length read must not be taken as the end
                    assertThat(in.read(out, pos, 0), equalTo(0));
                    n = in.read(out, pos, out.length - pos);
                }
                default -> throw new AssertionError();
            }
            if (n == -1) {
                break;
            }
            pos += n;
            assertThat(pos, lessThanOrEqualTo(expectedLength));
        }
        // the end stays the end
        assertThat(in.read(), equalTo(-1));
        assertThat(in.read(out, 0, 1), equalTo(-1));
        return Arrays.copyOf(out, pos);
    }

    public void testSameBytesAsTheSource() throws Exception {
        final int rangeSize = between(1, 6 * PAGE);
        final int length = switch (between(0, 6)) {
            case 0 -> 0;
            case 1 -> 1;
            case 2 -> rangeSize - 1;
            case 3 -> rangeSize;
            case 4 -> rangeSize + 1;
            case 5 -> 2 * rangeSize + 1;
            case 6 -> between(0, 30 * rangeSize);
            default -> throw new AssertionError();
        };
        final var store = new RateLimitedFakeStore(randomByteArrayOfLength(length), 0, 0);
        store.maxRead = between(1, 3 * PAGE);
        final var pool = new SourceReadBufferPool(PAGE, Long.MAX_VALUE / 4);
        final boolean sequential = randomBoolean();
        final int maxInFlight = sequential ? 1 : between(1, 5);

        final var stream = newStream(store, sequential, rangeSize, maxInFlight, pool, executor(), () -> {});
        final byte[] read = readSomeHow(stream, length);

        assertArrayEquals(store.data, read);
        assertThat(crc(read), equalTo(crc(store.data)));
        for (String thread : store.readerThreads) {
            assertThat("no read was made by the reader, with enough memory", thread, containsString(POOL));
        }
        stream.close();
        assertThat(store.openGets.get(), equalTo(0));
        assertIdle(pool);
    }

    public void testRangesAreRequestedInOrderAndAtMostTheMaximumAtOnce() throws Exception {
        final int rangeSize = between(1, 4 * PAGE);
        final int ranges = between(3, 12);
        final var store = new RateLimitedFakeStore(randomByteArrayOfLength(ranges * rangeSize - between(0, rangeSize - 1)), 0, 0);
        final int maxInFlight = between(1, 4);
        // every connection is held open until the reader has taken it, so that the ones in flight are the ones that were requested
        final var pool = new SourceReadBufferPool(PAGE, Long.MAX_VALUE / 4);
        final var stream = newStream(store, false, rangeSize, maxInFlight, pool, executor(), () -> {});
        // the biggest the buffers in use get is what is in flight and the one that is read
        final int pagesPerRange = pool.pagesFor(rangeSize);
        final var in = new byte[store.data.length];
        int read = 0;
        while (read < in.length) {
            final int n = stream.read(in, read, Math.min(in.length - read, between(1, 2 * PAGE)));
            assertThat(n, greaterThan(0));
            read += n;
            assertThat(pool.getBytesInUse(), lessThanOrEqualTo((long) (maxInFlight + 1) * pagesPerRange * PAGE));
        }
        assertArrayEquals(store.data, in);
        assertThat(store.maxOpenGets.get(), lessThanOrEqualTo(maxInFlight));
        // the ranges tile the source, each asked for once
        long expectedOffset = 0;
        final var asked = store.getRanges.stream().sorted((a, b) -> Long.compare(a[0], b[0])).toList();
        for (long[] range : asked) {
            assertThat(range[0], equalTo(expectedOffset));
            assertThat(range[1], equalTo(Math.min(rangeSize, store.data.length - expectedOffset)));
            expectedOffset += range[1];
        }
        assertThat(expectedOffset, equalTo((long) store.data.length));
        stream.close();
        assertIdle(pool);
    }

    /**
     * The ranges are fetched at the same time, each over its own connection that is capped, and delivered in order, so the file is read
     * faster than over one connection.
     */
    public void testParallelRangesOverlapAndReadFasterThanOneConnection() throws Exception {
        assertWithRetries(3, this::testParallelRangesOverlapAndReadFasterThanOneConnectionOnce);
    }

    private void testParallelRangesOverlapAndReadFasterThanOneConnectionOnce() throws Exception {
        final int rangeSize = 128 * 1024;
        final int length = 8 * rangeSize;
        final int connections = between(2, 4);
        final long readRate = 8L * 1024 * 1024; // so that one connection takes 125 ms for the file

        final var singleStore = new RateLimitedFakeStore(randomByteArrayOfLength(length), readRate, 0);
        final long singleStart = System.nanoTime();
        try (var in = singleStore.get(0, length)) {
            assertArrayEquals(singleStore.data, in.readAllBytes());
        }
        final long singleNanos = System.nanoTime() - singleStart;

        final var store = new RateLimitedFakeStore(singleStore.data, readRate, 0);
        final var pool = new SourceReadBufferPool(PAGE_FOR_BIG, Long.MAX_VALUE / 4);
        final long start = System.nanoTime();
        try (var stream = newStream(store, false, rangeSize, connections, pool, executor(), () -> {})) {
            assertArrayEquals(store.data, stream.readAllBytes());
        }
        final long parallelNanos = System.nanoTime() - start;

        assertThat("ranges were open at the same time", store.maxOpenGets.get(), greaterThan(1));
        assertThat(store.maxOpenGets.get(), lessThanOrEqualTo(connections));
        // the ideal is 1/connections of the time of one connection; the bar is far below what one connection can do
        assertThat(
            "parallel [" + parallelNanos + "ns] vs single [" + singleNanos + "ns]",
            (double) parallelNanos,
            lessThan(0.8 * singleNanos)
        );
        assertIdle(pool);
    }

    /**
     * What the upload does with a part: reads a chunk from the source and writes it to its own connection, in turn. Over a connection per
     * direction that is capped, a thread that does both takes the time of both. With the source read ahead the reads happen while the
     * chunks are written, so the part takes the time of the slower of the two.
     */
    public void testReadAheadOverlapsReadsWithWrites() throws Exception {
        assertWithRetries(3, this::testReadAheadOverlapsReadsWithWritesOnce);
    }

    private void testReadAheadOverlapsReadsWithWritesOnce() throws Exception {
        final int rangeSize = 128 * 1024;
        final int length = 8 * rangeSize;
        final long rate = 8L * 1024 * 1024;
        final byte[] data = randomByteArrayOfLength(length);

        // the source as it is read without read-ahead: one connection that the upload loop reads and writes
        final var plain = new RateLimitedFakeStore(data, rate, rate);
        final long plainStart = System.nanoTime();
        try (var in = plain.get(0, length)) {
            copy(in, plain.upload(), 32 * 1024);
        }
        final long plainNanos = System.nanoTime() - plainStart;
        assertThat("the reads of the plain loop never overlap its writes", plain.writesDuringReads.get(), equalTo(0));

        final var store = new RateLimitedFakeStore(data, rate, rate);
        final var pool = new SourceReadBufferPool(PAGE_FOR_BIG, Long.MAX_VALUE / 4);
        final long start = System.nanoTime();
        try (var stream = newStream(store, true, rangeSize, 1, pool, executor(), () -> {})) {
            copy(stream, store.upload(), 32 * 1024);
        }
        final long readAheadNanos = System.nanoTime() - start;

        assertThat(store.uploadedBytes.get(), equalTo(length));
        assertThat(
            "most writes were made while a read was in progress, writes [" + store.writes.get() + "] gets [" + store.gets.get() + "]",
            store.writesDuringReads.get(),
            greaterThan(store.writes.get() / 2)
        );
        assertThat(store.maxOpenGets.get(), equalTo(1));
        assertThat(
            "read-ahead [" + readAheadNanos + "ns] vs plain [" + plainNanos + "ns]",
            (double) readAheadNanos,
            lessThan(0.85 * plainNanos)
        );
        assertIdle(pool);
    }

    private static void copy(InputStream in, OutputStream out, int chunk) throws IOException {
        final byte[] buffer = new byte[chunk];
        int n;
        while ((n = in.read(buffer)) >= 0) {
            out.write(buffer, 0, n);
        }
    }

    public void testNodeCapIsRespectedAndTheBytesAreTheSame() throws Exception {
        final int rangeSize = between(PAGE, 4 * PAGE);
        final int pagesPerRange = (rangeSize + PAGE - 1) / PAGE;
        final var store = new RateLimitedFakeStore(randomByteArrayOfLength(rangeSize * between(10, 30)), 0, 0);
        store.maxRead = between(1, PAGE);
        // sometimes less than one range, so that there is nothing to prefetch with
        final long capPages = between(0, 4 * pagesPerRange);
        final var pool = new SourceReadBufferPool(PAGE, capPages * PAGE);
        final var highWater = new AtomicLong();
        store.beforeRead = position -> highWater.accumulateAndGet(pool.getBytesInUse(), Math::max);
        final var tasks = new AtomicInteger();
        final Executor counting = r -> {
            tasks.incrementAndGet();
            executor().execute(r);
        };

        try (var stream = newStream(store, false, rangeSize, between(1, 6), pool, counting, () -> {})) {
            assertArrayEquals(store.data, stream.readAllBytes());
            highWater.accumulateAndGet(pool.getBytesInUse(), Math::max);
        }

        assertThat(highWater.get(), lessThanOrEqualTo(capPages * PAGE));
        if (capPages < pagesPerRange) {
            assertThat("without memory for a range, the reader reads it itself", tasks.get(), equalTo(0));
            assertThat(store.readerThreads, equalTo(java.util.Set.of(Thread.currentThread().getName())));
        }
        assertIdle(pool);
    }

    public void testStreamsShareTheCapOfThePool() throws Exception {
        final int rangeSize = 4 * PAGE;
        final var data = randomByteArrayOfLength(40 * rangeSize);
        final var pool = new SourceReadBufferPool(PAGE, 5L * 4 * PAGE); // five ranges, for all the streams
        final int streams = between(3, 6);
        final var highWater = new AtomicLong();
        final var failures = new AtomicInteger();
        // the readers stand for the snapshot threads, of which a node has many
        runInParallel(streams, i -> {
            final var store = new RateLimitedFakeStore(data, 0, 0);
            store.beforeRead = position -> highWater.accumulateAndGet(pool.getBytesInUse(), Math::max);
            try (var stream = newStream(store, false, rangeSize, 3, pool, executor(), () -> {})) {
                assertArrayEquals(data, stream.readAllBytes());
            } catch (Throwable t) {
                failures.incrementAndGet();
            }
        });
        assertThat(failures.get(), equalTo(0));
        assertThat(highWater.get(), lessThanOrEqualTo(pool.getCapBytes()));
        assertIdle(pool);
    }

    public void testFailureOfARangeIsDeliveredAfterTheBytesBeforeIt() throws Exception {
        final int rangeSize = between(PAGE, 4 * PAGE);
        final var store = new RateLimitedFakeStore(randomByteArrayOfLength(between(5 * rangeSize, 12 * rangeSize)), 0, 0);
        final int failAt = between(0, store.data.length - 1);
        final boolean runtime = randomBoolean();
        store.failFromPosition = failAt;
        store.failWithRuntimeException = runtime;
        store.maxRead = between(1, PAGE);
        final var pool = new SourceReadBufferPool(PAGE, Long.MAX_VALUE / 4);
        final boolean sequential = randomBoolean();

        try (var stream = newStream(store, sequential, rangeSize, sequential ? 1 : between(1, 4), pool, executor(), () -> {})) {
            final var read = new byte[store.data.length];
            int pos = 0;
            final Exception e;
            try {
                while (true) {
                    final int n = stream.read(read, pos, read.length - pos);
                    assertThat(n, greaterThan(0));
                    pos += n;
                }
            } catch (IOException | IllegalStateException ex) {
                e = ex;
            }
            assertThat(e.getMessage(), containsString("fetch failed at "));
            assertThat(e instanceof IllegalStateException, equalTo(runtime));
            // everything that was read before the failure was delivered first, and nothing after it
            assertThat(pos, equalTo(failAt));
            assertArrayEquals(Arrays.copyOf(store.data, failAt), Arrays.copyOf(read, pos));
            // and it stays failed
            expectThrows(Exception.class, stream::read);
        }
        assertIdle(pool);
    }

    public void testRangeThatEndsEarlyEndsTheStream() throws Exception {
        final int rangeSize = between(PAGE, 4 * PAGE);
        final var store = new RateLimitedFakeStore(randomByteArrayOfLength(between(5 * rangeSize, 12 * rangeSize)), 0, 0);
        // the stream is expected to be longer than the blob, as one whose source ended early
        final int expected = store.data.length + between(1, 3 * rangeSize);
        final var pool = new SourceReadBufferPool(PAGE, Long.MAX_VALUE / 4);
        try (
            var stream = new PrefetchingInputStream(
                "test",
                expected,
                rangeSize,
                between(1, 4),
                ranges(store),
                executor(),
                pool,
                () -> {},
                null,
                null
            )
        ) {
            assertArrayEquals(store.data, stream.readAllBytes());
        }
        assertIdle(pool);
    }

    public void testRejectedFetchesAreReadByTheReader() throws Exception {
        final int rangeSize = between(PAGE, 4 * PAGE);
        final var store = new RateLimitedFakeStore(randomByteArrayOfLength(between(8 * rangeSize, 20 * rangeSize)), 0, 0);
        final var pool = new SourceReadBufferPool(PAGE, Long.MAX_VALUE / 4);
        final int accepted = between(0, 3);
        final var seen = new AtomicInteger();
        final Executor rejecting = r -> {
            if (seen.incrementAndGet() > accepted) {
                throw new RejectedExecutionException("full");
            }
            executor().execute(r);
        };
        try (var stream = newStream(store, false, rangeSize, between(1, 4), pool, rejecting, () -> {})) {
            assertArrayEquals(store.data, stream.readAllBytes());
        }
        assertThat(store.openGets.get(), equalTo(0));
        assertIdle(pool);
    }

    /**
     * An abort or a pause makes the snapshot's read fail, and the stream is closed from there, with ranges in flight that are blocked on
     * the network. Closing does not wait for them, and they give back their buffers, and the connection of a read ahead, when they stop.
     */
    public void testCloseDoesNotWaitForFetchesAndTheyGiveBackWhatTheyHold() throws Exception {
        final int rangeSize = 2 * PAGE;
        final var store = new RateLimitedFakeStore(randomByteArrayOfLength(20 * rangeSize), 0, 0);
        final var release = new CountDownLatch(1);
        final var blocked = new CountDownLatch(1);
        final int blockIndex = between(1, 4);
        final int blockFrom = rangeSize * blockIndex; // ranges before it complete, so the reader can read them
        store.beforeRead = position -> {
            if (position >= blockFrom) {
                blocked.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    throw new IOException(e);
                }
            }
        };
        final var pool = new SourceReadBufferPool(PAGE, Long.MAX_VALUE / 4);
        final boolean sequential = randomBoolean();
        final var stream = newStream(store, sequential, rangeSize, sequential ? 1 : between(1, 4), pool, executor(), () -> {});
        // the reader is in the range before the blocked one, so that the blocked one is in flight, whatever the window is
        final var first = new byte[(blockIndex - 1) * rangeSize + between(1, rangeSize)];
        assertThat(stream.readNBytes(first, 0, first.length), equalTo(first.length));
        safeAwait(blocked);
        assertThat(pool.getBytesInUse(), greaterThan(0L));

        stream.close(); // returns while a read of the source is blocked

        // the buffers of ranges that nothing was reading are back, the others are held by the task that is blocked
        release.countDown();
        assertIdle(pool);
        assertThat("every connection was closed", store.openGets.get(), equalTo(0));
        expectThrows(IOException.class, stream::read);
    }

    public void testAbortStopsTheFetchesAndTheWaitingReader() throws Exception {
        final int rangeSize = 2 * PAGE;
        final var store = new RateLimitedFakeStore(randomByteArrayOfLength(20 * rangeSize), 0, 0);
        final var aborted = new AtomicBoolean();
        final var blockAt = between(0, 10 * rangeSize);
        final var release = new CountDownLatch(1);
        final var blocked = new CountDownLatch(1);
        store.beforeRead = position -> {
            if (position >= blockAt) {
                blocked.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    throw new IOException(e);
                }
            }
        };
        final var pool = new SourceReadBufferPool(PAGE, Long.MAX_VALUE / 4);
        final boolean sequential = randomBoolean();
        final Runnable abortCheck = () -> {
            if (aborted.get()) {
                throw new IllegalStateException("aborted");
            }
        };
        final var stream = newStream(store, sequential, rangeSize, sequential ? 1 : between(1, 4), pool, executor(), abortCheck);

        final var readerFailure = new java.util.concurrent.atomic.AtomicReference<Throwable>();
        final var readerDone = new CountDownLatch(1);
        threadPool.generic().execute(() -> {
            try {
                stream.readAllBytes();
            } catch (Throwable t) {
                readerFailure.set(t);
            } finally {
                readerDone.countDown();
            }
        });
        safeAwait(blocked);
        aborted.set(true);
        // the reader waits for a range that is blocked on the network, and notices the abort without it
        safeAwait(readerDone);
        assertThat(readerFailure.get(), org.hamcrest.Matchers.instanceOf(IllegalStateException.class));
        assertThat(readerFailure.get().getMessage(), equalTo("aborted"));
        final int getsWhenAborted = store.gets.get();
        stream.close();
        release.countDown();
        assertIdle(pool);
        // the ranges that were in flight stopped at their next read, and none started after the abort
        assertThat(store.gets.get(), equalTo(getsWhenAborted));
        assertThat(store.openGets.get(), equalTo(0));
    }

    public void testNothingLeaksWhateverTheReaderDoes() throws Exception {
        final var pool = new SourceReadBufferPool(PAGE, between(0, 40) * PAGE);
        for (int run = 0; run < 20; run++) {
            final int rangeSize = between(1, 3 * PAGE);
            final var store = new RateLimitedFakeStore(randomByteArrayOfLength(between(0, 15 * rangeSize)), 0, 0);
            store.maxRead = between(1, 2 * PAGE);
            final boolean sequential = randomBoolean();
            final var stream = newStream(store, sequential, rangeSize, sequential ? 1 : between(1, 4), pool, executor(), () -> {});
            final int toRead = between(0, store.data.length);
            final var read = new byte[toRead];
            final int n = stream.readNBytes(read, 0, toRead);
            assertArrayEquals(Arrays.copyOf(store.data, n), Arrays.copyOf(read, n));
            if (randomBoolean()) {
                stream.close();
            } else {
                stream.readAllBytes();
                stream.close();
            }
            stream.close(); // twice
        }
        assertIdle(pool);
    }

    public void testPoolLendsPagesWithinTheCapAndReusesThem() {
        final var pool = new SourceReadBufferPool(PAGE, 5L * PAGE);
        final var a = pool.tryAcquire(3L * PAGE - 1);
        assertNotNull(a);
        assertThat(pool.getBytesInUse(), equalTo(3L * PAGE));
        assertNull("the cap is five pages", pool.tryAcquire(3L * PAGE));
        final var b = pool.tryAcquire(2L * PAGE);
        assertNotNull(b);
        assertNull(pool.tryAcquire(1));
        final var firstPage = a.page(0);
        a.release();
        a.release(); // only once
        assertThat(pool.getBytesInUse(), equalTo(2L * PAGE));
        assertThat(pool.getFreePages(), equalTo(3L));
        final var c = pool.tryAcquire(PAGE);
        assertNotNull(c);
        assertThat("pages are reused", Arrays.asList(c.page(0)).contains(firstPage) || c.page(0).length == PAGE, equalTo(true));
        b.release();
        c.release();
        assertThat(pool.getBytesInUse(), equalTo(0L));
        // the pages kept are never more than the cap
        pool.setCapBytes(PAGE);
        assertThat(pool.getFreePages(), lessThanOrEqualTo(1L));
        pool.clear();
        assertThat(pool.getFreePages(), equalTo(0L));
        assertNull(new SourceReadBufferPool(PAGE, 0).tryAcquire(1));
    }

    public void testPagesAreSmallerThanWhatG1TreatsAsHumongous() {
        // regions are at least 1 MiB, and an object is humongous from half a region
        assertThat(SourceReadBufferPool.PAGE_SIZE_BYTES, lessThan(512 * 1024));
        final var pool = new SourceReadBufferPool(1024L * 1024 * 1024);
        final var buffer = pool.tryAcquire(16L * 1024 * 1024);
        assertNotNull(buffer);
        for (int i = 0; i < pool.pagesFor(16L * 1024 * 1024); i++) {
            assertThat(buffer.page(i).length, equalTo(SourceReadBufferPool.PAGE_SIZE_BYTES));
        }
        buffer.release();
    }

    public void testRejectsBadArguments() {
        final var pool = new SourceReadBufferPool(PAGE, 10 * PAGE);
        expectThrows(
            IllegalArgumentException.class,
            () -> new PrefetchingInputStream(
                "t",
                10,
                0,
                1,
                (o, l) -> InputStream.nullInputStream(),
                Runnable::run,
                pool,
                () -> {},
                null,
                null
            )
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> new PrefetchingInputStream(
                "t",
                10,
                1,
                0,
                (o, l) -> InputStream.nullInputStream(),
                Runnable::run,
                pool,
                () -> {},
                null,
                null
            )
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> new PrefetchingInputStream(
                "t",
                -1,
                1,
                1,
                (o, l) -> InputStream.nullInputStream(),
                Runnable::run,
                pool,
                () -> {},
                null,
                null
            )
        );
    }

    /**
     * The tests of speed compare the times of two reads that sleep to stay under a rate, which a machine that is busy with something
     * else stretches, so a failed comparison is tried again, twice, before it is a failure. Where the reads overlap is checked in each try.
     */
    private static void assertWithRetries(int attempts, CheckedRunnable<Exception> attempt) throws Exception {
        for (int i = 1;; i++) {
            try {
                attempt.run();
                return;
            } catch (AssertionError e) {
                if (i == attempts) {
                    throw e;
                }
            }
        }
    }
}
