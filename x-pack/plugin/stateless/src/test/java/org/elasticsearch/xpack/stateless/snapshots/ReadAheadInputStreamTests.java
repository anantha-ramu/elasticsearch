/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots;

import org.apache.logging.log4j.Level;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.threadpool.ScalingExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.CRC32;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public class ReadAheadInputStreamTests extends ESTestCase {

    private static final String POOL = "test_read_ahead";

    private ThreadPool threadPool;

    @Before
    public void createThreadPool() {
        threadPool = new TestThreadPool(
            getTestName(),
            new ScalingExecutorBuilder(POOL, 0, between(1, 4), TimeValue.timeValueMinutes(1), true, "test.read_ahead")
        );
    }

    @After
    public void terminateThreadPool() {
        // a thread that was left running would make the termination, or the thread leak check of the test, fail
        terminate(threadPool);
    }

    /**
     * Reads {@code data} in chunks of at most {@code maxRead} bytes, and records on which threads, and tells a hook before each read.
     */
    private interface BeforeRead {
        void accept(int position) throws IOException;
    }

    private static class TestSource extends InputStream {
        final byte[] data;
        final int maxRead;
        final Set<String> readerThreads = ConcurrentHashMap.newKeySet();
        final AtomicInteger readCalls = new AtomicInteger();
        volatile int position;
        volatile boolean closed;
        BeforeRead beforeRead = pos -> {};

        TestSource(byte[] data, int maxRead) {
            this.data = data;
            this.maxRead = maxRead;
        }

        @Override
        public int read() throws IOException {
            final byte[] one = new byte[1];
            return read(one, 0, 1) == -1 ? -1 : one[0] & 0xFF;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            assertFalse("the source was read after it was closed", closed);
            readerThreads.add(Thread.currentThread().getName());
            readCalls.incrementAndGet();
            beforeRead.accept(position);
            if (position >= data.length) {
                return -1;
            }
            final int n = Math.min(Math.min(len, maxRead), data.length - position);
            System.arraycopy(data, position, b, off, n);
            position += n;
            return n;
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    private ReadAheadInputStream newStream(TestSource source, long length, int blockSize) {
        return new ReadAheadInputStream("test", source, length, blockSize, threadPool.executor(POOL));
    }

    private static long crc(byte[] bytes, int length) {
        final var crc = new CRC32();
        crc.update(bytes, 0, length);
        return crc.getValue();
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
                    // an array read with offsets, and a zero-length read, which must not be taken as the end
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

    public void testSameBytesAsTheSource() throws IOException {
        final int blockSize = between(1, 4096);
        final int length = switch (between(0, 7)) {
            case 0 -> 0;
            case 1 -> 1;
            case 2 -> blockSize - 1;
            case 3 -> blockSize;
            case 4 -> blockSize + 1;
            case 5 -> 2 * blockSize;
            case 6 -> 2 * blockSize + 1;
            case 7 -> between(0, 20 * blockSize);
            default -> throw new AssertionError();
        };
        final byte[] data = randomByteArrayOfLength(length);
        final var source = new TestSource(data, between(1, 2 * blockSize));
        final var stream = newStream(source, length, blockSize);

        final byte[] read = readSomeHow(stream, length);

        assertArrayEquals(data, read);
        assertThat(crc(read, read.length), equalTo(crc(data, length)));
        // the source was only ever read by the pool
        assertThat(source.readerThreads.size() > 0 || length == 0, equalTo(true));
        for (String thread : source.readerThreads) {
            assertThat(thread, org.hamcrest.Matchers.containsString(POOL));
        }
        // it never asked for more than the length, so it did not wait for an end it did not need
        assertThat(source.position, lessThanOrEqualTo(length));
        assertFalse(source.closed);
        stream.close();
        assertTrue(source.closed);
        assertThat(stream.getOutstandingBufferBytes(), equalTo(0L));
        assertFalse(stream.hasFetchInFlight());
    }

    public void testSameBytesAtTheDefaultBlockSize() throws IOException {
        final int blockSize = SourceReadAhead.DEFAULT_BLOCK_SIZE_BYTES;
        final int length = blockSize + between(0, 1) * between(1, 100_000);
        final byte[] data = randomByteArrayOfLength(length);
        final var source = new TestSource(data, ReadAheadInputStream.MAX_SOURCE_READ_BYTES);
        try (var stream = newStream(source, length, blockSize)) {
            final byte[] read = stream.readAllBytes();
            assertArrayEquals(data, read);
            assertThat(crc(read, read.length), equalTo(crc(data, length)));
        }
        assertThat(source.closed, equalTo(true));
    }

    public void testSourceThatEndsEarlyEndsTheStream() throws IOException {
        final int blockSize = between(1, 1024);
        final int expectedLength = between(blockSize + 1, 5 * blockSize);
        final int actualLength = between(0, expectedLength - 1);
        final byte[] data = randomByteArrayOfLength(actualLength);
        try (var stream = newStream(new TestSource(data, between(1, 2 * blockSize)), expectedLength, blockSize)) {
            assertArrayEquals(data, stream.readAllBytes());
            assertThat(stream.read(), equalTo(-1));
        }
    }

    public void testCloseBeforeTheFirstRead() throws IOException {
        final var source = new TestSource(randomByteArrayOfLength(100), 10);
        final var stream = newStream(source, 100, 10);
        stream.close();
        stream.close(); // idempotent
        assertTrue(source.closed);
        assertThat(source.readCalls.get(), equalTo(0));
        assertThat(stream.getOutstandingBufferBytes(), equalTo(0L));
        expectThrows(IOException.class, stream::read);
    }

    public void testAbortInTheMiddleOfAFileReleasesTheBuffersAndStopsThePrefetch() throws Exception {
        // sources are read in chunks, so that a close is seen between two reads of one block
        final int chunk = between(1, 100);
        final int blockSize = chunk * between(3, 5);
        final int length = blockSize * between(3, 6);
        final byte[] data = randomByteArrayOfLength(length);
        final var source = new TestSource(data, chunk);
        final var inSecondBlock = new CountDownLatch(1);
        final var release = new CountDownLatch(1);
        final int blockedAt = blockSize + chunk;
        source.beforeRead = pos -> {
            if (pos == blockedAt) {
                inSecondBlock.countDown();
                safeAwait(release);
            }
        };
        final var stream = newStream(source, length, blockSize);

        // the first block is delivered, and the fetch of the second one is under way, blocked in the source
        final var first = new byte[blockSize / 2];
        assertThat(stream.readNBytes(first, 0, first.length), equalTo(first.length));
        assertArrayEquals(Arrays.copyOf(data, first.length), first);
        safeAwait(inSecondBlock);
        assertTrue(stream.hasFetchInFlight());
        assertThat(stream.getOutstandingBufferBytes(), equalTo(2L * blockSize));

        // the abort: the snapshot's next read fails and the file reader closes the stream, which has to wait for the read in the source
        final var closer = new Thread(() -> {
            try {
                stream.close();
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }, "closer");
        closer.start();
        assertBusy(() -> assertTrue(stream.isClosed()));
        assertFalse("the source must not be closed while it is being read", source.closed);
        release.countDown();
        closer.join();

        assertTrue(source.closed);
        assertFalse(stream.hasFetchInFlight());
        assertThat(stream.getOutstandingBufferBytes(), equalTo(0L));
        // the read that was under way finished, and the prefetch stopped there rather than going on to the end of the block
        assertThat(source.position, equalTo(blockedAt + chunk));
        assertBusy(() -> assertThat(((ThreadPoolExecutor) threadPool.executor(POOL)).getActiveCount(), equalTo(0)));
        expectThrows(IOException.class, stream::read);
    }

    public void testCloseDoesNotRunAFetchThatHasNotStarted() throws Exception {
        final var queued = new CopyOnWriteArrayList<Runnable>();
        final byte[] data = randomByteArrayOfLength(100);
        final var source = new TestSource(data, 10);
        final var stream = new ReadAheadInputStream("test", source, 100, 10, queued::add);
        assertThat(stream.read(new byte[0], 0, 0), equalTo(0));
        // the executor holds the fetch: the reader runs it itself in a thread of its own, to be able to wait for it
        final var reader = new Thread(() -> {
            try {
                stream.read();
            } catch (IOException e) {
                // closed while it waited is not expected, as the fetch is run below
                throw new AssertionError(e);
            }
        });
        reader.start();
        assertBusy(() -> assertThat(queued.size(), equalTo(1)));
        assertTrue(stream.hasFetchInFlight());
        queued.get(0).run();
        reader.join();
        // the fetch of the second block is queued now
        assertThat(queued.size(), equalTo(2));
        stream.close();
        assertThat(stream.getOutstandingBufferBytes(), equalTo(0L));
        final int readsBefore = source.readCalls.get();
        queued.get(1).run(); // it finds the stream closed and does nothing
        assertThat(source.readCalls.get(), equalTo(readsBefore));
        assertTrue(source.closed);
    }

    public void testFailureOfThePrefetchSurfacesToTheReader() throws IOException {
        final int blockSize = between(2, 512);
        final int length = between(2 * blockSize, 10 * blockSize);
        final byte[] data = randomByteArrayOfLength(length);
        final var source = new TestSource(data, between(1, blockSize));
        // every call of the source returns at most maxRead bytes and a block needs at least this many, so this one is made
        final int failingCall = between(0, (length + source.maxRead - 1) / source.maxRead - 1);
        final boolean ioException = randomBoolean();
        final var failure = new Exception[1];
        source.beforeRead = pos -> {
            if (source.readCalls.get() > failingCall) {
                if (ioException) {
                    final var e = new IOException("source failed at " + pos);
                    failure[0] = e;
                    throw e;
                }
                final var e = new IllegalStateException("source failed at " + pos);
                failure[0] = e;
                throw e;
            }
        };
        final var stream = newStream(source, length, blockSize);

        final var delivered = new byte[length];
        int pos = 0;
        final Exception thrown;
        try {
            while (true) {
                assertThat("the failure was not delivered", pos, lessThan(length));
                final int n = stream.read(delivered, pos, length - pos);
                assertThat(n, not(equalTo(-1)));
                pos += n;
            }
        } catch (IOException | IllegalStateException e) {
            thrown = e;
        }
        // the error of the thread of the prefetch is the one the reader gets
        assertThat(thrown, sameInstance(failure[0]));
        // what was read before the failure was delivered first, in order, and the error comes with the read that needs the rest
        assertThat(pos, equalTo(source.position));
        assertArrayEquals(Arrays.copyOf(data, pos), Arrays.copyOf(delivered, pos));
        // and it stays failed
        expectThrows(thrown.getClass(), () -> stream.read(delivered, 0, 1));
        stream.close();
        assertThat(stream.getOutstandingBufferBytes(), equalTo(0L));
        assertTrue(source.closed);
    }

    public void testNoThreadIsLeftRunningAfterClose() throws Exception {
        final int blockSize = between(1, 256);
        final int length = between(0, 10 * blockSize);
        final byte[] data = randomByteArrayOfLength(length);
        final var streams = new java.util.ArrayList<ReadAheadInputStream>();
        for (int i = 0; i < 8; i++) {
            final var source = new TestSource(data, between(1, blockSize));
            final var stream = newStream(source, length, blockSize);
            streams.add(stream);
            // abandoned after a few bytes, read to the end, or not read at all
            switch (between(0, 2)) {
                case 0 -> stream.readNBytes(between(0, length));
                case 1 -> assertArrayEquals(data, stream.readAllBytes());
                case 2 -> {
                }
                default -> throw new AssertionError();
            }
        }
        if (length > 0) {
            // at least one of them read a byte
            final var partly = newStream(new TestSource(data, 1), length, blockSize);
            assertThat(partly.read(), equalTo(data[0] & 0xFF));
            streams.add(partly);
        }
        for (var stream : streams) {
            stream.close();
            assertThat(stream.getOutstandingBufferBytes(), equalTo(0L));
            assertFalse(stream.hasFetchInFlight());
        }
        assertBusy(() -> assertThat(((ThreadPoolExecutor) threadPool.executor(POOL)).getActiveCount(), equalTo(0)));
        assertBusy(() -> assertThat(((ThreadPoolExecutor) threadPool.executor(POOL)).getQueue().size(), equalTo(0)));
    }

    public void testReaderDoesTheFetchWhenTheExecutorRejects() throws IOException {
        final int blockSize = between(1, 512);
        final int length = between(0, 10 * blockSize);
        final byte[] data = randomByteArrayOfLength(length);
        final Executor rejecting = r -> { throw new RejectedExecutionException("test"); };
        final var source = new TestSource(data, between(1, blockSize));
        try (var stream = new ReadAheadInputStream("test", source, length, blockSize, rejecting)) {
            assertArrayEquals(data, readSomeHow(stream, length));
            assertThat(source.readerThreads.size() > 0 || length == 0, equalTo(true));
            for (String thread : source.readerThreads) {
                assertThat(thread, equalTo(Thread.currentThread().getName()));
            }
        }
    }

    public void testInterruptedWhileWaitingForTheFetch() throws Exception {
        final var release = new CountDownLatch(1);
        final var source = new TestSource(randomByteArrayOfLength(40), 10);
        source.beforeRead = pos -> safeAwait(release);
        final var stream = newStream(source, 40, 20);
        Thread.currentThread().interrupt();
        try {
            expectThrows(InterruptedIOException.class, stream::read);
            assertTrue("the interrupt is kept", Thread.interrupted());
        } finally {
            release.countDown();
        }
        stream.close();
        assertThat(stream.getOutstandingBufferBytes(), equalTo(0L));
        assertTrue(source.closed);
    }

    public void testSettingOffReturnsTheSourceItself() throws IOException {
        final var tasks = new AtomicInteger();
        final Executor counting = r -> {
            tasks.incrementAndGet();
            r.run();
        };
        final int blockSize = between(1, 100);
        final var readAhead = new SourceReadAhead(counting, false, blockSize);
        final var source = new TestSource(randomByteArrayOfLength(10 * blockSize), blockSize);

        assertThat(readAhead.maybeWrap(source, 10L * blockSize, "off"), sameInstance(source));
        assertThat(tasks.get(), equalTo(0));

        // on, but a stream that is not longer than a block has nothing to read ahead of
        readAhead.setEnabled(true);
        assertThat(readAhead.maybeWrap(source, blockSize, "short"), sameInstance(source));
        assertThat(tasks.get(), equalTo(0));

        final var wrapped = readAhead.maybeWrap(source, 10L * blockSize, "on");
        assertThat(wrapped, not(sameInstance(source)));
        wrapped.readAllBytes();
        assertThat(tasks.get(), equalTo(10));
        wrapped.close();

        readAhead.setEnabled(false);
        assertThat(readAhead.maybeWrap(source, 10L * blockSize, "off again"), sameInstance(source));
    }

    /**
     * The lines that show the overlap in the wave-4 readout: one per block, from the reader, with the read time on the pool and the wait.
     */
    public void testBlocksAreLoggedAtDebug() throws IOException {
        final String loggerName = ReadAheadInputStream.class.getCanonicalName();
        final var logger = org.apache.logging.log4j.LogManager.getLogger(ReadAheadInputStream.class);
        final var originalLevel = logger.getLevel();
        Loggers.setLevel(logger, Level.DEBUG);
        try (var mockLog = MockLog.capture(ReadAheadInputStream.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "first block",
                    loggerName,
                    Level.DEBUG,
                    "test read-ahead block [0] at offset [0]: [100] bytes read from source in * ms on [*"
                        + POOL
                        + "*] from [*] (epoch ms), "
                        + "reader waited * ms"
                )
            );
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "last block",
                    loggerName,
                    Level.DEBUG,
                    "test read-ahead block [2] at offset [200]: [50] bytes read from source in * ms on [*"
                        + POOL
                        + "*] from [*] (epoch ms), "
                        + "reader waited * ms"
                )
            );
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "close",
                    loggerName,
                    Level.DEBUG,
                    "test closed after [3] blocks, [*] ms of source reads on the executor, consumer waited [*] ms for them"
                )
            );
            try (var stream = newStream(new TestSource(randomByteArrayOfLength(250), 25), 250, 100)) {
                assertThat(stream.readAllBytes().length, equalTo(250));
            }
            mockLog.assertAllExpectationsMatched();
        } finally {
            Loggers.setLevel(logger, originalLevel);
        }
    }
}
