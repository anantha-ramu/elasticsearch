/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * An object store of one blob, for tests, whose every connection is capped to a rate, in each direction, as the connections to GCS are:
 * a ranged GET over the connection delivers its bytes no faster than {@code readBytesPerSecond} however many others are open, and an upload
 * over a connection takes its bytes no faster than {@code writeBytesPerSecond}. A rate of 0 does not cap.
 * <p>
 * It records what the tests need to show that reads overlap each other, and overlap writes: the most GETs that were open at once, the
 * writes that began while a read was in progress, and the threads that read.
 */
final class RateLimitedFakeStore {

    /** Called before a read of a connection, with the position of the blob that it is at; may block or throw. */
    @FunctionalInterface
    interface BeforeRead {
        void accept(long position) throws IOException;
    }

    final byte[] data;
    private final long readBytesPerSecond;
    private final long writeBytesPerSecond;

    final AtomicInteger gets = new AtomicInteger();
    final AtomicInteger openGets = new AtomicInteger();
    final AtomicInteger maxOpenGets = new AtomicInteger();
    final AtomicInteger closedGets = new AtomicInteger();
    final AtomicInteger readsInProgress = new AtomicInteger();
    final AtomicInteger writes = new AtomicInteger();
    final AtomicInteger writesDuringReads = new AtomicInteger();
    final AtomicInteger uploadedBytes = new AtomicInteger();
    final Set<String> readerThreads = ConcurrentHashMap.newKeySet();
    /** The ranges of the GETs, as {offset, length}, in the order they were made. */
    final ConcurrentLinkedQueue<long[]> getRanges = new ConcurrentLinkedQueue<>();
    volatile BeforeRead beforeRead = position -> {};
    /** The size of the largest read that a connection serves, so that a stream cannot assume one read returns all it asked for. */
    volatile int maxRead = Integer.MAX_VALUE;
    /** From this position of the blob, a read fails: with a {@link IOException}, or an {@link IllegalStateException} if so told. */
    volatile long failFromPosition = Long.MAX_VALUE;
    volatile boolean failWithRuntimeException;

    RateLimitedFakeStore(byte[] data, long readBytesPerSecond, long writeBytesPerSecond) {
        this.data = data;
        this.readBytesPerSecond = readBytesPerSecond;
        this.writeBytesPerSecond = writeBytesPerSecond;
    }

    /**
     * A ranged GET: a connection that delivers {@code length} bytes from {@code offset}, or fewer if the blob ends first.
     */
    InputStream get(long offset, long length) {
        gets.incrementAndGet();
        getRanges.add(new long[] { offset, length });
        final int open = openGets.incrementAndGet();
        maxOpenGets.accumulateAndGet(open, Math::max);
        return new Connection(offset, Math.min(length, data.length - offset));
    }

    /**
     * An upload connection, which takes the bytes written to it at the capped rate.
     */
    OutputStream upload() {
        return new OutputStream() {
            private final Gate gate = new Gate(writeBytesPerSecond);

            @Override
            public void write(int b) throws IOException {
                write(new byte[] { (byte) b }, 0, 1);
            }

            @Override
            public void write(byte[] b, int off, int len) throws IOException {
                writes.incrementAndGet();
                if (readsInProgress.get() > 0) {
                    writesDuringReads.incrementAndGet();
                }
                uploadedBytes.addAndGet(len);
                gate.pass(len);
            }
        };
    }

    /**
     * The cap of one connection in one direction: the bytes that pass take the time they take at the rate, one after the other, and a
     * connection that was idle does not make up for it afterwards, as a real one does not.
     */
    private static final class Gate {
        private final long bytesPerSecond;
        private long nextFreeNanos = System.nanoTime();

        Gate(long bytesPerSecond) {
            this.bytesPerSecond = bytesPerSecond;
        }

        void pass(long bytes) {
            if (bytesPerSecond <= 0) {
                return;
            }
            final long now = System.nanoTime();
            nextFreeNanos = Math.max(now, nextFreeNanos) + bytes * TimeUnit.SECONDS.toNanos(1) / bytesPerSecond;
            final long waitMillis = TimeUnit.NANOSECONDS.toMillis(nextFreeNanos - now);
            if (waitMillis > 0) {
                ESTestCase.safeSleep(waitMillis);
            }
        }
    }

    private final class Connection extends InputStream {
        private final long offset;
        private final long length;
        private final Gate gate = new Gate(readBytesPerSecond);
        private long position;
        private boolean closed;

        Connection(long offset, long length) {
            this.offset = offset;
            this.length = length;
        }

        @Override
        public int read() throws IOException {
            final byte[] one = new byte[1];
            return read(one, 0, 1) == -1 ? -1 : one[0] & 0xFF;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (closed) {
                throw new IOException("connection is closed");
            }
            readerThreads.add(Thread.currentThread().getName());
            readsInProgress.incrementAndGet();
            try {
                beforeRead.accept(offset + position);
                if (offset + position >= failFromPosition) {
                    if (failWithRuntimeException) {
                        throw new IllegalStateException("fetch failed at " + (offset + position));
                    }
                    throw new IOException("fetch failed at " + (offset + position));
                }
                if (position >= length) {
                    return -1;
                }
                // a read does not go past the position where reads start to fail, so that what was delivered before the failure is known
                final long untilFailure = failFromPosition - (offset + position);
                final int n = (int) Math.min(Math.min(Math.min(len, maxRead), length - position), untilFailure);
                System.arraycopy(data, (int) (offset + position), b, off, n);
                position += n;
                gate.pass(n);
                return n;
            } finally {
                readsInProgress.decrementAndGet();
            }
        }

        @Override
        public void close() {
            if (closed == false) {
                closed = true;
                openGets.decrementAndGet();
                closedGets.incrementAndGet();
            }
        }
    }
}
