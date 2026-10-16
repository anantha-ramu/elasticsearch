/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Experimental. A stream over a source of known length that reads the source in blocks, one block ahead of the reader: while the reader
 * consumes block N, a task on the given executor reads block N+1 into the other of two buffers. The reader sees the source's bytes in
 * order and unchanged, so anything that sums them (a checksum) sees the same bytes as without read-ahead.
 * <ul>
 * <li>At most one fetch is in flight. A fetch only fills its buffer, then completes: it never waits for the reader, so a busy executor
 * slows the reader down but cannot block it forever. If the executor rejects the task, the reader does the fetch itself.</li>
 * <li>The source is only ever read by the fetch task, and only closed by {@link #close()} once no fetch is running, so the source never
 * sees two threads.</li>
 * <li>An error of a fetch is delivered after the bytes that were read before it, as an error of the read that needs the missing bytes.</li>
 * <li>{@link #close()} stops a running fetch at its next source read, waits for it (at most one read of the source, which has its own
 * timeouts), closes the source and drops both buffers.</li>
 * </ul>
 * The reader is a single thread. Not thread-safe for concurrent reads, as with any stream; {@link #close()} must not race with a read.
 */
final class ReadAheadInputStream extends InputStream {

    private static final Logger logger = LogManager.getLogger(ReadAheadInputStream.class);

    /** A fetch reads the source with calls of at most this size, so that a close is noticed between them. */
    static final int MAX_SOURCE_READ_BYTES = 1024 * 1024;

    private final String description;
    private final InputStream source;
    private final long length;
    private final int blockSize;
    private final Executor executor;

    private final Object lock = new Object();
    private volatile boolean closed;
    // guarded by lock
    private Fetch pending;

    // the reader's state, not shared with the fetch task, except that the buffers are lent to it while it fills them
    private byte[] current;
    private int currentPos;
    private int currentLen;
    @Nullable // the buffer the next fetch fills, null while one is in flight or before the second buffer is needed
    private byte[] spare;
    private long requested; // bytes of the source handed to fetches so far
    private boolean ended;
    @Nullable
    private Throwable deferredFailure;

    private final AtomicLong outstandingBufferBytes = new AtomicLong();
    private int blocksDelivered;
    private long fetchNanosTotal;
    private long waitNanosTotal;

    /**
     * @param description for the logs
     * @param source      read by the fetch tasks only
     * @param length      the number of bytes the source is expected to deliver
     * @param blockSize   the size of a block, and so of each of the two buffers
     * @param executor    runs the fetch tasks
     */
    ReadAheadInputStream(String description, InputStream source, long length, int blockSize, Executor executor) {
        if (blockSize <= 0) {
            throw new IllegalArgumentException("block size must be positive, but was [" + blockSize + "]");
        }
        if (length < 0) {
            throw new IllegalArgumentException("length must not be negative, but was [" + length + "]");
        }
        this.description = description;
        this.source = Objects.requireNonNull(source);
        this.length = length;
        this.blockSize = blockSize;
        this.executor = Objects.requireNonNull(executor);
    }

    @Override
    public int read() throws IOException {
        ensureOpen();
        if (currentPos == currentLen && nextBlock() == false) {
            return -1;
        }
        return current[currentPos++] & 0xFF;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        Objects.checkFromIndexSize(off, len, b.length);
        ensureOpen();
        if (len == 0) {
            return 0;
        }
        if (currentPos == currentLen && nextBlock() == false) {
            return -1;
        }
        final int n = Math.min(len, currentLen - currentPos);
        System.arraycopy(current, currentPos, b, off, n);
        currentPos += n;
        return n;
    }

    @Override
    public int available() throws IOException {
        ensureOpen();
        return currentLen - currentPos;
    }

    @Override
    public void close() throws IOException {
        boolean interrupted = false;
        final Fetch fetch;
        final boolean fetchRunning;
        synchronized (lock) {
            if (closed) {
                return;
            }
            closed = true;
            fetch = pending;
            pending = null;
            // a fetch that has not started will see the flag and do nothing, so there is nothing to wait for; one that has stops at its
            // next source read
            fetchRunning = fetch != null && fetch.started;
        }
        if (fetchRunning) {
            while (true) {
                try {
                    fetch.done.await();
                    break;
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        }
        if (fetch != null) {
            synchronized (lock) {
                release(fetch.buffer);
                fetch.buffer = null;
            }
        }
        release(current);
        release(spare);
        current = null;
        spare = null;
        currentPos = currentLen = 0;
        try {
            source.close();
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
            if (logger.isDebugEnabled()) {
                logger.debug(
                    "{} closed after [{}] blocks, [{}] ms of source reads on the executor, consumer waited [{}] ms for them",
                    description,
                    blocksDelivered,
                    TimeUnit.NANOSECONDS.toMillis(fetchNanosTotal),
                    TimeUnit.NANOSECONDS.toMillis(waitNanosTotal)
                );
            }
        }
    }

    /**
     * The bytes of the buffers that this stream holds, which is 0 once it is closed.
     */
    long getOutstandingBufferBytes() {
        return outstandingBufferBytes.get();
    }

    boolean isClosed() {
        return closed;
    }

    /**
     * Whether a fetch task is running, or about to run, for this stream.
     */
    boolean hasFetchInFlight() {
        synchronized (lock) {
            return pending != null && pending.done.getCount() > 0;
        }
    }

    private void ensureOpen() throws IOException {
        if (closed) {
            throw new IOException("read-ahead stream " + description + " is closed");
        }
    }

    /**
     * Makes the next block the current one, waiting for its fetch, and starts the fetch of the one after it.
     *
     * @return false at the end of the source
     */
    private boolean nextBlock() throws IOException {
        if (ended == false && pending == null) {
            if (requested >= length) {
                ended = true;
            } else {
                startFetch();
            }
        }
        if (pending == null) {
            // at the end, or after a failed fetch whose bytes were delivered
            if (deferredFailure != null) {
                throw rethrow(deferredFailure);
            }
            return false;
        }
        final Fetch fetch;
        synchronized (lock) {
            fetch = pending;
        }
        final long waitStart = System.nanoTime();
        try {
            fetch.done.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new InterruptedIOException("interrupted while waiting for the read-ahead of " + description);
        }
        synchronized (lock) {
            pending = null;
        }
        final long waitNanos = System.nanoTime() - waitStart;
        waitNanosTotal += waitNanos;
        fetchNanosTotal += fetch.fetchNanos;

        // the buffer the reader finished with is free for the next fetch
        spare = current;
        current = fetch.buffer;
        fetch.buffer = null;
        currentPos = 0;
        currentLen = fetch.length;
        blocksDelivered++;
        if (logger.isDebugEnabled()) {
            logger.debug(
                "{} read-ahead block [{}] at offset [{}]: [{}] bytes read from source in [{}] ms on [{}] from [{}] (epoch ms), "
                    + "reader waited [{}] ms",
                description,
                blocksDelivered - 1,
                fetch.offset,
                fetch.length,
                TimeUnit.NANOSECONDS.toMicros(fetch.fetchNanos) / 1000.0,
                fetch.threadName,
                fetch.startedAtMillis,
                TimeUnit.NANOSECONDS.toMicros(waitNanos) / 1000.0
            );
        }
        if (fetch.failure != null) {
            deferredFailure = fetch.failure;
            ended = true;
        } else if (fetch.length < fetch.size) {
            // the source ended before the length it was expected to have
            ended = true;
        } else if (requested < length) {
            startFetch();
        } else {
            ended = true;
        }
        if (currentLen == 0) {
            if (deferredFailure != null) {
                throw rethrow(deferredFailure);
            }
            return false;
        }
        return true;
    }

    private void startFetch() throws IOException {
        ensureOpen();
        final int size = (int) Math.min(blockSize, length - requested);
        byte[] buffer = spare;
        spare = null;
        if (buffer == null) {
            buffer = new byte[size];
            outstandingBufferBytes.addAndGet(buffer.length);
        }
        assert buffer.length >= size : "buffer of [" + buffer.length + "] bytes for a block of [" + size + "] bytes";
        final Fetch fetch = new Fetch(requested, size, buffer);
        requested += size;
        synchronized (lock) {
            pending = fetch;
        }
        try {
            executor.execute(() -> runFetch(fetch));
        } catch (RejectedExecutionException e) {
            // the executor is shutting down or full: this reader does the fetch itself, as without read-ahead
            runFetch(fetch);
        }
    }

    private void runFetch(Fetch fetch) {
        final byte[] buffer;
        synchronized (lock) {
            if (closed) {
                fetch.done.countDown();
                return;
            }
            buffer = fetch.buffer;
            fetch.started = true;
        }
        final long startedAtMillis = System.currentTimeMillis();
        final long startNanos = System.nanoTime();
        int n = 0;
        Throwable failure = null;
        try {
            while (n < fetch.size && closed == false) {
                final int read = source.read(buffer, n, Math.min(fetch.size - n, MAX_SOURCE_READ_BYTES));
                if (read < 0) {
                    break;
                }
                n += read;
            }
        } catch (Throwable t) {
            failure = t;
        }
        final long fetchNanos = System.nanoTime() - startNanos;
        // the latch makes these visible to the reader that waits for it
        fetch.length = n;
        fetch.failure = failure;
        fetch.fetchNanos = fetchNanos;
        fetch.startedAtMillis = startedAtMillis;
        fetch.threadName = Thread.currentThread().getName();
        fetch.done.countDown();
    }

    private void release(@Nullable byte[] buffer) {
        if (buffer != null) {
            outstandingBufferBytes.addAndGet(-buffer.length);
        }
    }

    private static IOException rethrow(Throwable t) throws IOException {
        if (t instanceof IOException e) {
            throw e;
        } else if (t instanceof RuntimeException e) {
            throw e;
        } else if (t instanceof Error e) {
            throw e;
        }
        throw new IOException(t);
    }

    /**
     * One block of the source, read by a task. {@code buffer} and {@code started} are guarded by the stream's lock. The fields after
     * {@code done} are set by the task before it counts the latch down, and are read by the reader after it has waited for it.
     */
    private static final class Fetch {
        final long offset;
        final int size;
        byte[] buffer;
        boolean started;
        final CountDownLatch done = new CountDownLatch(1);
        int length;
        Throwable failure;
        long fetchNanos;
        long startedAtMillis;
        String threadName;

        Fetch(long offset, int size, byte[] buffer) {
            this.offset = offset;
            this.size = size;
            this.buffer = buffer;
        }
    }
}
