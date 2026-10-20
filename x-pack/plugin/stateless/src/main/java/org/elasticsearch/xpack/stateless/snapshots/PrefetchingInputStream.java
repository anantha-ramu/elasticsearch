/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots;

import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.util.ArrayDeque;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Experimental. A stream over a source of known length that reads the source in ranges ahead of the reader, on the threads of an executor,
 * and hands the reader the bytes in order and unchanged, so that anything that sums them (a checksum) sees the same bytes as without it.
 * <p>
 * Up to {@code maxInFlight} ranges are requested ahead of the one the reader is consuming, each by a task that fills a buffer of pages from
 * the pool and completes. With {@code maxInFlight} of 1 and a {@link RangeOpener} that continues one stream, this reads ahead over a single
 * connection: while the reader consumes range N, one task reads range N+1. With more, and an opener that makes a ranged GET per call, the
 * ranges are fetched over as many connections at once, and still delivered in order. A file holds at most {@code maxInFlight + 1} buffers:
 * the ones in flight and the one the reader is consuming.
 * <ul>
 * <li>A task never waits for the reader or for another task, so a busy executor slows the reader down but cannot block it. When the
 * executor rejects a task, or the pool has no buffer for a range because the cap on the node's buffer memory is used up, fewer ranges are
 * in flight, and when there is none the reader reads the next range directly from the opener, as without prefetching.</li>
 * <li>An error of a fetch is delivered after the bytes that were read before it, as an error of the read that needs the missing bytes. If
 * a range ends before its length, the stream ends there, as a source that ends early would.</li>
 * <li>Tasks stop at their next read of the source once the stream is closed, or when {@code abortCheck} throws, which is how an abort or a
 * pause of the snapshot reaches them while the reader is busy elsewhere. The reader polls the same check while it waits.</li>
 * <li>{@link #close()} does not wait for tasks: buffers of ranges that no task is reading are released at once, the others by their task as
 * it stops, and the source is closed by the last task to stop.</li>
 * </ul>
 * The reader is a single thread. Not thread-safe for concurrent reads, as with any stream; {@link #close()} must not race with a read.
 */
final class PrefetchingInputStream extends InputStream {

    /**
     * Opens the bytes of one range of the source.
     */
    @FunctionalInterface
    interface RangeOpener {
        /**
         * @param offset from the start of the source
         * @return a stream of exactly {@code length} bytes from {@code offset}, whose {@link InputStream#close()} releases what it holds
         */
        InputStream open(long offset, long length) throws IOException;
    }

    /** How often a waiting reader checks whether the snapshot was aborted or paused. */
    static final long ABORT_CHECK_INTERVAL_MILLIS = 100;

    private enum State {
        QUEUED,
        RUNNING,
        DONE
    }

    private enum TopUp {
        FULL,
        END,
        NO_MEMORY,
        REJECTED
    }

    private final String description;
    private final long length;
    private final int rangeSize;
    private final int maxInFlight;
    private final RangeOpener opener;
    private final Executor executor;
    private final SourceReadBufferPool pool;
    private final Runnable abortCheck;
    @Nullable
    private final SourceReadTiming.Stats stats;
    @Nullable // closed once the stream is closed and no task is reading
    private final Closeable source;

    private final Object lock = new Object();
    private volatile boolean closed;
    // guarded by lock
    private final ArrayDeque<Fetch> window = new ArrayDeque<>();
    private int running;
    private boolean sourceClosed;

    // the state of the reader
    private long nextOffset; // the next range to request, relative to the start of the source
    @Nullable
    private Fetch current;
    private int currentPos;
    @Nullable
    private InputStream direct; // a range that is read without a buffer, when there is no memory or executor for one
    private long directRemaining;
    private boolean ended;
    @Nullable
    private Throwable deferredFailure;

    /**
     * @param description for error messages
     * @param length      the number of bytes the source is expected to deliver
     * @param rangeSize   the size of a range
     * @param maxInFlight the number of ranges that are requested ahead of the one that is consumed
     * @param opener      opens a range, on the thread of the task that reads it, or of the reader for a range that is read directly
     * @param executor    runs the tasks that fill the buffers
     * @param pool        lends the buffers
     * @param abortCheck  run before each read of the source, by the tasks, and by the reader while it waits: throws when the snapshot was
     *                    aborted or paused
     * @param stats       to record the waits and fetches in, if they are logged
     * @param source      closed after the stream was closed and the last task stopped, if the opener reads one stream that nothing else
     *                    closes
     */
    PrefetchingInputStream(
        String description,
        long length,
        int rangeSize,
        int maxInFlight,
        RangeOpener opener,
        Executor executor,
        SourceReadBufferPool pool,
        Runnable abortCheck,
        @Nullable SourceReadTiming.Stats stats,
        @Nullable Closeable source
    ) {
        if (rangeSize <= 0) {
            throw new IllegalArgumentException("range size must be positive, but was [" + rangeSize + "]");
        }
        if (maxInFlight <= 0) {
            throw new IllegalArgumentException("the number of ranges in flight must be positive, but was [" + maxInFlight + "]");
        }
        if (length < 0) {
            throw new IllegalArgumentException("length must not be negative, but was [" + length + "]");
        }
        this.description = description;
        this.length = length;
        this.rangeSize = rangeSize;
        this.maxInFlight = maxInFlight;
        this.opener = Objects.requireNonNull(opener);
        this.executor = Objects.requireNonNull(executor);
        this.pool = Objects.requireNonNull(pool);
        this.abortCheck = Objects.requireNonNull(abortCheck);
        this.stats = stats;
        this.source = source;
    }

    @Override
    public int read() throws IOException {
        final byte[] one = new byte[1];
        while (true) {
            final int n = read(one, 0, 1);
            if (n < 0) {
                return -1;
            }
            if (n > 0) {
                return one[0] & 0xFF;
            }
        }
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        Objects.checkFromIndexSize(off, len, b.length);
        ensureOpen();
        if (len == 0) {
            return 0;
        }
        while (true) {
            if (direct != null) {
                return readDirect(b, off, len);
            }
            if (current != null && currentPos < current.length) {
                return copyFromCurrent(b, off, len);
            }
            if (nextRange() == false) {
                return -1;
            }
        }
    }

    @Override
    public int available() throws IOException {
        ensureOpen();
        return current == null ? 0 : current.length - currentPos;
    }

    private int copyFromCurrent(byte[] b, int off, int len) {
        final int pageSize = pool.getPageSize();
        final int n = Math.min(len, current.length - currentPos);
        int copied = 0;
        while (copied < n) {
            final int pageOffset = currentPos % pageSize;
            final int chunk = Math.min(n - copied, pageSize - pageOffset);
            System.arraycopy(current.buffer.page(currentPos / pageSize), pageOffset, b, off + copied, chunk);
            currentPos += chunk;
            copied += chunk;
        }
        return n;
    }

    private int readDirect(byte[] b, int off, int len) throws IOException {
        final long start = System.nanoTime();
        final int n;
        try {
            n = direct.read(b, off, (int) Math.min(len, directRemaining));
        } finally {
            addWait(System.nanoTime() - start);
        }
        if (n < 0) {
            // the range ended before its length, so the source did
            ended = true;
            closeDirect();
            return -1;
        }
        directRemaining -= n;
        if (directRemaining == 0) {
            closeDirect();
        }
        return n;
    }

    private void closeDirect() throws IOException {
        final var toClose = direct;
        direct = null;
        directRemaining = 0;
        if (toClose != null) {
            toClose.close();
        }
    }

    /**
     * Makes the next range the one that is read, waiting for its fetch, and requests the ones after it.
     *
     * @return false at the end of the source
     */
    private boolean nextRange() throws IOException {
        releaseCurrent();
        final TopUp topUp = ended ? TopUp.END : topUp();
        final Fetch head;
        synchronized (lock) {
            head = window.peekFirst();
        }
        if (head == null) {
            if (ended || nextOffset >= length) {
                ended = true;
                if (deferredFailure != null) {
                    throw rethrow(deferredFailure);
                }
                return false;
            }
            startDirect(topUp == TopUp.NO_MEMORY);
            return true;
        }
        awaitFetch(head);
        synchronized (lock) {
            window.pollFirst();
        }
        current = head;
        currentPos = 0;
        if (head.failure != null) {
            deferredFailure = head.failure;
            ended = true;
        } else if (head.length < head.size) {
            // the range ended before its length, so the source did
            ended = true;
        }
        if (ended) {
            abandonWindow();
        } else {
            topUp();
        }
        if (head.length == 0) {
            releaseCurrent();
            if (deferredFailure != null) {
                throw rethrow(deferredFailure);
            }
            return false;
        }
        return true;
    }

    private void startDirect(boolean noMemory) throws IOException {
        final long start = System.nanoTime();
        final int size = (int) Math.min(rangeSize, length - nextOffset);
        try {
            direct = opener.open(nextOffset, size);
        } finally {
            addWait(System.nanoTime() - start);
        }
        directRemaining = size;
        nextOffset += size;
        if (stats != null) {
            stats.directRange();
            if (noMemory) {
                stats.capFallback();
            }
        }
    }

    private void awaitFetch(Fetch fetch) throws IOException {
        final long start = System.nanoTime();
        try {
            while (fetch.done.await(ABORT_CHECK_INTERVAL_MILLIS, TimeUnit.MILLISECONDS) == false) {
                try {
                    abortCheck.run();
                } catch (RuntimeException e) {
                    // the snapshot was aborted or paused: whatever this stream holds is given back now, not when the caller gets to close
                    // it
                    closeAfterAbort(e);
                    throw e;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new InterruptedIOException("interrupted while waiting for the read of " + description);
        } finally {
            addWait(System.nanoTime() - start);
        }
    }

    private void addWait(long nanos) {
        if (stats != null) {
            stats.addConsumerWait(nanos);
        }
    }

    /**
     * Requests ranges until {@code maxInFlight} are, the source is covered, or there is no buffer or executor for another.
     */
    private TopUp topUp() {
        while (true) {
            synchronized (lock) {
                if (closed) {
                    return TopUp.END;
                }
                if (nextOffset >= length) {
                    return TopUp.END;
                }
                if (window.size() >= maxInFlight) {
                    return TopUp.FULL;
                }
            }
            final int size = (int) Math.min(rangeSize, length - nextOffset);
            final SourceReadBufferPool.Buffer buffer = pool.tryAcquire(size);
            if (buffer == null) {
                return TopUp.NO_MEMORY;
            }
            final Fetch fetch = new Fetch(nextOffset, size, buffer);
            synchronized (lock) {
                if (closed) {
                    buffer.release();
                    return TopUp.END;
                }
                window.addLast(fetch);
            }
            try {
                executor.execute(() -> runFetch(fetch));
            } catch (RejectedExecutionException e) {
                synchronized (lock) {
                    window.remove(fetch);
                }
                buffer.release();
                return TopUp.REJECTED;
            }
            nextOffset += size;
        }
    }

    private void runFetch(Fetch fetch) {
        synchronized (lock) {
            if (closed || fetch.abandoned) {
                fetch.state = State.DONE;
                fetch.done.countDown();
                closeSourceIfIdleAfterTask();
                return;
            }
            fetch.state = State.RUNNING;
            running++;
        }
        final long startNanos = System.nanoTime();
        if (stats != null) {
            stats.fetchStarted();
        }
        int n = 0;
        Throwable failure = null;
        try {
            abortCheck.run();
            try (InputStream in = opener.open(fetch.offset, fetch.size)) {
                final int pageSize = pool.getPageSize();
                while (n < fetch.size && closed == false && fetch.abandoned == false) {
                    abortCheck.run();
                    final int pageOffset = n % pageSize;
                    final int read = in.read(fetch.buffer.page(n / pageSize), pageOffset, Math.min(fetch.size - n, pageSize - pageOffset));
                    if (read < 0) {
                        break;
                    }
                    n += read;
                }
            }
        } catch (Throwable t) {
            failure = t;
        }
        if (stats != null) {
            stats.fetchFinished(System.nanoTime() - startNanos);
        }
        // the latch makes these visible to the reader that waits for it
        fetch.length = n;
        fetch.failure = failure;
        synchronized (lock) {
            running--;
            fetch.state = State.DONE;
            if (closed || fetch.abandoned) {
                fetch.buffer.release();
            }
        }
        fetch.done.countDown();
        closeSourceIfIdleAfterTask();
    }

    private void closeSourceIfIdleAfterTask() {
        if (closed) {
            closeSourceIfIdle();
        }
    }

    private void closeSourceIfIdle() {
        final boolean doClose;
        synchronized (lock) {
            doClose = closed && running == 0 && source != null && sourceClosed == false;
            if (doClose) {
                sourceClosed = true;
            }
        }
        if (doClose) {
            IOUtils.closeWhileHandlingException(source);
        }
    }

    private void releaseCurrent() {
        final var fetch = current;
        current = null;
        currentPos = 0;
        if (fetch != null) {
            fetch.buffer.release();
        }
    }

    /**
     * Gives up the ranges that are in flight, when the stream ends before the source was covered.
     */
    private void abandonWindow() {
        synchronized (lock) {
            for (Fetch fetch : window) {
                fetch.abandoned = true;
                if (fetch.state != State.RUNNING) {
                    fetch.buffer.release();
                }
            }
            window.clear();
        }
    }

    private void closeAfterAbort(RuntimeException abort) {
        try {
            close();
        } catch (IOException | RuntimeException e) {
            abort.addSuppressed(e);
        }
    }

    @Override
    public void close() throws IOException {
        synchronized (lock) {
            if (closed) {
                return;
            }
            closed = true;
            for (Fetch fetch : window) {
                fetch.abandoned = true;
                if (fetch.state != State.RUNNING) {
                    fetch.buffer.release();
                }
            }
            window.clear();
        }
        releaseCurrent();
        try {
            closeDirect();
        } finally {
            closeSourceIfIdle();
        }
    }

    boolean isClosed() {
        return closed;
    }

    private void ensureOpen() throws IOException {
        if (closed) {
            throw new IOException("prefetching stream of " + description + " is closed");
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
     * One range of the source, read by a task. {@code state} and {@code abandoned} are guarded by the stream's lock. The fields after
     * {@code done} are set by the task before it counts the latch down, and are read by the reader after it has waited for it.
     */
    private static final class Fetch {
        final long offset;
        final int size;
        final SourceReadBufferPool.Buffer buffer;
        State state = State.QUEUED;
        volatile boolean abandoned;
        final CountDownLatch done = new CountDownLatch(1);
        int length;
        Throwable failure;

        Fetch(long offset, int size, SourceReadBufferPool.Buffer buffer) {
            this.offset = offset;
            this.size = size;
            this.buffer = buffer;
        }
    }
}
