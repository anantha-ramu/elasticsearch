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

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Experimental. What a snapshot of a shard learns about reading a file from the object store, logged at DEBUG as one line per file part
 * when its stream is closed, to tell where the time of a read goes: waiting for bytes, or somewhere else, such as the snapshot rate
 * limiter. The lines of each GET that the read makes come from the GCS client, under the logger
 * {@code org.elasticsearch.repositories.gcs.GetTiming}, for every GET of the client; they carry the same blob and range start, so the two
 * can be joined.
 * <p>
 * Nothing is recorded or wrapped unless the logger is at DEBUG when the stream is opened.
 */
final class SourceReadTiming {

    private static final Logger logger = LogManager.getLogger(SourceReadTiming.class);

    /** The files whose parts are being read at the moment, by blob, to tell how many workers read the same blob at once. */
    private static final ConcurrentHashMap<String, AtomicInteger> READERS_BY_BLOB = new ConcurrentHashMap<>();

    private SourceReadTiming() {}

    static boolean isEnabled() {
        return logger.isDebugEnabled();
    }

    /**
     * What is learned about one file part. The counters are updated by the thread that reads the stream and by the fetch tasks.
     */
    static final class Stats {
        private final String file;
        private final String blob;
        private final long blobPosition;
        private final long length;
        private final String mode;
        private final long startNanos = System.nanoTime();
        private final int otherReadersOfBlob;

        private final AtomicLong bytesDelivered = new AtomicLong();
        private final AtomicLong consumerWaitNanos = new AtomicLong();
        private final AtomicLong limiterPauseNanos = new AtomicLong();
        private final AtomicLong fetchNanos = new AtomicLong();
        private final AtomicInteger fetches = new AtomicInteger();
        private final AtomicInteger directRanges = new AtomicInteger();
        private final AtomicInteger capFallbacks = new AtomicInteger();
        private final AtomicInteger inFlight = new AtomicInteger();
        private final AtomicInteger maxInFlight = new AtomicInteger();
        private boolean logged;

        Stats(String file, String blob, long blobPosition, long length, String mode) {
            this.file = file;
            this.blob = blob;
            this.blobPosition = blobPosition;
            this.length = length;
            this.mode = mode;
            this.otherReadersOfBlob = READERS_BY_BLOB.computeIfAbsent(blob, b -> new AtomicInteger()).getAndIncrement();
        }

        void addConsumerWait(long nanos) {
            consumerWaitNanos.addAndGet(nanos);
        }

        void addDelivered(long bytes) {
            bytesDelivered.addAndGet(bytes);
        }

        /**
         * The time the snapshot rate limiter paused the reader for, as reported by its listener.
         */
        void addLimiterPause(long nanos) {
            limiterPauseNanos.addAndGet(nanos);
        }

        void fetchStarted() {
            final int now = inFlight.incrementAndGet();
            maxInFlight.accumulateAndGet(now, Math::max);
            fetches.incrementAndGet();
        }

        void fetchFinished(long nanos) {
            inFlight.decrementAndGet();
            fetchNanos.addAndGet(nanos);
        }

        void directRange() {
            directRanges.incrementAndGet();
        }

        void capFallback() {
            capFallbacks.incrementAndGet();
        }

        int getMaxInFlight() {
            return maxInFlight.get();
        }

        long getConsumerWaitNanos() {
            return consumerWaitNanos.get();
        }

        long getLimiterPauseNanos() {
            return limiterPauseNanos.get();
        }

        int getOtherReadersOfBlob() {
            return otherReadersOfBlob;
        }

        void logAndRelease() {
            synchronized (this) {
                if (logged) {
                    return;
                }
                logged = true;
            }
            READERS_BY_BLOB.computeIfPresent(blob, (b, readers) -> readers.decrementAndGet() <= 0 ? null : readers);
            final long wallNanos = System.nanoTime() - startNanos;
            logger.debug(
                "file [{}] blob [{}] from [{}]: mode [{}] length [{}] delivered [{}] bytes in [{}] ms, consumer waited for data [{}] ms, "
                    + "limiter paused [{}] ms, [{}] fetches took [{}] ms in total with at most [{}] in flight, [{}] ranges read directly "
                    + "([{}] of them because the buffer memory was used up), [{}] other readers of the blob when it started",
                file,
                blob,
                blobPosition,
                mode,
                length,
                bytesDelivered.get(),
                TimeUnit.NANOSECONDS.toMillis(wallNanos),
                TimeUnit.NANOSECONDS.toMillis(consumerWaitNanos.get()),
                TimeUnit.NANOSECONDS.toMillis(limiterPauseNanos.get()),
                fetches.get(),
                TimeUnit.NANOSECONDS.toMillis(fetchNanos.get()),
                maxInFlight.get(),
                directRanges.get(),
                capFallbacks.get(),
                otherReadersOfBlob
            );
        }
    }

    /**
     * Logs the {@link Stats} when the stream of a file part closes. When {@code timeReads} is set, the stream also counts the time of its
     * reads as the time that the consumer waited for data, which is what they are for a stream that is not read ahead.
     */
    static final class LoggingInputStream extends FilterInputStream {
        private final Stats stats;
        private final boolean timeReads;

        LoggingInputStream(InputStream in, Stats stats, boolean timeReads) {
            super(in);
            this.stats = stats;
            this.timeReads = timeReads;
        }

        @Override
        public int read() throws IOException {
            final long start = timeReads ? System.nanoTime() : 0L;
            try {
                final int b = in.read();
                if (b != -1) {
                    stats.addDelivered(1);
                }
                return b;
            } finally {
                if (timeReads) {
                    stats.addConsumerWait(System.nanoTime() - start);
                }
            }
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            final long start = timeReads ? System.nanoTime() : 0L;
            try {
                final int n = in.read(b, off, len);
                if (n > 0) {
                    stats.addDelivered(n);
                }
                return n;
            } finally {
                if (timeReads) {
                    stats.addConsumerWait(System.nanoTime() - start);
                }
            }
        }

        @Override
        public void close() throws IOException {
            try {
                super.close();
            } finally {
                stats.logAndRelease();
            }
        }
    }

    /**
     * @return the stream, logging when it closes if the logger is at DEBUG; the stream as it is otherwise
     */
    static InputStream maybeLog(InputStream stream, @Nullable Stats stats, boolean timeReads) {
        return stats == null ? stream : new LoggingInputStream(stream, stats, timeReads);
    }
}
