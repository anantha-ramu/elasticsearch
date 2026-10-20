/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.Executor;

/**
 * Experimental. Decides how the stream that a stateless snapshot reads a file's bytes from is read, and builds it: as it is opened
 * ({@link Mode#SINGLE}, which is what happens when everything here is off), read ahead over one connection ({@link Mode#READ_AHEAD}), or
 * read as ranges over several connections at once ({@link Mode#PARALLEL}), see {@link PrefetchingInputStream}. The mode comes from dynamic
 * settings, which are read once when the file starts, as a {@link Plan}, so that a change applies to the next file and never to one that is
 * being read.
 */
final class SourceReadPrefetch {

    enum Mode {
        SINGLE("single"),
        READ_AHEAD("readahead"),
        PARALLEL("parallel");

        private final String description;

        Mode(String description) {
            this.description = description;
        }
    }

    /**
     * How one file is read, fixed when it starts.
     */
    record Plan(Mode mode, int connections, int rangeSize) {
        static final Plan SINGLE = new Plan(Mode.SINGLE, 1, SourceReadBufferPool.PAGE_SIZE_BYTES);

        String describe() {
            return mode == Mode.PARALLEL ? mode.description + " " + connections : mode.description;
        }
    }

    private final Executor executor;
    private final SourceReadBufferPool pool;
    private volatile boolean readAhead;
    private volatile int connections = 1;
    private volatile int rangeSize = SourceReadBufferPool.PAGE_SIZE_BYTES;

    SourceReadPrefetch(Executor executor, SourceReadBufferPool pool) {
        this.executor = executor;
        this.pool = pool;
    }

    /**
     * @return a prefetch that follows the node's settings, and runs its fetches on the given executor
     */
    static SourceReadPrefetch create(Executor executor, ClusterSettings clusterSettings) {
        final var prefetch = new SourceReadPrefetch(executor, new SourceReadBufferPool(0));
        // initializeAndWatch applies the current values at once
        clusterSettings.initializeAndWatch(StatelessSnapshotSettings.SOURCE_READ_AHEAD_SETTING, prefetch::setReadAhead);
        clusterSettings.initializeAndWatch(StatelessSnapshotSettings.SOURCE_READ_PARALLEL_CONNECTIONS_SETTING, prefetch::setConnections);
        clusterSettings.initializeAndWatch(
            StatelessSnapshotSettings.SOURCE_READ_RANGE_SIZE_SETTING,
            size -> prefetch.setRangeSize(Math.toIntExact(size.getBytes()))
        );
        clusterSettings.initializeAndWatch(
            StatelessSnapshotSettings.SOURCE_READ_MAX_BUFFER_MEMORY_SETTING,
            size -> prefetch.pool.setCapBytes(size.getBytes())
        );
        return prefetch;
    }

    void setReadAhead(boolean readAhead) {
        this.readAhead = readAhead;
        maybeDropPooledPages();
    }

    void setConnections(int connections) {
        this.connections = connections;
        maybeDropPooledPages();
    }

    void setRangeSize(int rangeSize) {
        this.rangeSize = rangeSize;
    }

    private void maybeDropPooledPages() {
        // the pages that were kept for reuse are of no use once nothing reads ahead, and are heap
        if (plan().mode() == Mode.SINGLE) {
            pool.clear();
        }
    }

    SourceReadBufferPool getPool() {
        return pool;
    }

    /**
     * @return how a file that starts now is read
     */
    Plan plan() {
        final int connections = this.connections;
        final int rangeSize = this.rangeSize;
        if (connections > 1) {
            return new Plan(Mode.PARALLEL, connections, rangeSize);
        } else if (readAhead) {
            return new Plan(Mode.READ_AHEAD, 1, rangeSize);
        }
        return Plan.SINGLE;
    }

    /**
     * Opens the stream of the bytes of a file part.
     *
     * @param plan        how the file is read
     * @param length      the number of bytes the stream is expected to deliver
     * @param ranges      opens a ranged read of the blob, at an offset from the start of the stream
     * @param abortCheck  run before each read of the source, by the tasks that read it, and by the reader while it waits for them: throws
     *                    when the snapshot was aborted or paused
     * @param description for error messages
     * @param stats       what is recorded about the stream, if it is logged
     * @return the stream to read from: the one of {@code ranges} itself unless the plan reads ahead and the stream is longer than a range
     */
    InputStream open(
        Plan plan,
        long length,
        PrefetchingInputStream.RangeOpener ranges,
        Runnable abortCheck,
        String description,
        @Nullable SourceReadTiming.Stats stats
    ) throws IOException {
        if (plan.mode() == Mode.SINGLE || length <= plan.rangeSize()) {
            return ranges.open(0, length);
        }
        return switch (plan.mode()) {
            case READ_AHEAD -> {
                final InputStream source = ranges.open(0, length);
                try {
                    final var sequential = new SequentialView(source);
                    yield new PrefetchingInputStream(
                        description,
                        length,
                        plan.rangeSize(),
                        1,
                        (offset, len) -> sequential.continueAt(offset),
                        executor,
                        pool,
                        abortCheck,
                        stats,
                        source
                    );
                } catch (RuntimeException | Error e) {
                    IOUtils.closeWhileHandlingException(source);
                    throw e;
                }
            }
            case PARALLEL -> new PrefetchingInputStream(
                description,
                length,
                plan.rangeSize(),
                plan.connections(),
                ranges,
                executor,
                pool,
                abortCheck,
                stats,
                null
            );
            case SINGLE -> throw new AssertionError("handled above");
        };
    }

    /**
     * What a plan reads, for a stream of this length: its description if the plan reads ahead of it, "single" if it does not.
     */
    static String modeOf(Plan plan, long length) {
        return plan.mode() == Mode.SINGLE || length <= plan.rangeSize() ? Mode.SINGLE.description : plan.describe();
    }

    /**
     * One stream that the ranges of a read ahead are read from in turn, which never closes it: it is closed with the read ahead.
     */
    private static final class SequentialView extends FilterInputStream {
        private long position;

        SequentialView(InputStream in) {
            super(in);
        }

        SequentialView continueAt(long offset) {
            assert offset == position
                : "ranges of a stream that is read ahead are read in order, but offset [" + offset + "] != " + position;
            return this;
        }

        @Override
        public int read() throws IOException {
            final int b = super.read();
            if (b >= 0) {
                position++;
            }
            return b;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            final int n = super.read(b, off, len);
            if (n > 0) {
                position += n;
            }
            return n;
        }

        @Override
        public void close() {}
    }
}
