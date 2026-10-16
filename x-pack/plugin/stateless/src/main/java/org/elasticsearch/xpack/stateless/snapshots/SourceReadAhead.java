/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots;

import org.elasticsearch.common.settings.ClusterSettings;

import java.io.InputStream;
import java.util.concurrent.Executor;

/**
 * Experimental. Decides whether the stream that a stateless snapshot reads a file's bytes from is read ahead, see
 * {@link ReadAheadInputStream}, and with what block size. Both are dynamic settings, read once each time a stream is opened, so a change
 * applies to the next stream and never to one that is being read. When read-ahead is off, or the stream is no longer than a block, the
 * stream is returned as it is and the executor is not used.
 */
final class SourceReadAhead {

    static final int DEFAULT_BLOCK_SIZE_BYTES = 16 * 1024 * 1024;

    private final Executor executor;
    private volatile boolean enabled;
    private volatile int blockSize;

    SourceReadAhead(Executor executor, boolean enabled, int blockSize) {
        this.executor = executor;
        this.enabled = enabled;
        this.blockSize = blockSize;
    }

    /**
     * @return a read-ahead that follows the node's settings, and runs its fetches on the given executor
     */
    static SourceReadAhead create(Executor executor, ClusterSettings clusterSettings) {
        // initializeAndWatch applies the current values at once
        final var readAhead = new SourceReadAhead(executor, false, DEFAULT_BLOCK_SIZE_BYTES);
        clusterSettings.initializeAndWatch(StatelessSnapshotSettings.SOURCE_READ_AHEAD_ENABLED_SETTING, readAhead::setEnabled);
        clusterSettings.initializeAndWatch(
            StatelessSnapshotSettings.SOURCE_READ_AHEAD_BLOCK_SIZE_SETTING,
            size -> readAhead.setBlockSize(Math.toIntExact(size.getBytes()))
        );
        return readAhead;
    }

    void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    void setBlockSize(int blockSize) {
        this.blockSize = blockSize;
    }

    boolean isEnabled() {
        return enabled;
    }

    /**
     * @param source      the stream of the bytes of a file part, as opened
     * @param length      the number of bytes the stream is expected to deliver
     * @param description for the logs
     * @return the stream to read from: the source itself unless read-ahead is on and the source is longer than one block
     */
    InputStream maybeWrap(InputStream source, long length, String description) {
        final boolean enabled = this.enabled;
        final int blockSize = this.blockSize;
        if (enabled == false || length <= blockSize) {
            return source;
        }
        return new ReadAheadInputStream(description, source, length, blockSize, executor);
    }
}
