/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.blobstore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.TimeUnit;

/**
 * Debug timing of the reads of a snapshot file's source while a part is uploaded. Reads are small (the repository buffer size), so they
 * are summed up and logged once per {@link #WINDOW_BYTES} read, which is also the default size of a GCS resumable upload chunk. Only active
 * when the {@code org.elasticsearch.repositories.blobstore.SnapshotSourceReadTiming} logger is at DEBUG. The time includes any wait in the
 * snapshot rate limiter.
 */
final class SnapshotSourceReadTiming {

    private static final Logger logger = LogManager.getLogger(SnapshotSourceReadTiming.class);

    static final long WINDOW_BYTES = 16L * 1024 * 1024;

    private final boolean enabled = logger.isDebugEnabled();
    private final String partName;
    private long windowBytes;
    private long windowReads;
    private long windowNanos;
    private long windowMaxNanos;
    private long totalReads;

    SnapshotSourceReadTiming(String partName) {
        this.partName = partName;
    }

    void record(int bytes, long nanos) {
        if (enabled == false || bytes <= 0) {
            return;
        }
        totalReads++;
        windowReads++;
        windowBytes += bytes;
        windowNanos += nanos;
        windowMaxNanos = Math.max(windowMaxNanos, nanos);
        if (windowBytes >= WINDOW_BYTES) {
            flushWindow();
        }
    }

    /**
     * Logs what is left of the last window and the totals of the part.
     */
    void finish(long partBytes, long uploadMillis) {
        if (enabled == false) {
            return;
        }
        flushWindow();
        logger.debug("part [{}] of [{}] bytes: upload took [{}] ms, [{}] source reads", partName, partBytes, uploadMillis, totalReads);
    }

    private void flushWindow() {
        if (windowReads > 0) {
            logger.debug(
                "part [{}] source read of [{}] bytes in [{}] reads took [{}] ms (longest read [{}] ms)",
                partName,
                windowBytes,
                windowReads,
                TimeUnit.NANOSECONDS.toMicros(windowNanos) / 1000.0,
                TimeUnit.NANOSECONDS.toMicros(windowMaxNanos) / 1000.0
            );
        }
        windowBytes = 0;
        windowReads = 0;
        windowNanos = 0;
        windowMaxNanos = 0;
    }
}
