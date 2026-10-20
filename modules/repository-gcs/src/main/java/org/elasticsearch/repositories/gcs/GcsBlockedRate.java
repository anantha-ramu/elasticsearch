/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.repositories.gcs;

import java.util.Locale;

/**
 * The rate of a connection, estimated over the reads or writes that waited for it: the bytes and time of the calls that took at least
 * {@link #BLOCKED_NANOS}. A call that is served from what the kernel and the TLS layer already hold takes less, and says nothing about the
 * connection, so leaving those out removes the pauses of the caller from the estimate. Not thread-safe.
 * <p>
 * The threshold is small because a TLS read returns at most one record of 16 KiB, which at 85 MiB/s arrives about every 0.2 ms: a threshold
 * of a millisecond would count almost no reads, and cap the estimate near 16 MiB/s whatever the connection does.
 * <p>
 * For reads this is an estimate of the rate at which the connection delivers. For writes it is an upper bound: a socket write returns
 * once the data is in the kernel's send buffer, so the writes that waited are those that found it full, and the time they took is when the
 * connection made room, which can be a little before all the bytes of the write have gone.
 */
final class GcsBlockedRate {

    /** A read or write that takes this long has waited, as one that is served from what was already there does not. */
    static final long BLOCKED_NANOS = 50_000;

    private static final double MIB = 1024 * 1024;

    private long blockedNanos;
    private long blockedBytes;

    void record(long tookNanos, long bytes) {
        if (bytes > 0 && tookNanos >= BLOCKED_NANOS) {
            blockedNanos += tookNanos;
            blockedBytes += bytes;
        }
    }

    long getBlockedNanos() {
        return blockedNanos;
    }

    long getBlockedBytes() {
        return blockedBytes;
    }

    /**
     * @return MiB/s over the calls that waited, or "n/a" if none did
     */
    String describe() {
        return blockedNanos <= 0 ? "n/a" : String.format(Locale.ROOT, "%.1f", blockedBytes / MIB / (blockedNanos / 1e9));
    }
}
