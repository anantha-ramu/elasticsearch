/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.gcs;

import com.google.cloud.storage.StorageException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.core.CheckedSupplier;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks the chunk request a thread is currently sending in a resumable upload.
 * <p>
 * The SDK wraps a read timeout on a chunk PUT (or on the status query that follows a failed one) in a {@link StorageException} with code 0,
 * which its retry strategy treats as not retryable, so one stalled chunk fails the whole upload even though the SDK can resume from the
 * offset the server has committed. The retry strategy asks {@link #shouldRetry} about such failures. It only says yes while the calling
 * thread is inside {@link #track}, which the resumable write channel wraps around every write and close, so metadata writes, reads, deletes
 * and the multipart upload path keep the SDK's classification.
 * <p>
 * The SDK runs its retries on the thread that made the call, so the thread local is visible to the retry strategy.
 */
final class ResumableUploadChunks {

    private static final Logger logger = LogManager.getLogger(ResumableUploadChunks.class);

    private static final String WRITE_ERROR_MESSAGE = "Error writing request body to server";

    private static final ThreadLocal<Chunk> CURRENT = new ThreadLocal<>();

    private static final AtomicLong retriedChunks = new AtomicLong();

    private ResumableUploadChunks() {}

    /**
     * The chunk request in flight on a thread: a write that may flush a chunk, or the close that sends the final one.
     */
    private static final class Chunk {
        private final String blobName;
        private final String phase;
        private final long offset;
        private final long bytes;
        private int failedAttempts;

        Chunk(String blobName, String phase, long offset, long bytes) {
            this.blobName = blobName;
            this.phase = phase;
            this.offset = offset;
            this.bytes = bytes;
        }
    }

    /**
     * Runs a write or close on a resumable channel, during which retryable chunk failures are retried by the SDK.
     *
     * @param offset the number of bytes the channel had accepted before this call
     * @param bytes  the number of bytes passed in this call, or -1 if unknown (close)
     */
    static <T> T track(String blobName, String phase, long offset, long bytes, CheckedSupplier<T, IOException> call) throws IOException {
        final Chunk previous = CURRENT.get();
        CURRENT.set(new Chunk(blobName, phase, offset, bytes));
        try {
            return call.get();
        } finally {
            if (previous == null) {
                CURRENT.remove();
            } else {
                CURRENT.set(previous);
            }
        }
    }

    /**
     * Whether a failed attempt should be retried because it is a timed-out chunk request of a resumable upload. Called by the retry
     * strategy for every failed attempt, so a {@code true} answer means "classified as retryable", the SDK may still stop because the
     * attempt or backoff budget is used up.
     */
    static boolean shouldRetry(Throwable failure) {
        final Chunk chunk = CURRENT.get();
        if (chunk == null) {
            return false;
        }
        if ((failure instanceof StorageException storageException && storageException.getCode() == 0) == false) {
            return false;
        }
        final Throwable timeout = findTimeoutOrWriteError(failure);
        if (timeout != null) {
            chunk.failedAttempts++;
            final long total = retriedChunks.incrementAndGet();
            logger.warn(
                "resumable upload of blob [{}]: attempt [{}] failed on a {} of [{}] bytes ([{}] bytes accepted before it), "
                    + "will resume from the committed offset: [{}], chunk retries since startup [{}]",
                chunk.blobName,
                chunk.failedAttempts,
                chunk.phase,
                chunk.bytes,
                chunk.offset,
                timeout.getClass().getSimpleName() + ": " + timeout.getMessage(),
                total
            );
            return true;
        }
        return false;
    }

    /**
     * The number of chunk requests classified as retryable since startup.
     */
    static long getRetriedChunks() {
        return retriedChunks.get();
    }

    /**
     * The timeout or write error that made the SDK give up on a chunk request, looked up along the causes, or null if there is none.
     */
    private static Throwable findTimeoutOrWriteError(Throwable failure) {
        Throwable current = failure;
        for (int depth = 0; current != null && depth < 10; depth++, current = current.getCause()) {
            if (current instanceof SocketTimeoutException || (current instanceof IOException && isWriteError(current))) {
                return current;
            }
        }
        return null;
    }

    private static boolean isWriteError(Throwable t) {
        return t.getMessage() != null && t.getMessage().startsWith(WRITE_ERROR_MESSAGE);
    }
}
