/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.gcs;

import com.google.api.gax.retrying.ResultRetryAlgorithm;
import com.google.cloud.storage.StorageException;

import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.net.SocketException;
import java.net.SocketTimeoutException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class ResumableUploadChunksTests extends ESTestCase {

    private static StorageException chunkTimeout() {
        // what the SDK throws when a chunk PUT gets no response in time
        return new StorageException(0, "Unknown Error", null, new SocketTimeoutException("Read timed out"));
    }

    private static StorageException chunkWriteError() {
        return new StorageException(0, "Unknown Error", null, new IOException("Error writing request body to server"));
    }

    private static boolean strategyRetries(Throwable failure) throws IOException {
        final var service = new GoogleCloudStorageService(
            ClusterServiceUtils.createClusterService(new DeterministicTaskQueue().getThreadPool()),
            TestProjectResolvers.DEFAULT_PROJECT_ONLY
        );
        @SuppressWarnings("unchecked")
        final var algorithm = (ResultRetryAlgorithm<Object>) service.getRetryStrategy().getIdempotentHandler();
        return algorithm.shouldRetry(failure, null);
    }

    public void testChunkTimeoutIsOnlyRetriedWhileUploadingAChunk() throws IOException {
        assertFalse(strategyRetries(chunkTimeout()));
        assertFalse(strategyRetries(chunkWriteError()));

        final long before = ResumableUploadChunks.getRetriedChunks();
        ResumableUploadChunks.track("blob", "write", 0, 1024, () -> {
            assertTrue(strategyRetries(chunkTimeout()));
            assertTrue(strategyRetries(chunkWriteError()));
            return null;
        });
        assertThat(ResumableUploadChunks.getRetriedChunks(), equalTo(before + 2));

        // the context is gone once the call returns, also when it fails
        assertFalse(strategyRetries(chunkTimeout()));
        expectThrows(
            IOException.class,
            () -> ResumableUploadChunks.track("blob", "close", 0, -1, () -> { throw new IOException("boom"); })
        );
        assertFalse(strategyRetries(chunkTimeout()));
    }

    public void testOtherFailuresKeepTheSdkClassificationWhileUploadingAChunk() throws IOException {
        final long before = ResumableUploadChunks.getRetriedChunks();
        ResumableUploadChunks.track("blob", "write", 0, 1024, () -> {
            // not a chunk request failure
            assertFalse(strategyRetries(new StorageException(0, "Unknown Error", null, new IOException("something else"))));
            assertFalse(strategyRetries(new StorageException(0, "Unknown Error", null, null)));
            assertFalse(strategyRetries(new StorageException(412, "Precondition Failed")));
            assertFalse(strategyRetries(new StorageException(404, "Not Found")));
            assertFalse(strategyRetries(new SocketTimeoutException("Read timed out")));
            assertFalse(strategyRetries(new IllegalStateException("bug")));
            // already retryable in the SDK, no need for the special case
            assertTrue(strategyRetries(new StorageException(503, "Service Unavailable")));
            assertTrue(strategyRetries(new StorageException(0, "Unknown Error", null, new SocketException("Connection reset"))));
            return null;
        });
        assertThat(ResumableUploadChunks.getRetriedChunks(), equalTo(before));
    }

    public void testAttemptsAreCountedPerChunk() throws IOException {
        final long before = ResumableUploadChunks.getRetriedChunks();
        final int attempts = between(2, 5);
        ResumableUploadChunks.track("blob", "write", 0, 1024, () -> {
            for (int i = 0; i < attempts; i++) {
                assertTrue(ResumableUploadChunks.shouldRetry(chunkTimeout()));
            }
            return null;
        });
        assertThat(ResumableUploadChunks.getRetriedChunks() - before, equalTo((long) attempts));
        assertThat(attempts, greaterThan(1));
    }
}
