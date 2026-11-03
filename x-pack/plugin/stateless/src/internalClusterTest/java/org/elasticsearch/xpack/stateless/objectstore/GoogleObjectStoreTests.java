/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.objectstore;

import fixture.gcs.FakeOAuth2HttpHandler;
import fixture.gcs.GoogleCloudStorageHttpHandler;
import fixture.gcs.TestUtils;

import com.google.cloud.storage.Storage;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobStoreActionStats;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoryStats;
import org.elasticsearch.repositories.gcs.GcsRepositoryStatsCollector;
import org.elasticsearch.repositories.gcs.GoogleCloudStoragePlugin;
import org.elasticsearch.repositories.gcs.GoogleCloudStorageService;
import org.elasticsearch.repositories.gcs.MeteredStorage;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskId;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;

public class GoogleObjectStoreTests extends AbstractMockObjectStoreIntegTestCase {

    @SuppressForbidden(reason = "this test uses a HttpServer to emulate a GCS endpoint")
    private InterceptableGcsHttpHandler gcsHttpHandler;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), TestGoogleCloudStoragePlugin.class);
    }

    @Override
    protected Map<String, HttpHandler> createHttpHandlers() {
        gcsHttpHandler = new InterceptableGcsHttpHandler("bucket");
        return Map.of("/", gcsHttpHandler, "/token", new FakeOAuth2HttpHandler());
    }

    @Override
    protected Settings.Builder nodeSettings() {
        final Settings.Builder settings = super.nodeSettings();
        settings.put("gcs.client.test.endpoint", httpServerUrl());
        settings.put("gcs.client.test.token_uri", httpServerUrl() + "/token");
        settings.put("gcs.client.backup.endpoint", httpServerUrl());
        settings.put("gcs.client.backup.token_uri", httpServerUrl() + "/token");
        settings.put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.GCS);
        settings.put(ObjectStoreService.BUCKET_SETTING.getKey(), "bucket");
        settings.put(ObjectStoreService.CLIENT_SETTING.getKey(), "test");

        final byte[] serviceAccount = TestUtils.createServiceAccount(random(), UUID.randomUUID().toString(), "admin@cluster.com");
        MockSecureSettings mockSecureSettings = new MockSecureSettings();
        mockSecureSettings.setFile("gcs.client.test.credentials_file", serviceAccount);
        settings.setSecureSettings(mockSecureSettings);

        return settings;
    }

    @Override
    protected String repositoryType() {
        return "gcs";
    }

    @Override
    protected Settings repositorySettings() {
        return Settings.builder()
            .put(super.repositorySettings())
            .put("bucket", "bucket")
            .put("base_path", "backup")
            .put("client", "test")
            .build();
    }

    void assertMetricStats(Set<String> expectedMetrics, Map<String, BlobStoreActionStats> actualMetrics) {
        for (var expectedMetric : expectedMetrics) {
            var actualStats = actualMetrics.get(expectedMetric);
            assertNotNull(expectedMetric + " not found in " + actualMetrics, actualStats);
            assertTrue(expectedMetric + " value should be not zero", actualStats.operations() > 0);
        }
    }

    @Override
    protected void assertRepositoryStats(RepositoryStats actualStats, boolean withRandomCrud, OperationPurpose randomPurpose) {
        var expectedMetrics = new HashSet<>(
            List.of(
                "ClusterState_GetObject",
                "ClusterState_InsertObject",
                "ClusterState_ListObjects",
                "Indices_ListObjects",
                "Translog_ListObjects"
            )
        );
        if (withRandomCrud) {
            for (var op : List.of("GetObject", "ListObjects", "InsertObject")) {
                expectedMetrics.add(randomPurpose.getKey() + "_" + op);
            }
        }
        assertMetricStats(expectedMetrics, actualStats.actionStats);
    }

    @Override
    protected void assertObsRepositoryStatsSnapshots(RepositoryStats actualStats) {
        var expectedMetrics = new HashSet<>(
            List.of(
                "SnapshotMetadata_GetObject",
                "SnapshotMetadata_ListObjects",
                "SnapshotMetadata_InsertObject",
                "SnapshotData_ListObjects"
            )
        );
        assertMetricStats(expectedMetrics, actualStats.actionStats);
    }

    @Override
    protected boolean canRefuseCopies() {
        return true;
    }

    @SuppressForbidden(reason = "this test uses a HttpServer to emulate a GCS endpoint")
    @Override
    protected void refuseCopiesAfterTheFirst(AtomicInteger copyRequests) {
        gcsHttpHandler.interceptor = exchange -> {
            // a copy is a rewrite request
            if (exchange.getRequestMethod().equals("POST")
                && exchange.getRequestURI().getPath().contains("/rewriteTo/")
                && copyRequests.incrementAndGet() > 1) {
                try (exchange) {
                    final byte[] response = ("{\"error\":{\"code\":403,\"message\":\""
                        + accessDeniedText()
                        + "\","
                        + "\"errors\":[{\"message\":\""
                        + accessDeniedText()
                        + "\",\"domain\":\"global\",\"reason\":\"forbidden\"}]}}").getBytes(StandardCharsets.UTF_8);
                    exchange.getResponseHeaders().add("Content-Type", "application/json; charset=utf-8");
                    exchange.sendResponseHeaders(403, response.length);
                    exchange.getResponseBody().write(response);
                }
                return true;
            }
            return false;
        };
    }

    @Override
    protected Set<String> blobPathsInFakeStorage() {
        return Set.copyOf(gcsHttpHandler.blobs().keySet());
    }

    @Override
    protected String accessDeniedText() {
        return "the caller does not have storage.objects.get access";
    }

    public void testRecoverySucceedAgainstTransientCSPError() throws Exception {
        final var settings = Settings.builder()
            .put("gcs.client.test.max_retries", 0) // disable sdk client retries
            .put("gcs.client.test.tenacious_retries.enabled", true)
            .build();
        final var node0 = startMasterAndIndexNode(settings);
        final var node1 = startIndexNode(settings);
        ensureStableCluster(2);

        // Create the index with shard on node1 then start the 3rd node
        final var indexName = randomIndexName();
        createIndex(indexName, indexSettings(1, 0).put("index.routing.allocation.exclude._name", node0).build());
        ensureGreen(indexName);
        indexDocs(indexName, between(10, 100));
        flush(indexName);

        final var node2 = startIndexNode(settings);
        ensureStableCluster(3);

        // Inject errors on the 3rd node. Stop the node for ungraceful relocation since graceful relocation gets the list of blobs
        // from the source node and avoids listing operations which are what we want to test against.
        final var numberOfErrors = new AtomicInteger(5);
        findPlugin(node2, TestGoogleCloudStoragePlugin.class).shouldInjectErrorRef.set(() -> numberOfErrors.decrementAndGet() >= 0);

        logger.info("--> stopping " + node1);
        internalCluster().stopNode(node1);
        ensureGreen(indexName);
    }

    public void testCopyShardWithTenaciousRetries() throws Exception {
        final var settings = Settings.builder().put("gcs.client.test.tenacious_retries.enabled", true).build();
        final String node = startMasterAndIndexNode(settings);
        try {
            final var objectStoreService = getCurrentMasterObjectStoreService();

            final String indexName = randomIdentifier();
            createIndex(indexName, indexSettings(1, 0).build());
            final var sourceShardId = new ShardId(resolveIndex(indexName), 0);
            final var destShardId = new ShardId(resolveIndex(indexName), 1);
            final long primaryTerm = 1L;

            final var blobData = randomBytesReference(between(1, 100));
            objectStoreService.getProjectBlobContainer(sourceShardId, primaryTerm)
                .writeBlob(OperationPurpose.INDICES, "commit-1", blobData, true);

            final var task = new CancellableTask(1L, "test", "copyShard", "", TaskId.EMPTY_TASK_ID, Map.of());
            objectStoreService.copyShard(task, sourceShardId, destShardId, primaryTerm);

            try (
                var is = objectStoreService.getProjectBlobContainer(destShardId, primaryTerm).readBlob(OperationPurpose.INDICES, "commit-1")
            ) {
                assertArrayEquals(BytesReference.toBytes(blobData), is.readAllBytes());
            }
        } finally {
            internalCluster().stopNode(node);
        }
    }

    @SuppressForbidden(reason = "this test uses a HttpServer to emulate a GCS endpoint")
    private static class InterceptableGcsHttpHandler extends GoogleCloudStorageHttpHandler {
        volatile Interceptor interceptor;

        InterceptableGcsHttpHandler(String bucket) {
            super(bucket);
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (interceptor == null || interceptor.intercept(exchange) == false) {
                super.handle(exchange);
            }
        }
    }

    @SuppressForbidden(reason = "this test uses a HttpServer to emulate a GCS endpoint")
    private interface Interceptor {
        /**
         * Returns true if it has answered the request, and false if the request is for the fake service to answer.
         */
        boolean intercept(HttpExchange exchange) throws IOException;
    }

    public static class TestGoogleCloudStoragePlugin extends GoogleCloudStoragePlugin {
        final AtomicReference<BooleanSupplier> shouldInjectErrorRef = new AtomicReference<>();

        public TestGoogleCloudStoragePlugin(Settings settings) {
            super(settings);
        }

        @Override
        protected GoogleCloudStorageService createStorageService(ClusterService clusterService, ProjectResolver projectResolver) {
            return new TestGoogleCloudStorageService(clusterService, projectResolver, shouldInjectErrorRef);
        }
    }

    public static class TestGoogleCloudStorageService extends GoogleCloudStorageService {
        private final AtomicReference<BooleanSupplier> shouldInjectErrorRef;

        public TestGoogleCloudStorageService(
            ClusterService clusterService,
            ProjectResolver projectResolver,
            AtomicReference<BooleanSupplier> shouldInjectErrorRef
        ) {
            super(clusterService, projectResolver);
            this.shouldInjectErrorRef = shouldInjectErrorRef;
        }

        @Override
        public MeteredStorage client(
            ProjectId projectId,
            String clientName,
            String repositoryName,
            GcsRepositoryStatsCollector statsCollector
        ) throws IOException {
            final MeteredStorage original = super.client(projectId, clientName, repositoryName, statsCollector);

            final var shouldInjectError = shouldInjectErrorRef.get();
            if (shouldInjectError != null && shouldInjectError.getAsBoolean()) {
                final MeteredStorage spied = Mockito.spy(original);
                doAnswer(invocation -> {
                    final OperationPurpose purpose = invocation.getArgument(0);
                    if (purpose == OperationPurpose.INDICES) {
                        throw new UnknownHostException("simulated");
                    }
                    return invocation.callRealMethod();
                }).when(spied).meteredList(any(OperationPurpose.class), any(String.class), any(Storage.BlobListOption[].class));
                return spied;
            } else {
                return original;
            }
        }
    }
}
