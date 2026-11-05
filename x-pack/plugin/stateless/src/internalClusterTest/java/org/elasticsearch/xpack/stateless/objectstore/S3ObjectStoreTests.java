/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.objectstore;

import fixture.s3.BlobEntry;
import fixture.s3.S3ConsistencyModel;
import fixture.s3.S3HttpHandler;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.blobcache.shared.SharedBytes;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.Predicates;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo;
import org.elasticsearch.index.snapshots.blobstore.SnapshotFiles;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.RepositoryStats;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.repositories.blobstore.ESBlobStoreRepositoryIntegTestCase;
import org.elasticsearch.repositories.blobstore.ESMockAPIBasedRepositoryIntegTestCase;
import org.elasticsearch.repositories.s3.S3RepositoryPlugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.xpack.stateless.TestUtils;
import org.elasticsearch.xpack.stateless.action.NewCommitNotificationRequest;
import org.elasticsearch.xpack.stateless.action.TransportNewCommitNotificationAction;
import org.elasticsearch.xpack.stateless.commits.BatchedCompoundCommit;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitService;
import org.elasticsearch.xpack.stateless.snapshots.StatelessSnapshotSettings;
import org.elasticsearch.xpack.stateless.snapshots.StatelessSnapshotSettings.StatelessSnapshotEnabledStatus;
import org.junit.Before;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.rest.RestStatus.REQUEST_TIMEOUT;
import static org.elasticsearch.rest.RestStatus.SERVICE_UNAVAILABLE;
import static org.elasticsearch.rest.RestStatus.TOO_MANY_REQUESTS;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.xpack.stateless.cache.SearchCommitPrefetcherDynamicSettings.PREFETCH_COMMITS_UPON_NOTIFICATIONS_ENABLED_SETTING;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

public class S3ObjectStoreTests extends AbstractMockObjectStoreIntegTestCase {

    private static final Set<String> EXPECTED_MAIN_STORE_REQUEST_NAMES;
    private static final Set<String> EXPECTED_OBS_REQUEST_NAMES;
    private static final ByteSizeValue MULTIPART_UPLOAD_BUFFER_SIZE = ByteSizeValue.ofMb(5);
    private static final String COPY_ERROR_MESSAGE = "We encountered an internal error. Please try again.";

    /**
     * Where an error response puts its XML declaration, the {@code <?xml ... ?>} line, compared with the keep-alive whitespace.
     */
    private enum Declaration {
        /** No declaration. */
        NONE,
        /** The declaration first and the whitespace after it. This is the order that S3 sends during a long copy. */
        BEFORE_WHITESPACE,
        /** The whitespace first. A declaration that is not at the start makes the response invalid XML. */
        AFTER_WHITESPACE
    }

    static {
        final var mainStorePurposeNames = Set.of("ClusterState", "Indices", "Translog");
        final var obsPurposeNames = Set.of("SnapshotData", "SnapshotMetadata", "RepositoryAnalysis", "Resharding");
        final var operationNames = Set.of(
            "HeadObject",
            "GetObject",
            "ListObjects",
            "PutObject",
            "PutMultipartObject",
            "DeleteObjects",
            "AbortMultipartObject"
        );

        EXPECTED_MAIN_STORE_REQUEST_NAMES = Stream.concat(mainStorePurposeNames.stream(), obsPurposeNames.stream())
            .flatMap(p -> operationNames.stream().map(o -> p + "_" + o))
            .collect(Collectors.toUnmodifiableSet());

        EXPECTED_OBS_REQUEST_NAMES = obsPurposeNames.stream()
            .flatMap(p -> operationNames.stream().map(o -> p + "_" + o))
            .collect(Collectors.toUnmodifiableSet());
    }

    @SuppressForbidden(reason = "this test uses a HttpServer to emulate an S3 endpoint")
    protected InterceptableS3HttpHandler s3HttpHandler;
    protected int maxRetries;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(TestUtils.StatelessPluginWithTrialLicense.class);
        plugins.add(TestStatelessPlugin.class);
        plugins.add(S3RepositoryPlugin.class);
        return plugins;
    }

    @Override
    protected Map<String, HttpHandler> createHttpHandlers() {
        s3HttpHandler = new InterceptableS3HttpHandler("bucket");
        return Map.of("/bucket", s3HttpHandler);
    }

    @Before
    public void setupMaxRetries() {
        // max_retries set rarely to more than 1, so that we can test more retries within reasonable backoff
        maxRetries = rarely() ? randomIntBetween(2, 3) : 1;
    }

    @Override
    protected Settings.Builder nodeSettings() {
        MockSecureSettings mockSecureSettings = new MockSecureSettings();
        mockSecureSettings.setString("s3.client.test.access_key", "test_access_key");
        mockSecureSettings.setString("s3.client.test.secret_key", "test_secret_key");
        return super.nodeSettings().put("s3.client.test.endpoint", httpServerUrl())
            .put("s3.client.backup.endpoint", httpServerUrl())
            .put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.S3)
            .put(ObjectStoreService.BUCKET_SETTING.getKey(), "bucket")
            .put(ObjectStoreService.CLIENT_SETTING.getKey(), "test")
            .put("s3.client.test.max_retries", maxRetries)
            .setSecureSettings(mockSecureSettings);
    }

    @Override
    protected String repositoryType() {
        return "s3";
    }

    @Override
    protected Settings repositorySettings() {
        return Settings.builder()
            .put(super.repositorySettings())
            .put("bucket", "bucket")
            .put("base_path", "backup")
            .put("client", "test")
            .put("add_purpose_custom_query_parameter", "true")
            .build();
    }

    @Override
    protected void assertRepositoryStats(RepositoryStats repositoryStats, boolean withRandomCrud, OperationPurpose purpose) {
        assertTrue(EXPECTED_MAIN_STORE_REQUEST_NAMES.containsAll(repositoryStats.actionStats.keySet()));
        repositoryStats.actionStats.forEach((metricName, count) -> {
            if (metricName.endsWith("_AbortMultipartObject") || metricName.endsWith("_HeadObject")) {
                assertThat(metricName, count.operations(), greaterThanOrEqualTo(0L));
            } else {
                assertThat(metricName, count.operations(), greaterThan(0L));
            }
        });
    }

    @Override
    protected void assertObsRepositoryStatsSnapshots(RepositoryStats repositoryStats) {
        assertTrue(EXPECTED_OBS_REQUEST_NAMES.containsAll(repositoryStats.actionStats.keySet()));
        repositoryStats.actionStats.forEach((metricName, count) -> {
            if (metricName.endsWith("_AbortMultipartObject") || metricName.endsWith("_PutMultipartObject")) {
                assertThat(count.operations(), greaterThanOrEqualTo(0L));
            } else {
                assertThat(count.operations(), greaterThan(0L));
            }
        });
    }

    public void testShouldRetryMoreThanMaxRetriesForIndicesData() throws IOException {
        // More than the max retries by s3 client itself (1 original + maxRetries)
        final int errorsPerRequest = randomIntBetween(maxRetries + 1, maxRetries + 4);
        s3HttpHandler.setInterceptor(new Interceptor() {
            private final Map<String, AtomicInteger> requestPathErrorCount = ConcurrentCollections.newConcurrentMap();

            @SuppressForbidden(reason = "this test uses a HttpServer to emulate an S3 endpoint")
            @Override
            public boolean intercept(HttpExchange exchange) throws IOException {
                final String request = exchange.getRequestMethod() + " " + exchange.getRequestURI().toString();
                if (Regex.simpleMatch("GET /*/stateless_commit_*", request)) {
                    final AtomicInteger numberOfErrors = requestPathErrorCount.computeIfAbsent(request, k -> new AtomicInteger(0));
                    if (numberOfErrors.getAndIncrement() >= errorsPerRequest) {
                        return false;
                    }
                    try (exchange) {
                        ESMockAPIBasedRepositoryIntegTestCase.drainInputStream(exchange.getRequestBody());
                        exchange.sendResponseHeaders(
                            randomFrom(
                                RestStatus.INTERNAL_SERVER_ERROR,
                                RestStatus.TOO_MANY_REQUESTS,
                                RestStatus.BAD_GATEWAY,
                                RestStatus.GATEWAY_TIMEOUT
                            ).getStatus(),
                            -1
                        );
                        return true;
                    }
                }
                return false;
            }
        });

        final Settings nodeSettings = Settings.builder()
            .put(disableIndexingDiskAndMemoryControllersNodeSettings())
            .put(PREFETCH_COMMITS_UPON_NOTIFICATIONS_ENABLED_SETTING.getKey(), false)
            .build();
        final String masterAndIndexNode = startMasterAndIndexNode(nodeSettings);
        final String searchNode = startSearchNode(nodeSettings);

        final String loggerName = "org.elasticsearch.common.blobstore.RetryingInputStream";

        enum Status {
            STARTED, // On initial and subsequent failure messages before seeing the success message
            STOPPED, // On success message if failure messages have been observed before
            ERROR // Anything else
        }

        record CallSite(String threadName, String commitFile) {}

        final AtomicBoolean completed = new AtomicBoolean(false);
        try (var mockLogAppender = MockLog.capture(loggerName)) {
            // On each call site that experiences an initial failure, we expect to see a pairing of
            // failure messages (can be multiple) and the eventual success message.
            // We use a LoggingExpectation to perform bookkeeping for observed log messages and
            // ensures (1) we must see some related log messages and (2) the messages must make the pairing
            // (in the order of failure then success) for each call site.
            mockLogAppender.addExpectation(new MockLog.LoggingExpectation() {
                private final Pattern failurePattern = Pattern.compile(
                    "failed opening .*/(stateless_commit_[0-9]+).* at offset .* with purpose \\[Indices]; this was attempt .*"
                );
                private final Pattern eventualSuccessPattern = Pattern.compile(
                    "successfully open(?:ed)? input stream for .*/(stateless_commit_[0-9]+).* with purpose \\[Indices] after .* retries"
                );
                private final Map<CallSite, Status> bookkeeping = ConcurrentCollections.newConcurrentMap();

                @Override
                public void match(LogEvent event) {
                    if (event.getLevel().equals(Level.INFO) && event.getLoggerName().equals(loggerName)) {
                        if (maybeHandleAsFailureMessage(event)) {
                            return;
                        }
                        maybeHandleAsSuccessMessage(event);
                    }
                }

                @Override
                public void assertMatched() {
                    assertThat("Did not see any relevant logs", bookkeeping, not(anEmptyMap()));
                    assertThat("Seen log events: " + bookkeeping, bookkeeping.values(), everyItem(is(Status.STOPPED)));
                }

                private boolean maybeHandleAsFailureMessage(LogEvent event) {
                    final CallSite callSite = extractCallSite(event, failurePattern);
                    if (callSite == null) {
                        return false;
                    }
                    bookkeeping.compute(callSite, (k, v) -> {
                        if (v == null || v == Status.STARTED) {
                            return Status.STARTED;
                        } else {
                            return Status.ERROR;
                        }
                    });
                    return true;
                }

                private boolean maybeHandleAsSuccessMessage(LogEvent event) {
                    final CallSite callSite = extractCallSite(event, eventualSuccessPattern);
                    if (callSite == null) {
                        return false;
                    }
                    bookkeeping.compute(callSite, (k, v) -> {
                        if (v == Status.STARTED) {
                            return Status.STOPPED;
                        } else {
                            return Status.ERROR;
                        }
                    });
                    return true;
                }

                private CallSite extractCallSite(LogEvent event, Pattern pattern) {
                    final String formattedMessage = event.getMessage().getFormattedMessage();
                    final Matcher matcher = pattern.matcher(formattedMessage);
                    if (matcher.matches()) {
                        // The thread name including the node name and thread ID, e.g. elasticsearch[node_t0][refresh][T#2],
                        // which can be used to identify each call site and ensure the log messages match in pair,
                        // i.e. initial failure then eventually success
                        return new CallSite(event.getThreadName(), matcher.group(1));
                    } else {
                        return null;
                    }
                }
            });

            final String indexName = randomIdentifier();
            createIndex(indexName, indexSettings(1, 1).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1).build());
            ensureGreen(indexName);

            final CyclicBarrier uploadedCommitNotificationBarrier = new CyclicBarrier(2);
            MockTransportService.getInstance(searchNode)
                .addRequestHandlingBehavior(TransportNewCommitNotificationAction.NAME + "[u]", (handler, request, channel, task) -> {
                    if (((NewCommitNotificationRequest) request).isUploaded()) {
                        handler.messageReceived(request, new TransportChannel() {
                            @Override
                            public String getProfileName() {
                                return channel.getProfileName();
                            }

                            @Override
                            public void sendResponse(TransportResponse response) {
                                if (completed.get() == false) {
                                    safeAwait(uploadedCommitNotificationBarrier);
                                }
                                channel.sendResponse(response);
                            }

                            @Override
                            public void sendResponse(Exception exception) {
                                if (completed.get() == false) {
                                    fail(exception, "new uploaded commit notification should not fail");
                                }
                                // Exceptions can happen when the test ends which closes the index engine. They are innocuous
                                channel.sendResponse(exception);
                            }
                        }, task);
                    } else {
                        // Drop the notification for non-uploaded commits to force the search node reading from the object store
                        channel.sendResponse(new RuntimeException("dropping notification for non-uploaded commit"));
                    }
                });
            int totalNumDocs = 0;
            final int iterations = between(3, 5);
            for (int i = 0; i < iterations; i++) {
                final int numDocs = between(1, 10);
                totalNumDocs += numDocs;
                indexDocs(indexName, numDocs);
                indicesAdmin().prepareFlush(indexName).get(TimeValue.timeValueSeconds(10));
                safeAwait(uploadedCommitNotificationBarrier); // wait till uploaded commit notification is processed
                assertHitCount(client().prepareSearch(indexName), totalNumDocs); // ensure search works
            }
            logger.info("--> all search hit counts matched");
            mockLogAppender.assertAllExpectationsMatched();
            logger.info("--> logging expectation matched");
        } finally {
            completed.set(true);
            // Stop the node otherwise the test can fail because node tries to publish cluster state to a closed HTTP handler
            internalCluster().stopNode(searchNode);
            internalCluster().stopNode(masterAndIndexNode);
        }
    }

    public void testShouldNotRetryForNoSuchFileException() throws Exception {
        final Settings nodeSettings = disableIndexingDiskAndMemoryControllersNodeSettings();
        final String masterAndIndexNode = startMasterAndIndexNode(nodeSettings);
        final String searchNode = startSearchNode(nodeSettings);

        final String loggerName = "org.elasticsearch.common.blobstore.RetryingInputStream";

        try (var mockLogAppender = MockLog.capture(loggerName)) {
            mockLogAppender.addExpectation(
                new MockLog.UnseenEventExpectation("initial failure", loggerName, Level.INFO, "failed opening */stateless_commit_*")
            );
            mockLogAppender.addExpectation(
                new MockLog.UnseenEventExpectation(
                    "final success",
                    loggerName,
                    Level.INFO,
                    "successfully opened input stream for */stateless_commit_* after * retries"
                )
            );

            final String indexName = randomIdentifier();
            createIndex(indexName, indexSettings(1, 1).build());
            ensureGreen(indexName);

            s3HttpHandler.setInterceptor(new Interceptor() {
                private final Set<String> erroredRequestPaths = ConcurrentCollections.newConcurrentSet();

                @SuppressForbidden(reason = "this test uses a HttpServer to emulate an S3 endpoint")
                @Override
                public boolean intercept(HttpExchange exchange) throws IOException {
                    final String request = exchange.getRequestMethod() + " " + exchange.getRequestURI().toString();
                    if (Regex.simpleMatch("GET /*/stateless_commit_*", request) && false == erroredRequestPaths.contains(request)) {
                        try (exchange) {
                            ESMockAPIBasedRepositoryIntegTestCase.drainInputStream(exchange.getRequestBody());
                            exchange.sendResponseHeaders(RestStatus.NOT_FOUND.getStatus(), -1);
                        }
                        return true;
                    }
                    return false;
                }
            });

            indexDocs(indexName, between(1, 10));
            final CountDownLatch newUploadedCommitNotificationFailureLatch = new CountDownLatch(1);
            MockTransportService.getInstance(searchNode)
                .addRequestHandlingBehavior(TransportNewCommitNotificationAction.NAME + "[u]", (handler, request, channel, task) -> {
                    if (((NewCommitNotificationRequest) request).isUploaded()) {
                        handler.messageReceived(request, new TransportChannel() {
                            @Override
                            public String getProfileName() {
                                return channel.getProfileName();
                            }

                            @Override
                            public void sendResponse(TransportResponse response) {
                                fail("new uploaded commit notification should have failed");
                            }

                            @Override
                            public void sendResponse(Exception exception) {
                                assertThat(ExceptionsHelper.unwrap(exception, NoSuchFileException.class), notNullValue());
                                newUploadedCommitNotificationFailureLatch.countDown();
                                channel.sendResponse(exception);
                            }
                        }, task);
                    } else {
                        // Drop the notification for non-uploaded commits to force the search node reading from the object store
                        channel.sendResponse(new RuntimeException("dropping notification for non-uploaded commit"));
                    }
                });
            indicesAdmin().prepareFlush(indexName).get(TimeValue.timeValueSeconds(10));
            // Search shard should fail because it encounters 404 while reading from the object store
            safeAwait(newUploadedCommitNotificationFailureLatch);
            mockLogAppender.assertAllExpectationsMatched();

        } finally {
            // Stop the node otherwise the test can fail because node tries to publish cluster state to a closed HTTP handler
            internalCluster().stopNode(searchNode);
            internalCluster().stopNode(masterAndIndexNode);
        }
    }

    public void testUploadIndicesDataWithRetries() throws Exception {
        s3HttpHandler.setInterceptor(new Interceptor() {
            private final int errorsPerRequest = maxRetries + randomIntBetween(2, 3);
            private final Map<String, AtomicInteger> requestPathErrorCount = ConcurrentCollections.newConcurrentMap();

            @SuppressForbidden(reason = "this test uses a HttpServer to emulate an S3 endpoint")
            @Override
            public boolean intercept(HttpExchange exchange) throws IOException {
                final String request = exchange.getRequestMethod() + " " + exchange.getRequestURI().toString();
                if (request.contains("PUT") && request.contains("indices/")) {
                    // request format: /bucket/base_path/indices/UUID/0/1/stateless_commit_N?x-purpose=Indices for single part uploads,
                    // while for multi-part uploads: .../stateless_commit_N?uploadId=UUID&partNumber=N&x-purpose=Indices where the
                    // upload UUID changes with every retry.
                    long gen = Long.parseLong(
                        request.substring(
                            request.indexOf(BatchedCompoundCommit.PREFIX) + BatchedCompoundCommit.PREFIX.length(),
                            request.lastIndexOf('?')
                        )
                    );
                    final long part;
                    if (request.contains("partNumber")) {
                        final var partNumberIndex = request.indexOf("partNumber");
                        part = Long.parseLong(
                            request.substring(partNumberIndex + "partNumber=".length(), request.indexOf('&', partNumberIndex))
                        );
                    } else {
                        part = 1;
                    }
                    String genAndPart = gen + "/" + part;
                    final AtomicInteger numberOfErrors = requestPathErrorCount.computeIfAbsent(genAndPart, k -> new AtomicInteger(0));
                    if (numberOfErrors.getAndIncrement() >= errorsPerRequest) {
                        return false;
                    }
                    try (exchange) {
                        ESMockAPIBasedRepositoryIntegTestCase.drainInputStream(exchange.getRequestBody());
                        exchange.sendResponseHeaders(
                            randomFrom(
                                RestStatus.INTERNAL_SERVER_ERROR,
                                RestStatus.TOO_MANY_REQUESTS,
                                RestStatus.BAD_GATEWAY,
                                RestStatus.GATEWAY_TIMEOUT
                            ).getStatus(),
                            -1
                        );
                        return true;
                    }
                }
                return false;
            }
        });

        // Use random but sufficiently large cache settings. The default random cache configuration can produce a cache smaller than the
        // region size, resulting in zero usable regions and every read going through the mock HTTP handler which is too slow to complete
        // within the ensureGreen timeout.
        var regionPages = randomIntBetween(16, 64); // 64kb to 256kb regions
        var cachePages = regionPages * randomIntBetween(64, 256); // 64 to 256 regions, giving at least 4mb of cache
        final Settings nodeSettings = Settings.builder()
            .put(disableIndexingDiskAndMemoryControllersNodeSettings())
            .put(
                SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(),
                ByteSizeValue.ofBytes((long) regionPages * SharedBytes.PAGE_SIZE)
            )
            .put(
                SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(),
                ByteSizeValue.ofBytes((long) cachePages * SharedBytes.PAGE_SIZE)
            )
            .build();
        final String masterAndIndexNode = startMasterAndIndexNode(nodeSettings);
        final String searchNode = startSearchNode(nodeSettings);

        try {
            final String indexName = randomIdentifier();
            createIndex(indexName, indexSettings(1, 0).build());
            ensureGreen(indexName);

            // Index enough 1 MiB documents to produce a >5MiB Lucene compound segment file, to ensure S3 multi-part upload
            int numDocs = randomIntBetween(8, 10);
            for (int i = 0; i < numDocs; i++) {
                indexDoc(indexName, "doc_" + i, "field", randomByteArrayOfLength((int) ByteSizeValue.ofMb(1).getBytes()));
            }

            flush(indexName);

            setReplicaCount(1, indexName);
            ensureGreen(indexName);
            // we are setting size to 0 because we don't want to retrieve the documents (which are heavy in this test case) as it can cause
            // suite timeouts when the cache is very small
            assertHitCount(prepareSearch(indexName).setSize(0), numDocs);
        } finally {
            // Stop the node otherwise the test can fail because node tries to publish cluster state to a closed HTTP handler
            internalCluster().stopNode(searchNode);
            internalCluster().stopNode(masterAndIndexNode);
        }
    }

    @Override
    protected boolean canRefuseCopies() {
        return true;
    }

    @SuppressForbidden(reason = "this test uses a HttpServer to emulate an S3 endpoint")
    @Override
    protected void refuseCopiesAfterTheFirst(AtomicInteger copyRequests) {
        s3HttpHandler.setInterceptor(exchange -> {
            // a copy is a request with an X-amz-copy-source header
            if (exchange.getRequestHeaders().containsKey(S3HttpHandler.COPY_SOURCE_HEADER) && copyRequests.incrementAndGet() > 1) {
                try (exchange) {
                    final byte[] response = Strings.format("""
                        <?xml version="1.0" encoding="UTF-8"?>
                        <Error>
                          <Code>AccessDenied</Code>
                          <Message>Access Denied</Message>
                          <RequestId>%s</RequestId>
                        </Error>""", randomUUID()).getBytes(StandardCharsets.UTF_8);
                    exchange.getResponseHeaders().add("Content-Type", "application/xml");
                    exchange.sendResponseHeaders(403, response.length);
                    exchange.getResponseBody().write(response);
                }
                return true;
            }
            return false;
        });
    }

    @Override
    protected Set<String> blobPathsInFakeStorage() {
        return Set.copyOf(s3HttpHandler.blobs().keySet());
    }

    @Override
    protected String accessDeniedText() {
        return "Access Denied";
    }

    /**
     * A copy request that S3 answers with HTTP status 200 and an error document in the body fails the shard snapshot. The S3 client reads
     * the body and finds the error. The error code is InternalError, so the client retries the copy. Every retry gets the same error, so
     * the client gives up and throws, and {@code S3BlobContainer.copyBlob} fails.
     * <p>
     * Before a shard snapshot writes its shard snapshot blob, {@code snap-<uuid>.dat}, it checks the copies that it made. Each copy is a
     * copied blob in the repository, named {@code c__<uuid>}, and it is a copy of a commit blob from the object store. A commit blob
     * holds the files of a commit. The check is that each copied blob exists and has the length of its commit blob. A failed check has
     * "but expected" in its message. The check runs only after every copy call has returned normally. Here a copy call throws, so the
     * shard snapshot never reaches the check.
     */
    public void testCopyThatAnswers200WithAnErrorBodyFailsTheShardSnapshot() throws Exception {
        final String reason = snapshotWhileCopiesAnswer200WithAnError("", Declaration.BEFORE_WHITESPACE);
        // copyBlob fails with "Unable to copy object", and wraps the exception that the S3 client threw
        assertThat(reason, containsString("Unable to copy object"));
        // the exception of the S3 client has the message of the error document
        assertThat(reason, containsString(COPY_ERROR_MESSAGE));
        // a reason without "but expected" shows that the shard snapshot failed in copyBlob, and not in the check of the copies
        assertThat(reason, not(containsString("but expected")));
    }

    /**
     * Same as the test above, but the response also holds whitespace, which S3 sends during a long copy so that the connection does not
     * time out. The test picks at random where the XML declaration, the {@code <?xml ... ?>} line, goes. S3 sends the declaration first
     * and the whitespace after it. Then the S3 client skips the whitespace, reads the error document and throws the error, as in the test
     * above. It does the same when there is no declaration. With the whitespace before the declaration, the body is not valid XML,
     * because a declaration has to be the first thing in a document. Then the client cannot parse the body and throws. In every case
     * {@code S3BlobContainer.copyBlob} fails.
     */
    public void testCopyThatAnswers200WithKeepAliveWhitespaceThenAnErrorBodyFailsTheShardSnapshot() throws Exception {
        final Declaration declaration = randomFrom(Declaration.values());
        final String reason = snapshotWhileCopiesAnswer200WithAnError(randomKeepAliveWhitespace(), declaration);
        assertThat(reason, containsString("Unable to copy object"));
        if (declaration == Declaration.AFTER_WHITESPACE) {
            assertThat(reason, containsString("Could not parse XML response"));
        } else {
            assertThat(reason, containsString(COPY_ERROR_MESSAGE));
        }
        assertThat(reason, not(containsString("but expected")));
    }

    /**
     * The S3 client retries a copy request when the error document in the response has the code InternalError. Here each copy request
     * fails once and then works, so the shard snapshot succeeds. The failed request leaves no blob, so the retry makes the whole copy.
     * Each copied file names a copied blob and a byte range inside that blob. After the retries, every copied file lies inside the blob
     * that the file names.
     */
    public void testCopyThatAnswers200WithAnErrorBodyOnceIsRetried() throws Exception {
        final String indexName = startNodeAndIndexWithThreeCommitBlobs();
        final var copyRequests = new AtomicInteger();
        final Set<String> failedFirstAttempts = ConcurrentCollections.newConcurrentSet();
        answerCopiesWith200AndAnError(targetPath -> {
            copyRequests.incrementAndGet();
            // true for the first request of a copy, so the retry of the copy goes to the fake service
            return failedFirstAttempts.add(targetPath);
        }, "", Declaration.BEFORE_WHITESPACE);

        createSnapshot("backup", "snapshot", List.of(indexName), List.of());

        // each copy was requested twice: the request that failed, and the retry
        assertThat(failedFirstAttempts, not(empty()));
        assertThat(copyRequests.get(), equalTo(2 * failedFirstAttempts.size()));
        assertCopiedFilesFitTheirBlobs("backup", greaterThan(0));
    }

    /**
     * Between one and 64 whitespace characters, spaces and newlines in a random order.
     */
    private static String randomKeepAliveWhitespace() {
        final var whitespace = new StringBuilder();
        for (int i = between(1, 64); i > 0; i--) {
            whitespace.append(randomFrom(" ", "\n"));
        }
        return whitespace.toString();
    }

    /**
     * Starts a node, and creates an index with three commits and a repository. Each commit is in a commit blob of its own, which is a blob
     * in the object store that holds the files of a commit. The snapshots of the index copy the commit blobs into the repository.
     *
     * @return the name of the index
     */
    private String startNodeAndIndexWithThreeCommitBlobs() throws Exception {
        final Settings settings = Settings.builder()
            // copies need shard snapshots that read from the object store
            .put(
                StatelessSnapshotSettings.STATELESS_SNAPSHOT_ENABLED_SETTING.getKey(),
                randomFrom(StatelessSnapshotEnabledStatus.READ_FROM_OBJECT_STORE, StatelessSnapshotEnabledStatus.ENABLED)
            )
            .put(StatelessSnapshotSettings.SERVER_SIDE_COPY_ENABLED_SETTING.getKey(), true)
            // one commit in each commit blob, so that each flush adds a commit blob to copy
            .put(StatelessCommitService.STATELESS_UPLOAD_MAX_AMOUNT_COMMITS.getKey(), 1)
            .build();
        startMasterAndIndexNode(settings);
        final String indexName = randomIndexName();
        createIndex(indexName, 1, 0);
        ensureGreen(indexName);
        for (int commit = 0; commit < 3; commit++) {
            indexDocs(indexName, between(10, 50));
            flush(indexName);
        }
        createRepository("backup", repositorySettings());
        return indexName;
    }

    /**
     * Makes the fake S3 service answer some copy requests the way S3 answers a copy that fails after the copy has started. The service
     * does this for the requests that {@code failsCopy} accepts, and it makes no copy for them. {@code failsCopy} gets the path of the
     * blob that the request would make. By then S3 has already sent the status 200, so S3 reports the failure as an error document in the
     * body of the response. All other requests go to the fake service as usual.
     * <p>
     * With a {@code keepAlive} that is not empty, the response sends that whitespace before the error document, as S3 does during a long
     * copy so that the connection does not time out. The start of the response is flushed on its own, so it reaches the client first.
     * {@code declaration} says where the XML declaration, the {@code <?xml ... ?>} line, goes.
     */
    @SuppressForbidden(reason = "this test uses a HttpServer to emulate an S3 endpoint")
    private void answerCopiesWith200AndAnError(Predicate<String> failsCopy, String keepAlive, Declaration declaration) {
        s3HttpHandler.setInterceptor(exchange -> {
            // a copy is a request with an X-amz-copy-source header
            if (exchange.getRequestHeaders().containsKey(S3HttpHandler.COPY_SOURCE_HEADER)
                && failsCopy.test(exchange.getRequestURI().getPath())) {
                try (exchange) {
                    final String xmlDeclaration = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n";
                    final String errorDocument = Strings.format("""
                        <Error>
                          <Code>InternalError</Code>
                          <Message>%s</Message>
                          <RequestId>%s</RequestId>
                        </Error>""", COPY_ERROR_MESSAGE, randomUUID());
                    // Without whitespace there is nothing to send early, so the declaration, if any, goes with the error document.
                    final boolean declarationFirst = declaration == Declaration.BEFORE_WHITESPACE && keepAlive.isEmpty() == false;
                    final String start = (declarationFirst ? xmlDeclaration : "") + keepAlive;
                    final String rest = (declaration != Declaration.NONE && declarationFirst == false ? xmlDeclaration : "")
                        + errorDocument;
                    exchange.getResponseHeaders().add("Content-Type", "application/xml");
                    final byte[] restBytes = rest.getBytes(StandardCharsets.UTF_8);
                    if (start.isEmpty()) {
                        exchange.sendResponseHeaders(RestStatus.OK.getStatus(), restBytes.length);
                    } else {
                        // a length of 0 makes the response chunked, so the length does not have to be known when the response starts
                        exchange.sendResponseHeaders(RestStatus.OK.getStatus(), 0);
                        exchange.getResponseBody().write(start.getBytes(StandardCharsets.UTF_8));
                        // send the start now, so that it reaches the client before the error document does
                        exchange.getResponseBody().flush();
                    }
                    exchange.getResponseBody().write(restBytes);
                }
                return true;
            }
            return false;
        });
    }

    /**
     * Snapshots an index with three commit blobs while the fake S3 service answers every copy request but the first with HTTP status 200
     * and an error document, and checks what the repository holds afterwards. The shard snapshot must fail. It must leave no copied blob
     * and write no shard snapshot blob, and no copied file in the repository may name a blob that is missing or too short. The first copy
     * works, so that the failure cleanup has a copy to delete.
     *
     * @return the reason that the shard snapshot failed
     */
    private String snapshotWhileCopiesAnswer200WithAnError(String keepAlive, Declaration declaration) throws Exception {
        final String indexName = startNodeAndIndexWithThreeCommitBlobs();
        final var copyRequests = new AtomicInteger();
        answerCopiesWith200AndAnError(targetPath -> copyRequests.incrementAndGet() > 1, keepAlive, declaration);

        // The failed shard snapshot logs how many copies it made, before its cleanup deletes the copied blobs. It made exactly one copy,
        // the first copy, which the fake service lets work.
        final var response = new AtomicReference<CreateSnapshotResponse>();
        MockLog.assertThatLogger(
            () -> response.set(
                clusterAdmin().prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, "backup", "snapshot")
                    .setIndices(indexName)
                    .setPartial(true)
                    .setWaitForCompletion(true)
                    .get()
            ),
            BlobStoreRepository.class,
            new MockLog.SeenEventExpectation(
                "the failed attempt made the allowed copy",
                BlobStoreRepository.class.getCanonicalName(),
                Level.INFO,
                "*shard snapshot copy stats: attempt [failed], copies made [1], copied bytes [*], total copy time [*ms]"
            )
        );
        final SnapshotInfo snapshotInfo = response.get().getSnapshotInfo();
        assertThat(snapshotInfo.state(), equalTo(SnapshotState.PARTIAL));
        assertThat(snapshotInfo.shardFailures(), hasSize(1));
        // the first copy request worked, and at least one more copy request was answered with an error
        assertThat(copyRequests.get(), greaterThanOrEqualTo(2));
        // the failure cleanup deleted the copy that worked, and the shard snapshot blob was never written
        assertThat(copiedBlobPathsInFakeStorage(), empty());
        assertThat(
            blobPathsInFakeStorage().stream()
                .filter(path -> path.contains("/indices/") && path.contains("/" + BlobStoreRepository.SNAPSHOT_PREFIX))
                .toList(),
            empty()
        );
        // the only shard snapshot failed, so no shard snapshot finished and the repository has no copied file that names a missing blob
        assertCopiedFilesFitTheirBlobs("backup", equalTo(0));
        return snapshotInfo.shardFailures().get(0).reason();
    }

    /**
     * The paths of the copied blobs in the fake S3 service.
     */
    private Set<String> copiedBlobPathsInFakeStorage() {
        return blobPathsInFakeStorage().stream()
            .filter(path -> path.contains("/" + BlobStoreRepository.COPIED_DATA_BLOB_PREFIX))
            .collect(Collectors.toUnmodifiableSet());
    }

    /**
     * Checks the copied files of the shard snapshots in the repository against the blobs that the fake S3 service holds, which are what a
     * restore reads. A shard snapshot lists a copied file with the copied blob that holds it and the byte range of the file inside that
     * blob. That blob must exist and be long enough to hold the range. Every copied blob in the repository must also be named by at least
     * one copied file. The files come from the blob of each shard snapshot and from the shard generation that the next snapshot of the
     * shard starts from.
     *
     * @param copiedFileCount a matcher for how many copied files were checked, so that the assertion fails when no file was checked but
     *                        files are expected
     */
    private void assertCopiedFilesFitTheirBlobs(String repoName, org.hamcrest.Matcher<Integer> copiedFileCount) throws IOException {
        final var repository = (BlobStoreRepository) internalCluster().getCurrentMasterNodeInstance(RepositoriesService.class)
            .repository(repoName);
        final RepositoryData repositoryData = ESBlobStoreRepositoryIntegTestCase.getRepositoryData(repository);
        final Set<String> pointedAtBlobPaths = new HashSet<>();
        int checkedFiles = 0;
        for (IndexId indexId : repositoryData.getIndices().values()) {
            // a shard without a shard generation has no shard snapshot that finished successfully, so the shard has no copied file
            final List<ShardGeneration> shardGenerations = repositoryData.shardGenerations().getGens(indexId);
            for (int shard = 0; shard < shardGenerations.size(); shard++) {
                final BlobContainer shardContainer = repository.shardContainer(indexId, shard);
                final List<FileInfo> files = new ArrayList<>();
                for (SnapshotFiles snapshot : repository.getBlobStoreIndexShardSnapshots(indexId, shard, shardGenerations.get(shard))
                    .snapshots()) {
                    files.addAll(snapshot.indexFiles());
                }
                for (SnapshotId snapshotId : repositoryData.getSnapshots(indexId)) {
                    files.addAll(repository.loadShardSnapshot(shardContainer, snapshotId).indexFiles());
                }
                for (FileInfo file : files) {
                    if (file.isCopied()) {
                        // the fake S3 service keys a blob by its path, which starts with the name of the bucket
                        final String blobPath = "/bucket/" + shardContainer.path().buildAsString() + file.blob();
                        pointedAtBlobPaths.add(blobPath);
                        final BlobEntry blob = s3HttpHandler.blobs().get(blobPath);
                        assertNotNull("blob [" + blobPath + "] of the copied file " + file + " is missing", blob);
                        assertThat(
                            "blob [" + blobPath + "] is too short for the copied file " + file,
                            (long) blob.contents().length(),
                            greaterThanOrEqualTo(file.offset() + file.length())
                        );
                        checkedFiles++;
                    }
                }
            }
        }
        assertThat(copiedBlobPathsInFakeStorage(), equalTo(pointedAtBlobPaths));
        assertThat(checkedFiles, copiedFileCount);
    }

    public void testRetryOn403ForPutAndPost() throws IOException {
        final Set<String> requestsErroredFor403 = ConcurrentCollections.newConcurrentSet();
        s3HttpHandler.setInterceptor(new Interceptor() {
            @SuppressForbidden(reason = "this test uses a HttpServer to emulate an S3 endpoint")
            @Override
            public boolean intercept(HttpExchange exchange) throws IOException {
                final String request = exchange.getRequestMethod() + " " + exchange.getRequestURI().toString();
                if ((request.startsWith("PUT /") || request.startsWith("POST /")) && requestsErroredFor403.add(request)) {
                    try (exchange) {
                        final byte[] response = Strings.format("""
                            <?xml version="1.0" encoding="UTF-8"?>
                            <Error>
                              <Code>InvalidAccessKeyId</Code>
                              <Message>The AWS Access Key Id you provided does not exist in our records.</Message>
                              <RequestId>%s</RequestId>
                            </Error>""", randomUUID()).getBytes(StandardCharsets.UTF_8);
                        exchange.getResponseHeaders().add("Content-Type", "application/xml");
                        exchange.sendResponseHeaders(403, response.length);
                        exchange.getResponseBody().write(response);
                    }
                    return true;
                }
                return false;
            }
        });

        final String masterAndIndexNode = startMasterAndIndexNode();
        final String searchNode = startSearchNode();
        ensureStableCluster(2);
        try {
            final String indexName = randomIdentifier();
            createIndex(indexName, indexSettings(1, 1).build());
            ensureGreen(indexName);
            final int batches = between(5, 20);
            for (int i = 0; i < batches; i++) {
                indexDocs(indexName, between(1, 10));
                refresh(indexName);
            }
            ensureGreen(indexName);
            // The test passes as long as the cluster forms successfully and completes some normal activities
            assertThat(requestsErroredFor403, not(empty()));
        } finally {
            internalCluster().stopNode(searchNode);
            internalCluster().stopNode(masterAndIndexNode);
        }
    }

    public void testRetryOn403ForGet() throws Exception {
        final Set<String> requestsErroredFor403 = ConcurrentCollections.newConcurrentSet();
        var getBlobPredicate = new AtomicReference<Predicate<String>>(Predicates.never());
        var listBlobsPredicate = new AtomicReference<Predicate<String>>(Predicates.never());
        s3HttpHandler.setInterceptor(new Interceptor() {
            @SuppressForbidden(reason = "this test uses a HttpServer to emulate an S3 endpoint")
            @Override
            public boolean intercept(HttpExchange exchange) throws IOException {
                final String request = exchange.getRequestMethod() + " " + exchange.getRequestURI().toString();
                if ((getBlobPredicate.get().test(request) || listBlobsPredicate.get().test(request))
                    && requestsErroredFor403.add(request)) {
                    try (exchange) {
                        final byte[] response = Strings.format("""
                            <?xml version="1.0" encoding="UTF-8"?>
                            <Error>
                              <Code>InvalidAccessKeyId</Code>
                              <Message>The AWS Access Key Id you provided does not exist in our records.</Message>
                              <RequestId>%s</RequestId>
                            </Error>""", randomUUID()).getBytes(StandardCharsets.UTF_8);
                        exchange.getResponseHeaders().add("Content-Type", "application/xml");
                        exchange.sendResponseHeaders(403, response.length);
                        exchange.getResponseBody().write(response);
                    }
                    return true;
                }
                return false;
            }
        });

        final String masterNode = startMasterAndIndexNode();
        final String indexNode1 = startIndexNode();
        final String searchNode = startSearchNode();
        ensureStableCluster(3);

        String indexNode2 = null;
        try {
            var indexName = randomIdentifier();
            createIndex(
                indexName,
                indexSettings(1, 0).put(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_PREFIX + "._name", indexNode1).build()
            );
            var indexUuid = resolveIndex(indexName).getUUID();

            final ObjectStoreService objectStoreService = getCurrentMasterObjectStoreService();
            final BlobStoreRepository objectStore = randomBoolean()
                ? objectStoreService.getClusterObjectStore()
                : objectStoreService.getProjectObjectStore(ProjectId.DEFAULT);
            final var shardPath = "indices/" + indexUuid + "/0/";
            final var rawPrefix = objectStore.basePath().buildAsString() + shardPath;
            final var encodedPrefix = URLEncoder.encode(rawPrefix, StandardCharsets.UTF_8);
            listBlobsPredicate.set(rq -> rq.startsWith("GET /bucket?") && rq.contains("prefix=" + encodedPrefix));
            getBlobPredicate.set(rq -> rq.startsWith("GET /bucket/" + rawPrefix));

            int iters = between(1, 5);
            for (int i = 0; i < iters; i++) {
                indexDocs(indexName, between(1, 10));
                flush(indexName);
                internalCluster().restartNode(indexNode1);
                ensureGreen(indexName);

                assertThat(requestsErroredFor403, not(empty()));
                requestsErroredFor403.clear();
            }

            indexNode2 = startIndexNode();
            ensureStableCluster(4);

            updateIndexSettings(
                Settings.builder().putList(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_PREFIX + "._name", indexNode2),
                indexName
            );
            ensureGreen(indexName);

            assertThat(requestsErroredFor403, not(empty()));
            requestsErroredFor403.clear();

            updateIndexSettings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                    .putList(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_PREFIX + "._name", List.of(indexNode2, searchNode)),
                indexName
            );
            ensureGreen(indexName);
            // The test passes as long as the cluster forms successfully and completes some normal activities
            assertThat(requestsErroredFor403, not(empty()));
        } finally {
            internalCluster().stopNode(searchNode);
            internalCluster().stopNode(indexNode1);
            internalCluster().stopNode(indexNode2);
            internalCluster().stopNode(masterNode);
        }
    }

    public void testTenaciousRetriesAgainstTransientCSPError() throws Exception {
        AtomicLong getBlobAttempted = new AtomicLong(0);
        AtomicLong listBlobsAttempted = new AtomicLong(0);
        var getBlobPredicate = new AtomicReference<Predicate<String>>(Predicates.never());
        var listBlobsPredicate = new AtomicReference<Predicate<String>>(Predicates.never());

        final var requiredAttempts = maxRetries + randomLongBetween(3, 5);

        s3HttpHandler.setInterceptor(new Interceptor() {
            @SuppressForbidden(reason = "this test uses a HttpServer to emulate an S3 endpoint")
            @Override
            public boolean intercept(HttpExchange exchange) throws IOException {
                final String request = exchange.getRequestMethod() + " " + exchange.getRequestURI().toString();
                var shouldIntercept = getBlobPredicate.get().test(request) && getBlobAttempted.incrementAndGet() < requiredAttempts
                    || listBlobsPredicate.get().test(request) && listBlobsAttempted.incrementAndGet() < requiredAttempts;
                if (shouldIntercept) {
                    try (exchange) {
                        if (randomBoolean()) {
                            exchange.sendResponseHeaders(
                                randomFrom(SERVICE_UNAVAILABLE.getStatus(), TOO_MANY_REQUESTS.getStatus(), REQUEST_TIMEOUT.getStatus()),
                                -1
                            );
                        } else {
                            final var responseBody = Strings.format("""
                                <?xml version="1.0" encoding="UTF-8"?>
                                <Error>
                                  <Code>%s</Code>
                                  <Message>InvalidAccessKeyId</Message>
                                  <RequestId>%s</RequestId>
                                </Error>""", "InvalidAccessKeyId", randomUUID()).getBytes(StandardCharsets.UTF_8);
                            exchange.sendResponseHeaders(RestStatus.FORBIDDEN.getStatus(), responseBody.length);
                            exchange.getResponseBody().write(responseBody);
                        }
                    }
                    return true;
                }

                return false;
            }
        });

        final var settings = Settings.builder()
            .put("s3.client.test.max_retries", 0) // disable sdk client retries
            .put("s3.client.test.tenacious_retries.enabled", true)
            .build();

        final String masterNode = startMasterAndIndexNode(settings);
        final String indexNode1 = startIndexNode(settings);
        final String searchNode = startSearchNode(settings);
        ensureStableCluster(3);

        String indexNode2 = null;
        try {

            var indexName = randomIdentifier();
            createIndex(
                indexName,
                indexSettings(1, 0).put(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_PREFIX + "._name", indexNode1).build()
            );

            var indexUuid = resolveIndex(indexName).getUUID();

            final ObjectStoreService objectStoreService = getCurrentMasterObjectStoreService();
            final BlobStoreRepository objectStore = randomBoolean()
                ? objectStoreService.getClusterObjectStore()
                : objectStoreService.getProjectObjectStore(ProjectId.DEFAULT);

            final var shardPath = "indices/" + indexUuid + "/0/";
            final var rawPrefix = objectStore.basePath().buildAsString() + shardPath;
            final var encodedPrefix = URLEncoder.encode(rawPrefix, StandardCharsets.UTF_8);
            listBlobsPredicate.set(rq -> rq.startsWith("GET /bucket?") && rq.contains("prefix=" + encodedPrefix));
            getBlobPredicate.set(rq -> rq.startsWith("GET /bucket/" + rawPrefix));

            int iterations = between(1, 5);
            for (int i = 0; i < iterations; i++) {
                indexDocs(indexName, between(1, 10));
                flush(indexName);
                internalCluster().restartNode(indexNode1);
                ensureGreen(indexName);

                assertThat(getBlobAttempted.get(), greaterThanOrEqualTo(requiredAttempts));
                assertThat(listBlobsAttempted.get(), greaterThanOrEqualTo(requiredAttempts));
                getBlobAttempted.set(0);
                listBlobsAttempted.set(0);
            }

            indexNode2 = startIndexNode(settings);
            ensureStableCluster(4);

            updateIndexSettings(
                Settings.builder().putList(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_PREFIX + "._name", indexNode2),
                indexName
            );
            ensureGreen(indexName);

            assertThat(getBlobAttempted.get(), greaterThanOrEqualTo(requiredAttempts));
            getBlobAttempted.set(0);
            listBlobsAttempted.set(0);

            updateIndexSettings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                    .putList(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_PREFIX + "._name", List.of(indexNode2, searchNode)),
                indexName
            );
            ensureGreen(indexName);
            assertThat(getBlobAttempted.get(), greaterThanOrEqualTo(requiredAttempts));
            assertThat(listBlobsAttempted.get(), greaterThanOrEqualTo(requiredAttempts));
        } finally {
            internalCluster().stopNode(masterNode);
            internalCluster().stopNode(indexNode1);
            internalCluster().stopNode(indexNode2);
            internalCluster().stopNode(searchNode);
        }
    }

    public void testCopyShardWithTenaciousRetries() throws Exception {
        final var settings = Settings.builder().put("s3.client.test.tenacious_retries.enabled", true).build();
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

    @SuppressForbidden(reason = "this test uses a HttpServer to emulate an S3 endpoint")
    public static class InterceptableS3HttpHandler extends S3HttpHandler {
        private volatile Interceptor interceptor;

        public InterceptableS3HttpHandler(String bucket) {
            super(bucket, S3ConsistencyModel.AWS_DEFAULT);
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (interceptor == null || false == interceptor.intercept(exchange)) {
                super.handle(exchange);
            }
        }

        public void setInterceptor(Interceptor interceptor) {
            this.interceptor = interceptor;
        }
    }

    @SuppressForbidden(reason = "this test uses a HttpServer to emulate an S3 endpoint")
    public interface Interceptor {
        boolean intercept(HttpExchange exchange) throws IOException;
    }

    public static class TestStatelessPlugin extends TestUtils.StatelessPluginWithTrialLicense {

        public TestStatelessPlugin(Settings settings) {
            super(settings);
        }

        @Override
        protected ObjectStoreService createObjectStoreService(
            Settings settings,
            RepositoriesService repositoriesService,
            ThreadPool threadPool,
            ClusterService clusterService,
            ProjectResolver projectResolver,
            MeterRegistry meterRegistry
        ) {
            return new TestObjectStoreService(settings, repositoriesService, threadPool, clusterService, projectResolver, meterRegistry);
        }
    }

    public static class TestObjectStoreService extends ObjectStoreService {

        public TestObjectStoreService(
            Settings settings,
            RepositoriesService repositoriesService,
            ThreadPool threadPool,
            ClusterService clusterService,
            ProjectResolver projectResolver,
            MeterRegistry meterRegistry
        ) {
            super(settings, repositoriesService, threadPool, clusterService, projectResolver, meterRegistry);
        }

        @Override
        protected Settings getRepositorySettings(ObjectStoreType type, Settings settings) {
            return Settings.builder()
                .put(super.getRepositorySettings(type, settings))
                .put("buffer_size", MULTIPART_UPLOAD_BUFFER_SIZE)
                .build();
        }
    }
}
