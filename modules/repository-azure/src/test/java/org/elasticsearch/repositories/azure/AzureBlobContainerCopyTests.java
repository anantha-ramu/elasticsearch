/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.azure;

import fixture.azure.AzureHttpHandler;
import fixture.azure.MockAzureBlobStore;

import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RetryPolicyType;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStoreActionStats;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.mocksocket.MockHttpServer;
import org.elasticsearch.repositories.RepositoriesMetrics;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.NoSuchFileException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.elasticsearch.repositories.azure.AzureRepository.Repository.CONTAINER_SETTING;
import static org.elasticsearch.repositories.azure.AzureRepository.Repository.COPY_BLOCK_SIZE_SETTING;
import static org.elasticsearch.repositories.azure.AzureRepository.Repository.COPY_POLL_INTERVAL;
import static org.elasticsearch.repositories.azure.AzureRepository.Repository.MAX_CONCURRENT_COPY_BLOCKS_SETTING;
import static org.elasticsearch.repositories.azure.AzureStorageSettings.ACCOUNT_SETTING;
import static org.elasticsearch.repositories.azure.AzureStorageSettings.ENDPOINT_SUFFIX_SETTING;
import static org.elasticsearch.repositories.azure.AzureStorageSettings.KEY_SETTING;
import static org.elasticsearch.repositories.azure.AzureStorageSettings.MAX_RETRIES_SETTING;
import static org.elasticsearch.repositories.azure.AzureStorageSettings.SAS_TOKEN_SETTING;
import static org.elasticsearch.repositories.azure.AzureStorageSettings.TIMEOUT_SETTING;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;

/**
 * Tests the copy of a blob between two Azure storage accounts. The tests also check that a copy inside one storage account still uses
 * Copy Blob.
 * <p>
 * A copy between two accounts is split into byte ranges of the source blob. Azure copies each range into a block of the new blob (a Put
 * Block From URL request). One Put Block List request then commits the blocks, that is, joins them into the new blob.
 * <p>
 * The fixture serves only one account, so the test builds two clients with different endpoints. One client reaches the fixture at {@code
 * localhost} and the other at the numeric loopback address. {@code AzureBlobStore} compares only the endpoints of the two clients, so it
 * treats them as two storage accounts. Both clients use the same account name.
 * <p>
 * The client of the source has a SAS token, because a copy between accounts needs one. The fixture ignores the token, and the tests only
 * check that it is part of the source URL. The client of the destination has an account key.
 */
@SuppressForbidden(reason = "use a http server")
public class AzureBlobContainerCopyTests extends ESTestCase {

    private static final String ACCOUNT = "account";
    private static final String CONTAINER = "container";
    private static final Pattern BLOCK_ID = Pattern.compile("blockid=([^&]+)");

    private static HttpServer httpServer;
    private static ThreadPool threadPool;
    private static AzureClientProvider clientProvider;
    private static ClusterService clusterService;

    @BeforeClass
    public static void startServer() throws IOException {
        threadPool = new TestThreadPool(
            AzureBlobContainerCopyTests.class.getName(),
            AzureRepositoryPlugin.executorBuilder(Settings.EMPTY),
            AzureRepositoryPlugin.nettyEventLoopExecutorBuilder(Settings.EMPTY)
        );
        httpServer = MockHttpServer.createHttp(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        // The default executor of HttpServer handles one request at a time. Use the generic thread pool, so that requests can overlap and
        // the tests can see them overlap.
        httpServer.setExecutor(threadPool.generic());
        httpServer.start();
        clientProvider = AzureClientProvider.create(threadPool, Settings.EMPTY);
        clientProvider.start();
        clusterService = ClusterServiceUtils.createClusterService(threadPool);
    }

    @AfterClass
    public static void stopServer() throws Exception {
        clientProvider.close();
        httpServer.stop(0);
        ThreadPool.terminate(threadPool, 10L, TimeUnit.SECONDS);
    }

    /**
     * A request that reached the fixture.
     */
    private record LoggedRequest(String method, String query, String copySource, String sourceRange) {

        boolean isPutBlockFromUrl() {
            return "PUT".equals(method) && query.contains("comp=block&") && query.contains("blockid=") && copySource != null;
        }

        boolean isPutBlock() {
            return "PUT".equals(method) && query.contains("comp=block&") && query.contains("blockid=") && copySource == null;
        }

        boolean isPutBlockList() {
            return "PUT".equals(method) && query.contains("comp=blocklist");
        }

        boolean isCopyBlob() {
            return "PUT".equals(method) && copySource != null && query.contains("comp=block") == false;
        }

        String blockId() {
            final Matcher matcher = BLOCK_ID.matcher(query);
            assertTrue(query, matcher.find());
            return matcher.group(1);
        }
    }

    /**
     * Answers a request with an error before the fixture sees it, to make a request fail.
     */
    @SuppressForbidden(reason = "use a http server")
    private interface Fault {
        /**
         * @return true if this fault has answered the request, so the fixture must not handle it
         */
        boolean answer(HttpExchange exchange, LoggedRequest request) throws IOException;
    }

    private AzureHttpHandler azureHttpHandler;
    private final List<LoggedRequest> requests = new CopyOnWriteArrayList<>();
    private final AtomicInteger putBlockFromUrlInFlight = new AtomicInteger();
    private final AtomicInteger putBlockFromUrlMaxInFlight = new AtomicInteger();
    private volatile Fault fault = (exchange, request) -> false;
    private volatile long putBlockFromUrlDelayMillis = 0;

    @Before
    public void configureAzureHandler() {
        azureHttpHandler = new AzureHttpHandler(ACCOUNT, CONTAINER, null, MockAzureBlobStore.LeaseExpiryPredicate.NEVER_EXPIRE);
        httpServer.createContext("/", exchange -> {
            final String query = exchange.getRequestURI().getRawQuery();
            final LoggedRequest request = new LoggedRequest(
                exchange.getRequestMethod(),
                query == null ? "" : query,
                exchange.getRequestHeaders().getFirst("x-ms-copy-source"),
                exchange.getRequestHeaders().getFirst("x-ms-source-range")
            );
            requests.add(request);
            final boolean putBlockFromUrl = request.isPutBlockFromUrl();
            if (putBlockFromUrl) {
                putBlockFromUrlMaxInFlight.accumulateAndGet(putBlockFromUrlInFlight.incrementAndGet(), Math::max);
            }
            try {
                if (putBlockFromUrl && putBlockFromUrlDelayMillis > 0) {
                    safeSleep(putBlockFromUrlDelayMillis);
                }
                if (fault.answer(exchange, request)) {
                    // The faults in this class answer only Put Block From URL requests. They have no body, so the exchange can close
                    // without reading one.
                    exchange.close();
                } else {
                    azureHttpHandler.handle(exchange);
                }
            } finally {
                if (putBlockFromUrl) {
                    putBlockFromUrlInFlight.decrementAndGet();
                }
            }
        });
    }

    @After
    public void removeAzureHandler() {
        httpServer.removeContext("/");
    }

    private record Containers(AzureBlobContainer source, AzureBlobContainer destination, String sourceUrlPrefix, String sasToken) {}

    /**
     * Makes a source container and a destination container. They use two endpoints of the same fixture, so the repository code treats
     * them as two storage accounts.
     *
     * @param blockSize          the size of the blocks of a copy, in bytes
     * @param maxConcurrentBlocks how many blocks of a copy are in flight at the same time
     * @param maxRetries         how often the client retries a request that fails with a status that can be retried
     */
    private Containers buildContainers(long blockSize, int maxConcurrentBlocks, int maxRetries) {
        final InetSocketAddress address = httpServer.getAddress();
        final String numericHost = InetAddresses.toUriString(address.getAddress());
        final String sasToken = "sv=2024-08-04&ss=b&srt=co&sp=rl&sig=" + randomAlphaOfLength(20);
        final AzureBlobContainer source = buildContainer(numericHost, sasToken, blockSize, maxConcurrentBlocks, maxRetries);
        final AzureBlobContainer destination = buildContainer("localhost", null, blockSize, maxConcurrentBlocks, maxRetries);
        return new Containers(
            source,
            destination,
            "http://" + numericHost + ":" + address.getPort() + "/" + ACCOUNT + "/" + CONTAINER,
            sasToken
        );
    }

    private AzureBlobContainer buildContainer(String host, String sasToken, long blockSize, int maxConcurrentBlocks, int maxRetries) {
        final String clientName = randomIdentifier();
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(ACCOUNT_SETTING.getConcreteSettingForNamespace(clientName).getKey(), ACCOUNT);
        if (sasToken != null) {
            secureSettings.setString(SAS_TOKEN_SETTING.getConcreteSettingForNamespace(clientName).getKey(), sasToken);
        } else {
            final String key = Base64.getEncoder().encodeToString(randomAlphaOfLength(14).getBytes(UTF_8));
            secureSettings.setString(KEY_SETTING.getConcreteSettingForNamespace(clientName).getKey(), key);
        }

        final InetSocketAddress address = httpServer.getAddress();
        final String endpoint = "ignored;DefaultEndpointsProtocol=http;BlobEndpoint=http://"
            + host
            + ":"
            + address.getPort()
            + "/"
            + ACCOUNT;
        final Settings clientSettings = Settings.builder()
            .put(ENDPOINT_SUFFIX_SETTING.getConcreteSettingForNamespace(clientName).getKey(), endpoint)
            .put(MAX_RETRIES_SETTING.getConcreteSettingForNamespace(clientName).getKey(), maxRetries)
            .put(TIMEOUT_SETTING.getConcreteSettingForNamespace(clientName).getKey(), TimeValue.timeValueSeconds(60))
            .setSecureSettings(secureSettings)
            .build();

        final AzureStorageService service = new AzureStorageService(
            clientSettings,
            clientProvider,
            clusterService,
            TestProjectResolvers.DEFAULT_PROJECT_ONLY
        ) {
            @Override
            RequestRetryOptions getRetryOptions(LocationMode locationMode, AzureStorageSettings azureStorageSettings) {
                final RequestRetryOptions base = super.getRetryOptions(locationMode, azureStorageSettings);
                return new RequestRetryOptions(
                    RetryPolicyType.EXPONENTIAL,
                    base.getMaxTries(),
                    base.getTryTimeoutDuration(),
                    Duration.ofMillis(50),
                    Duration.ofMillis(100),
                    null
                );
            }
        };

        final RepositoryMetadata repositoryMetadata = new RepositoryMetadata(
            "repository",
            AzureRepository.TYPE,
            Settings.builder()
                .put(CONTAINER_SETTING.getKey(), CONTAINER)
                .put(AzureStorageSettings.ACCOUNT_SETTING.getKey(), clientName)
                .put(COPY_BLOCK_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(blockSize))
                .put(MAX_CONCURRENT_COPY_BLOCKS_SETTING.getKey(), maxConcurrentBlocks)
                .put(COPY_POLL_INTERVAL.getKey(), TimeValue.timeValueMillis(100))
                .build()
        );

        return new AzureBlobContainer(
            BlobPath.EMPTY,
            new AzureBlobStore(
                ProjectId.DEFAULT,
                repositoryMetadata,
                service,
                BigArrays.NON_RECYCLING_INSTANCE,
                RepositoriesMetrics.NOOP,
                null,
                null
            )
        );
    }

    /**
     * Writes the source blob with the source client. Then clears the request log, so that the test sees only the requests of the copy.
     */
    private void writeSource(Containers containers, String sourceName, byte[] data) throws IOException {
        containers.source().writeBlob(OperationPurpose.SNAPSHOT_DATA, sourceName, new BytesArray(data), true);
        requests.clear();
    }

    private byte[] destinationBytes(String destinationName) {
        return BytesReference.toBytes(azureHttpHandler.getMockBlobStore().getBlob(destinationName, null).getContents());
    }

    /**
     * The values of the source range header of the requests that stage the blocks of a blob of {@code blobSize} bytes.
     */
    private static List<String> expectedRanges(long blobSize, long blockSize) {
        final List<String> ranges = new ArrayList<>();
        for (long offset = 0; offset < blobSize; offset += blockSize) {
            ranges.add("bytes=" + offset + "-" + (Math.min(offset + blockSize, blobSize) - 1));
        }
        return ranges;
    }

    private List<LoggedRequest> requests(Predicate<LoggedRequest> filter) {
        return requests.stream().filter(filter).toList();
    }

    /**
     * Copies a blob of random bytes between the two accounts and checks the copy, the requests that made it, and the stats.
     */
    private void copyAndCheck(long blobSize, long blockSize, int maxConcurrentBlocks) throws IOException {
        final Containers containers = buildContainers(blockSize, maxConcurrentBlocks, 0);
        final String sourceName = randomIdentifier();
        final String destinationName = randomIdentifier();
        final byte[] data = randomByteArrayOfLength(Math.toIntExact(blobSize));
        writeSource(containers, sourceName, data);

        containers.destination().copyBlob(OperationPurpose.SNAPSHOT_DATA, containers.source(), sourceName, destinationName, blobSize, null);

        assertArrayEquals(data, destinationBytes(destinationName));
        final List<String> expectedRanges = expectedRanges(blobSize, blockSize);
        final List<LoggedRequest> staging = requests(LoggedRequest::isPutBlockFromUrl);
        assertThat(staging, hasSize(expectedRanges.size()));
        assertThat(staging.stream().map(LoggedRequest::sourceRange).sorted().toList(), equalTo(expectedRanges.stream().sorted().toList()));
        // the source is the URL of the source blob in its account, with the SAS token
        final String expectedSource = containers.sourceUrlPrefix() + "/" + sourceName + "?" + containers.sasToken();
        assertThat(staging.stream().map(LoggedRequest::copySource).collect(Collectors.toSet()), equalTo(Set.of(expectedSource)));
        assertThat(staging.stream().map(LoggedRequest::blockId).distinct().toList(), hasSize(expectedRanges.size()));
        // one commit, and no other way to copy or to stage a block
        assertThat(requests(LoggedRequest::isPutBlockList), hasSize(1));
        assertThat(requests(LoggedRequest::isCopyBlob), empty());
        assertThat(requests(LoggedRequest::isPutBlock), empty());
        assertThat(putBlockFromUrlMaxInFlight.get(), lessThanOrEqualTo(maxConcurrentBlocks));

        // The stats count the copy as Put Block From URL operations and one Put Block List operation, not as Put Block or Copy Blob.
        final Map<String, BlobStoreActionStats> stats = containers.destination().getBlobStore().stats();
        assertThat(stats.get(AzureBlobStore.Operation.PUT_BLOCK_FROM_URL.getKey()).operations(), equalTo((long) expectedRanges.size()));
        assertThat(stats.get(AzureBlobStore.Operation.PUT_BLOCK_LIST.getKey()).operations(), equalTo(1L));
        assertThat(stats.get(AzureBlobStore.Operation.PUT_BLOCK.getKey()).operations(), equalTo(0L));
        assertThat(stats.get(AzureBlobStore.Operation.COPY_BLOB.getKey()).operations(), equalTo(0L));
    }

    public void testCopyInSeveralBlocks() throws IOException {
        final long blockSize = between(16, 64);
        // the last block is shorter, or as long as the others
        final long blobSize = blockSize * between(1, 12) + between(1, (int) blockSize);
        copyAndCheck(blobSize, blockSize, between(1, 8));
    }

    public void testBlobSmallerThanOneBlock() throws IOException {
        final long blockSize = between(2, 1024);
        copyAndCheck(between(1, (int) blockSize - 1), blockSize, between(1, 8));
    }

    public void testBlobOfOneBlockAndOfWholeBlocks() throws IOException {
        final long blockSize = between(1, 64);
        copyAndCheck(blockSize * between(1, 12), blockSize, between(1, 8));
    }

    public void testBlocksInFlightAreLimited() throws IOException {
        final int maxConcurrentBlocks = between(2, 4);
        // the requests wait, so that the blocks in flight overlap
        putBlockFromUrlDelayMillis = 100;
        final long blockSize = 10;
        copyAndCheck(blockSize * between(maxConcurrentBlocks * 6, maxConcurrentBlocks * 10), blockSize, maxConcurrentBlocks);
        assertThat(putBlockFromUrlMaxInFlight.get(), equalTo(maxConcurrentBlocks));
    }

    public void testFailedBlockCommitsNothing() throws IOException {
        final long blockSize = between(8, 32);
        final long blobSize = blockSize * between(4, 10) + between(0, (int) blockSize - 1);
        final Containers containers = buildContainers(blockSize, between(1, 4), 0);
        final String sourceName = randomIdentifier();
        final String destinationName = randomIdentifier();
        writeSource(containers, sourceName, randomByteArrayOfLength(Math.toIntExact(blobSize)));

        // The fixture fails one block with 403 CannotVerifyCopySource. We expect Azure to answer like this when it cannot read the
        // source, for example when the SAS token does not allow the read. This is an assumption: we have not checked it against Azure.
        final String failingRange = randomFrom(expectedRanges(blobSize, blockSize));
        fault = (exchange, request) -> {
            if (request.isPutBlockFromUrl() && failingRange.equals(request.sourceRange())) {
                AzureHttpHandler.sendError(
                    exchange,
                    RestStatus.FORBIDDEN,
                    "CannotVerifyCopySource",
                    "Server failed to authenticate the request.",
                    "<CopySourceStatusCode>403</CopySourceStatusCode><CopySourceErrorCode>AuthenticationFailed</CopySourceErrorCode>"
                );
                return true;
            }
            return false;
        };
        final IOException e = expectThrows(
            IOException.class,
            () -> containers.destination()
                .copyBlob(OperationPurpose.SNAPSHOT_DATA, containers.source(), sourceName, destinationName, blobSize, null)
        );
        assertThat(e, not(instanceOf(NoSuchFileException.class)));
        assertThat(e.getMessage(), containsString("Unable to copy object [" + destinationName + "]"));

        // No block list is committed, and the new blob is not visible, even if some blocks were staged before the failure.
        assertThat(requests(LoggedRequest::isPutBlockList), empty());
        assertFalse(containers.destination().blobExists(OperationPurpose.SNAPSHOT_DATA, destinationName));
        // A delete of the new blob after the failure does not fail, and the blob is still not visible.
        containers.destination()
            .deleteBlobsIgnoringIfNotExists(OperationPurpose.SNAPSHOT_DATA, Collections.singletonList(destinationName).iterator());
        assertFalse(containers.destination().blobExists(OperationPurpose.SNAPSHOT_DATA, destinationName));
    }

    public void testServerBusyOnStagingIsRetried() throws IOException {
        final long blockSize = between(8, 32);
        final long blobSize = blockSize * between(3, 10) + between(0, (int) blockSize - 1);
        final Containers containers = buildContainers(blockSize, between(1, 4), 3);
        final String sourceName = randomIdentifier();
        final String destinationName = randomIdentifier();
        final byte[] data = randomByteArrayOfLength(Math.toIntExact(blobSize));
        writeSource(containers, sourceName, data);

        // the first attempt to stage some of the blocks finds Azure busy
        final List<String> ranges = expectedRanges(blobSize, blockSize);
        final Set<String> busyRanges = Set.copyOf(randomSubsetOf(between(1, 2), ranges));
        final Map<String, AtomicInteger> attempts = new ConcurrentHashMap<>();
        fault = (exchange, request) -> {
            if (request.isPutBlockFromUrl()
                && busyRanges.contains(request.sourceRange())
                && attempts.computeIfAbsent(request.sourceRange(), r -> new AtomicInteger()).incrementAndGet() == 1) {
                AzureHttpHandler.sendError(exchange, RestStatus.SERVICE_UNAVAILABLE);
                return true;
            }
            return false;
        };
        containers.destination().copyBlob(OperationPurpose.SNAPSHOT_DATA, containers.source(), sourceName, destinationName, blobSize, null);

        assertArrayEquals(data, destinationBytes(destinationName));
        // the client sent each busy block twice, and then committed once
        assertThat(requests(LoggedRequest::isPutBlockFromUrl), hasSize(ranges.size() + busyRanges.size()));
        assertThat(requests(LoggedRequest::isPutBlockList), hasSize(1));
        // The stats count one operation for each block. A retry adds a request but not an operation.
        final var putBlockFromUrl = containers.destination()
            .getBlobStore()
            .stats()
            .get(AzureBlobStore.Operation.PUT_BLOCK_FROM_URL.getKey());
        assertThat(putBlockFromUrl.operations(), equalTo((long) ranges.size()));
        assertThat(putBlockFromUrl.requests(), equalTo((long) (ranges.size() + busyRanges.size())));
    }

    public void testMissingSourceIsNoSuchFile() {
        final long blockSize = between(8, 32);
        final Containers containers = buildContainers(blockSize, between(1, 4), 0);
        final long blobSize = blockSize * between(1, 5);

        // The fixture answers a block request with 404 CannotVerifyCopySource and says that the source blob was not found.
        expectThrows(
            NoSuchFileException.class,
            () -> containers.destination()
                .copyBlob(OperationPurpose.SNAPSHOT_DATA, containers.source(), randomIdentifier(), randomIdentifier(), blobSize, null)
        );
        assertThat(requests(LoggedRequest::isPutBlockList), empty());
    }

    public void testSourceInAnotherAccountNeedsASasToken() {
        final long blockSize = between(8, 32);
        final String numericHost = InetAddresses.toUriString(httpServer.getAddress().getAddress());
        // the clients of both containers have an account key and no SAS token
        final AzureBlobContainer source = buildContainer(numericHost, null, blockSize, 1, 0);
        final AzureBlobContainer destination = buildContainer("localhost", null, blockSize, 1, 0);

        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> destination.copyBlob(OperationPurpose.SNAPSHOT_DATA, source, randomIdentifier(), randomIdentifier(), blockSize, null)
        );
        assertThat(e.getMessage(), containsString("without an SAS token"));
        assertThat(requests, empty());
    }

    public void testCopyInsideOneStorageAccountUsesCopyBlob() throws IOException {
        final long blockSize = between(8, 32);
        final Containers containers = buildContainers(blockSize, between(1, 4), 0);
        final String sourceName = randomIdentifier();
        final String destinationName = randomIdentifier();
        final byte[] data = randomByteArrayOfLength(Math.toIntExact(blockSize * between(2, 6)));
        // the destination container is both the source and the destination of the copy
        containers.destination().writeBlob(OperationPurpose.SNAPSHOT_DATA, sourceName, new BytesArray(data), true);
        requests.clear();

        containers.destination()
            .copyBlob(OperationPurpose.SNAPSHOT_DATA, containers.destination(), sourceName, destinationName, data.length, null);

        assertArrayEquals(data, destinationBytes(destinationName));
        assertThat(requests(LoggedRequest::isCopyBlob), hasSize(1));
        assertThat(requests(LoggedRequest::isPutBlockFromUrl), empty());
        assertThat(requests(LoggedRequest::isPutBlockList), empty());
    }

    public void testBlobWithNoBytesUsesCopyBlob() throws IOException {
        final Containers containers = buildContainers(between(8, 32), between(1, 4), 0);
        final String sourceName = randomIdentifier();
        final String destinationName = randomIdentifier();
        writeSource(containers, sourceName, new byte[0]);

        containers.destination().copyBlob(OperationPurpose.SNAPSHOT_DATA, containers.source(), sourceName, destinationName, 0L, null);

        assertThat(destinationBytes(destinationName).length, equalTo(0));
        // a blob with no bytes has no range to copy
        assertThat(requests(LoggedRequest::isCopyBlob), hasSize(1));
        assertThat(requests(LoggedRequest::isPutBlockFromUrl), empty());
    }

    public void testBlobThatNeedsTooManyBlocksFails() {
        // Azure allows at most 50,000 blocks in a blob. With blocks of 1 byte, a blob of 50,001 bytes needs one block too many. The check
        // runs before any request, so the fixture sees none. The arguments of buildContainers are a block size of 1 byte, one block at a
        // time and no retries.
        final Containers containers = buildContainers(1, 1, 0);

        final IOException e = expectThrows(
            IOException.class,
            () -> containers.destination()
                .copyBlob(OperationPurpose.SNAPSHOT_DATA, containers.source(), randomIdentifier(), randomIdentifier(), 50_001L, null)
        );
        assertThat(e.getMessage(), containsString("need [50001] blocks"));
        assertThat(e.getMessage(), containsString(COPY_BLOCK_SIZE_SETTING.getKey()));
        assertThat(requests, empty());
    }
}
