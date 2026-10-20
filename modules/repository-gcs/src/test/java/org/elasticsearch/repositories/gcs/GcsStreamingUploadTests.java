/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.repositories.gcs;

import fixture.gcs.FakeOAuth2HttpHandler;
import fixture.gcs.GoogleCloudStorageHttpHandler;

import com.sun.management.ThreadMXBean;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpPrincipal;
import com.sun.net.httpserver.HttpServer;

import org.apache.logging.log4j.Level;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RateLimiter;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.BackoffPolicy;
import org.elasticsearch.common.blobstore.BlobStoreActionStats;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.lucene.store.ByteArrayIndexInput;
import org.elasticsearch.common.lucene.store.InputStreamIndexInput;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.snapshots.blobstore.RateLimitingInputStream;
import org.elasticsearch.mocksocket.MockHttpServer;
import org.elasticsearch.repositories.RepositoriesMetrics;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.TestEsExecutors;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;
import org.junit.After;
import org.junit.Before;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.CRC32C;

import static fixture.gcs.TestUtils.createServiceAccount;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageClientSettings.CREDENTIALS_FILE_SETTING;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageClientSettings.ENDPOINT_SETTING;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageClientSettings.MAX_RETRIES_SETTING;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageClientSettings.READ_TIMEOUT_SETTING;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageClientSettings.TOKEN_URI_SETTING;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * Tests of the streaming snapshot upload (see {@link GcsStreamingUpload}) through the blob store, against a fake GCS server that can
 * commit part of a request and then fail it ({@link FakeResumableUploadHandler}) and against the repository's GCS fixture.
 */
@SuppressForbidden(reason = "use a http server")
public class GcsStreamingUploadTests extends ESTestCase {

    private static final String BUCKET = "bucket";
    private static final int KB256 = 256 * 1024;
    private static final String DEBUG_LOGGER = "org.elasticsearch.repositories.gcs.StreamingUpload";

    private HttpServer httpServer;
    private ExecutorService executor;
    private ThreadPool threadPool;
    private FakeResumableUploadHandler fake;
    private ClusterService clusterService;
    private GcsRepositoryStatsCollector statsCollector;
    private RecordingMeterRegistry meterRegistry;
    private final List<CountDownLatch> latches = new ArrayList<>();
    /** replaces the storage client of the blob store, to make it unsuitable for streaming */
    private java.util.function.UnaryOperator<MeteredStorage> clientWrapper;
    /** chunk size of the SDK write channel, 16MiB if not set */
    private ByteSizeValue sdkChunkSize;

    @Before
    public void startServer() throws Exception {
        executor = EsExecutors.newScaling(
            "gcs-streaming-upload-test",
            1,
            100,
            30,
            TimeUnit.SECONDS,
            true,
            TestEsExecutors.testOnlyDaemonThreadFactory("gcs-streaming-upload-test"),
            new ThreadContext(Settings.EMPTY)
        );
        httpServer = MockHttpServer.createHttp(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        httpServer.setExecutor(executor);
        fake = new FakeResumableUploadHandler(BUCKET);
        httpServer.createContext("/", fake);
        httpServer.createContext("/token", new FakeOAuth2HttpHandler());
        httpServer.start();
        threadPool = new TestThreadPool(getTestClass().getName());
        meterRegistry = new RecordingMeterRegistry();
        statsCollector = new GcsRepositoryStatsCollector(
            GcsRepositoryStatsCollector.NOOP_TIMER,
            new RepositoryMetadata("repo", GoogleCloudStorageRepository.TYPE, Settings.EMPTY),
            new RepositoriesMetrics(meterRegistry)
        );
    }

    @After
    public void stopServer() {
        latches.forEach(CountDownLatch::countDown);
        httpServer.stop(0);
        ThreadPool.terminate(executor, 10, TimeUnit.SECONDS);
        if (clusterService != null) {
            clusterService.close();
        }
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    // ---- scaffolding ----

    private String httpServerUrl() {
        final InetSocketAddress address = httpServer.getAddress();
        return "http://" + InetAddresses.toUriString(address.getAddress()) + ":" + address.getPort();
    }

    private CountDownLatch newLatch() {
        final CountDownLatch latch = new CountDownLatch(1);
        latches.add(latch);
        return latch;
    }

    private static Settings.Builder streamingSettings(long partSize) {
        return Settings.builder()
            .put(GcsStreamingUploadSettings.PART_SIZE_SETTING.getKey(), partSize + "b")
            .put(GcsStreamingUploadSettings.INITIAL_BACKOFF_SETTING.getKey(), "1ms")
            .put(GcsStreamingUploadSettings.MAX_BACKOFF_SETTING.getKey(), "5ms");
    }

    private GoogleCloudStorageBlobStore createBlobStore(Settings.Builder streamingSettings) {
        return createBlobStore(streamingSettings, null, null);
    }

    private GoogleCloudStorageBlobStore createBlobStore(
        Settings.Builder streamingSettings,
        @Nullable TimeValue readTimeout,
        @Nullable String dataStorageClass
    ) {
        final String client = randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
        final Settings.Builder clientSettings = Settings.builder();
        clientSettings.put(ENDPOINT_SETTING.getConcreteSettingForNamespace(client).getKey(), httpServerUrl());
        clientSettings.put(TOKEN_URI_SETTING.getConcreteSettingForNamespace(client).getKey(), httpServerUrl() + "/token");
        clientSettings.put(MAX_RETRIES_SETTING.getConcreteSettingForNamespace(client).getKey(), 0);
        if (readTimeout != null) {
            clientSettings.put(READ_TIMEOUT_SETTING.getConcreteSettingForNamespace(client).getKey(), readTimeout);
        }
        if (sdkChunkSize != null) {
            clientSettings.put(
                GoogleCloudStorageClientSettings.RESUMABLE_WRITE_BUFFER_SIZE_SETTING.getConcreteSettingForNamespace(client).getKey(),
                sdkChunkSize
            );
        }
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setFile(CREDENTIALS_FILE_SETTING.getConcreteSettingForNamespace(client).getKey(), createServiceAccount(random()));
        clientSettings.setSecureSettings(secureSettings);

        clusterService = ClusterServiceUtils.createClusterService(
            threadPool,
            ProjectId.DEFAULT,
            streamingSettings.build(),
            Set.copyOf(GcsStreamingUploadSettings.getSettings())
        );
        final GoogleCloudStorageService service = new GoogleCloudStorageService(clusterService, TestProjectResolvers.DEFAULT_PROJECT_ONLY);
        service.refreshAndClearCache(GoogleCloudStorageClientSettings.load(clientSettings.build()));
        return new GoogleCloudStorageBlobStore(
            ProjectId.DEFAULT,
            BUCKET,
            client,
            "repo",
            service,
            BigArrays.NON_RECYCLING_INSTANCE,
            randomIntBetween(1, 8) * 1024,
            1024,
            ByteSizeValue.ofMb(1).getBytes(),
            BackoffPolicy.noBackoff(),
            statsCollector,
            dataStorageClass,
            null
        ) {
            @Override
            MeteredStorage client() throws IOException {
                final MeteredStorage client = super.client();
                return clientWrapper == null ? client : clientWrapper.apply(client);
            }
        };
    }

    private static void write(GoogleCloudStorageBlobStore store, String name, InputStream source, long size, boolean failIfAlreadyExists)
        throws IOException {
        store.writeBlob(OperationPurpose.SNAPSHOT_DATA, name, source, size, failIfAlreadyExists);
    }

    /**
     * A source like the one the snapshot code hands to the blob store: an InputStreamIndexInput, whose reset seeks back to the mark (for
     * snapshots of stateless shards this reopens a GET), behind the rate limiter and the wrapper that checks for aborted snapshots.
     *
     * @param resets counts how often the source was rewound to its mark
     * @param beforeRead called with the offset of the next byte before each read, and may throw like an aborted snapshot does
     */
    private static InputStream snapshotSource(byte[] data, AtomicInteger resets, @Nullable ReadHook beforeRead) {
        final IndexInput input = new ByteArrayIndexInput("snapshot", data) {
            @Override
            public void seek(long position) throws IOException {
                resets.incrementAndGet();
                super.seek(position);
            }
        };
        final InputStream rateLimited = new RateLimitingInputStream(
            new InputStreamIndexInput(input, data.length),
            () -> new RateLimiter.SimpleRateLimiter(100_000),
            nanos -> {}
        );
        return new HookedInputStream(rateLimited, beforeRead);
    }

    interface ReadHook {
        void beforeRead(long offset, int resets) throws IOException;
    }

    /**
     * Like the wrapper in BlobStoreRepository#snapshotFile, which leaves mark and reset to the stream it wraps; this also tracks the
     * offset so that a hook can act on it.
     */
    private static final class HookedInputStream extends FilterInputStream {
        private final ReadHook hook;
        private long offset;
        private long markedOffset;
        private int resets;

        HookedInputStream(InputStream in, @Nullable ReadHook hook) {
            super(in);
            this.hook = hook;
        }

        @Override
        public int read() throws IOException {
            if (hook != null) {
                hook.beforeRead(offset, resets);
            }
            final int b = super.read();
            if (b >= 0) {
                offset++;
            }
            return b;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (hook != null) {
                hook.beforeRead(offset, resets);
            }
            final int n = super.read(b, off, len);
            if (n > 0) {
                offset += n;
            }
            return n;
        }

        @Override
        public synchronized void mark(int readlimit) {
            super.mark(readlimit);
            markedOffset = offset;
        }

        @Override
        public synchronized void reset() throws IOException {
            super.reset();
            offset = markedOffset;
            resets++;
        }
    }

    /**
     * A source of {@code size} bytes of a repeating pattern that is never held in memory, for uploads too big to hold.
     */
    private static final class PatternInputStream extends InputStream {
        private final long size;
        private final ReadHook hook;
        private long position;
        private long mark;

        PatternInputStream(long size, @Nullable ReadHook hook) {
            this.size = size;
            this.hook = hook;
        }

        @Override
        public int read() {
            return position >= size ? -1 : (int) ((position++ * 31 + 7) & 0xff);
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (hook != null) {
                hook.beforeRead(position, 0);
            }
            if (position >= size) {
                return -1;
            }
            final int n = (int) Math.min(len, size - position);
            for (int i = 0; i < n; i++) {
                b[off + i] = (byte) ((position + i) * 31 + 7);
            }
            position += n;
            return n;
        }

        @Override
        public boolean markSupported() {
            return true;
        }

        @Override
        public synchronized void mark(int readlimit) {
            mark = position;
        }

        @Override
        public synchronized void reset() {
            position = mark;
        }
    }

    private byte[] randomData(long size) {
        return randomByteArrayOfLength(Math.toIntExact(size));
    }

    private List<FakeResumableUploadHandler.Call> puts(String name) {
        return fake.getCalls(FakeResumableUploadHandler.Type.PUT).stream().filter(c -> c.name().equals(name)).toList();
    }

    private List<FakeResumableUploadHandler.Call> queries(String name) {
        return fake.getCalls(FakeResumableUploadHandler.Type.QUERY).stream().filter(c -> c.name().equals(name)).toList();
    }

    private static List<long[]> ranges(List<FakeResumableUploadHandler.Call> calls) {
        return calls.stream().map(c -> new long[] { c.from(), c.end() }).toList();
    }

    private static void assertRanges(List<FakeResumableUploadHandler.Call> calls, long... fromEndPairs) {
        final List<String> expected = new ArrayList<>();
        for (int i = 0; i < fromEndPairs.length; i += 2) {
            expected.add("[" + fromEndPairs[i] + "," + fromEndPairs[i + 1] + ")");
        }
        assertThat(ranges(calls).stream().map(r -> "[" + r[0] + "," + r[1] + ")").toList(), equalTo(expected));
    }

    /**
     * The requests that upload a blob of the given size: the parts, and, if the checksum is verified, the last up to 256kb as a request
     * of its own.
     */
    private static List<long[]> expectedRequests(long size, long partSize, boolean verifyChecksum) {
        final long tail = verifyChecksum ? size - ((size - 1) / KB256) * KB256 : 0;
        final long streamedEnd = size - tail;
        final List<long[]> requests = new ArrayList<>();
        for (long start = 0; start < streamedEnd; start += partSize) {
            requests.add(new long[] { start, Math.min(streamedEnd, start + partSize) });
        }
        if (tail > 0) {
            requests.add(new long[] { streamedEnd, size });
        }
        return requests;
    }

    private static String crc32cHeader(byte[] data) {
        final CRC32C crc = new CRC32C();
        crc.update(data);
        return "crc32c="
            + java.util.Base64.getEncoder().encodeToString(java.nio.ByteBuffer.allocate(4).putInt((int) crc.getValue()).array());
    }

    private List<String> hashHeaders(String blob) {
        return fake.getPutHeaders().stream().filter(h -> blob.equals(h.get("x-test-blob"))).map(h -> h.get("x-goog-hash")).toList();
    }

    private GcsRepositoryStatsCollector.StreamingUploadCounters counters() {
        return statsCollector.getStreamingUploadCounters();
    }

    private BlobStoreActionStats insertStats() {
        return statsCollector.operationsStats(false).get(StorageOperation.INSERT.key());
    }

    private long metricSum(String name) {
        return meterRegistry.getRecorder()
            .getMeasurements(InstrumentType.LONG_COUNTER, name)
            .stream()
            .mapToLong(Measurement::getLong)
            .sum();
    }

    // ---- on and off, parts and boundaries ----

    public void testStreamingOffUsesTheSdkPath() throws IOException {
        // the repository's own fixture, which knows how the SDK uploads
        final var fixtureHandler = new GoogleCloudStorageHttpHandler(BUCKET);
        httpServer.removeContext("/");
        httpServer.createContext("/", fixtureHandler);
        final var store = createBlobStore(streamingSettings(0));
        final byte[] data = randomData(randomIntBetween(2000, 100_000));

        write(store, "blob", snapshotSource(data, new AtomicInteger(), null), data.length, false);

        assertThat(fixtureHandler.blobs().get("blob"), equalTo(new BytesArray(data)));
        assertThat(counters().getFiles(), equalTo(0L));
    }

    public void testStreamsInPartsOfTheConfiguredSize() throws IOException {
        final long partSize = KB256 * randomIntBetween(1, 3);
        final boolean verifyChecksum = randomBoolean();
        final var store = createBlobStore(
            streamingSettings(partSize).put(GcsStreamingUploadSettings.VERIFY_CRC32C_SETTING.getKey(), verifyChecksum)
        );
        final long[] sizes = {
            1025, // just over the resumable threshold, a single short part
            partSize - 1,
            partSize,
            partSize + 1,
            2 * partSize - 1,
            2 * partSize,
            2 * partSize + 1,
            3 * partSize,
            randomLongBetween(1025, 4 * partSize) };
        long expectedParts = 0;
        for (int i = 0; i < sizes.length; i++) {
            final long size = sizes[i];
            final String name = "blob-" + i;
            final byte[] data = randomData(size);
            final AtomicInteger resets = new AtomicInteger();
            final InputStream source = randomBoolean() ? snapshotSource(data, resets, null) : new java.io.ByteArrayInputStream(data);

            write(store, name, source, size, randomBoolean());

            assertArrayEquals(name, data, fake.getBlob(name));
            final List<FakeResumableUploadHandler.Call> puts = puts(name);
            final List<long[]> expected = expectedRequests(size, partSize, verifyChecksum);
            assertThat(name, puts.size(), equalTo(expected.size()));
            for (int request = 0; request < expected.size(); request++) {
                assertThat(name, puts.get(request).from(), equalTo(expected.get(request)[0]));
                assertThat(name, puts.get(request).end(), equalTo(expected.get(request)[1]));
                assertThat(name, puts.get(request).total(), equalTo(size));
            }
            final long parts = expected.size();
            assertThat("no retries means the source is never rewound", resets.get(), equalTo(0));
            expectedParts += parts;
        }
        assertThat(fake.getProtocolErrors(), empty());
        assertThat(counters().getFiles(), equalTo((long) sizes.length));
        assertThat(counters().getParts(), equalTo(expectedParts));
        assertThat(counters().getBytes(), equalTo(Arrays.stream(sizes).sum()));
        assertThat(counters().getRetries(), equalTo(0L));
        // the parts have their length declared, so the HTTP client streams them and does not use chunked encoding
        for (var headers : fake.getPutHeaders()) {
            assertThat(headers.get("transfer-encoding"), nullValue());
            assertThat(headers.get("content-length"), notNullValue());
        }
    }

    public void testOnlySnapshotDataIsStreamed() throws IOException {
        final var store = createBlobStore(streamingSettings(KB256));
        for (OperationPurpose purpose : OperationPurpose.values()) {
            final long streamedBefore = counters().getFiles();
            final byte[] data = randomData(randomIntBetween(2000, KB256 * 2));
            final String name = "blob-" + purpose.getKey();

            store.writeBlob(purpose, name, new java.io.ByteArrayInputStream(data), data.length, false);

            assertArrayEquals(name, data, fake.getBlob(name));
            assertThat(name, counters().getFiles() - streamedBefore, equalTo(purpose == OperationPurpose.SNAPSHOT_DATA ? 1L : 0L));
        }
    }

    public void testChecksumOfTheBlobIsSentOnTheLastRequest() throws IOException {
        final long partSize = 2L * KB256;
        final var store = createBlobStore(streamingSettings(partSize).put(GcsStreamingUploadSettings.VERIFY_CRC32C_SETTING.getKey(), true));
        final byte[] data = randomData(
            new long[] { 5 * partSize, 5 * partSize + 1, 5 * partSize - 1, KB256, KB256 / 2, 1025 }[between(0, 5)]
        );
        final AtomicBoolean done = new AtomicBoolean();
        // the bytes of the first part are sent twice, to show that the checksum is not thrown off by reading them again
        fake.faults = call -> call.type() == FakeResumableUploadHandler.Type.PUT && call.from() == 0 && done.compareAndSet(false, true)
            ? FakeResumableUploadHandler.Fault.commitThenStatus(KB256, 503)
            : FakeResumableUploadHandler.Fault.NONE;

        write(store, "blob", snapshotSource(data, new AtomicInteger(), null), data.length, false);

        // the fake refuses an object whose checksum is not the one in the header, so this is the checksum of the whole blob
        assertArrayEquals(data, fake.getBlob("blob"));
        assertThat(fake.getProtocolErrors(), empty());
        final List<String> hashes = hashHeaders("blob");
        assertThat(hashes.get(hashes.size() - 1), equalTo(crc32cHeader(data)));
        for (String hash : hashes.subList(0, hashes.size() - 1)) {
            assertThat("only the last request carries the checksum", hash, nullValue());
        }
        // the last request is at most 256kb, the part before it ends on a multiple of 256kb
        final var puts = puts("blob");
        assertThat(puts.get(puts.size() - 1).length() <= KB256, equalTo(true));
    }

    public void testChecksumCanBeSwitchedOff() throws IOException {
        final long partSize = 2L * KB256;
        final var store = createBlobStore(
            streamingSettings(partSize).put(GcsStreamingUploadSettings.VERIFY_CRC32C_SETTING.getKey(), false)
        );
        final byte[] data = randomData(5 * partSize + 3);

        write(store, "blob", snapshotSource(data, new AtomicInteger(), null), data.length, false);

        assertArrayEquals(data, fake.getBlob("blob"));
        assertRanges(
            puts("blob"),
            0,
            partSize,
            partSize,
            2 * partSize,
            2 * partSize,
            3 * partSize,
            3 * partSize,
            4 * partSize,
            4 * partSize,
            5 * partSize,
            5 * partSize,
            5 * partSize + 3
        );
        assertThat(hashHeaders("blob"), equalTo(Arrays.asList(new String[6])));
    }

    public void testPartSizeIsReadAtWriteTime() throws IOException {
        final var store = createBlobStore(streamingSettings(0));
        final byte[] data = randomData(KB256 * 3);

        write(store, "off", snapshotSource(data, new AtomicInteger(), null), data.length, false);
        assertThat(counters().getFiles(), equalTo(0L));

        clusterService.getClusterSettings()
            .applySettings(Settings.builder().put(GcsStreamingUploadSettings.PART_SIZE_SETTING.getKey(), KB256 + "b").build());
        write(store, "on", snapshotSource(data, new AtomicInteger(), null), data.length, false);
        assertThat(counters().getFiles(), equalTo(1L));
        assertThat(counters().getParts(), equalTo(3L));

        clusterService.getClusterSettings()
            .applySettings(Settings.builder().put(GcsStreamingUploadSettings.PART_SIZE_SETTING.getKey(), "0").build());
        write(store, "off-again", snapshotSource(data, new AtomicInteger(), null), data.length, false);
        assertThat(counters().getFiles(), equalTo(1L));
    }

    // ---- session options ----

    public void testFailIfAlreadyExists() throws IOException {
        final var store = createBlobStore(streamingSettings(KB256));
        final byte[] data = randomData(KB256 * 2 + 10);
        fake.putBlob("existing", new byte[] { 1 });

        expectThrows(
            FileAlreadyExistsException.class,
            () -> write(store, "existing", snapshotSource(data, new AtomicInteger(), null), data.length, true)
        );
        assertThat(fake.getOpenQueries().getLast(), containsString("ifGenerationMatch=0"));
        assertThat(puts("existing"), empty());
        assertArrayEquals(new byte[] { 1 }, fake.getBlob("existing"));

        // another writer wins while the parts are on their way: the last part is refused
        fake.faults = call -> call.type() == FakeResumableUploadHandler.Type.PUT && call.end() == call.total()
            ? FakeResumableUploadHandler.Fault.status(412)
            : FakeResumableUploadHandler.Fault.NONE;
        expectThrows(
            FileAlreadyExistsException.class,
            () -> write(store, "raced", snapshotSource(data, new AtomicInteger(), null), data.length, true)
        );
        // without failIfAlreadyExists a 412 is not about existence and is just an error
        final var e = expectThrows(
            com.google.cloud.storage.StorageException.class,
            () -> write(store, "raced-overwrite", snapshotSource(data, new AtomicInteger(), null), data.length, false)
        );
        assertThat(e.getCode(), equalTo(412));
        assertThat(fake.getOpenQueries().getLast(), not(containsString("ifGenerationMatch")));
        fake.faults = call -> FakeResumableUploadHandler.Fault.NONE;

        // overwrite is allowed without the flag, with the flag a new blob is written
        write(store, "existing", snapshotSource(data, new AtomicInteger(), null), data.length, false);
        assertArrayEquals(data, fake.getBlob("existing"));
        write(store, "fresh", snapshotSource(data, new AtomicInteger(), null), data.length, true);
        assertArrayEquals(data, fake.getBlob("fresh"));
        assertThat(fake.getOpenQueries().getLast(), containsString("ifGenerationMatch=0"));
        assertThat(fake.getProtocolErrors(), empty());
    }

    public void testSessionIsOpenedWithTheMetadataOfTheBlob() throws IOException {
        final var store = createBlobStore(streamingSettings(KB256), null, "NEARLINE");
        final byte[] data = randomData(KB256 * 2 + 5);

        // a blob written from bytes has its md5 computed up front
        store.writeBlob(OperationPurpose.SNAPSHOT_DATA, "with-md5", new BytesArray(data), false);
        // one written from a stream does not
        write(store, "no-md5", snapshotSource(data, new AtomicInteger(), null), data.length, false);

        final var md5 = Map.<String, Object>of(
            "name",
            "with-md5",
            "bucket",
            BUCKET,
            "storageClass",
            "NEARLINE",
            "md5Hash",
            java.util.Base64.getEncoder().encodeToString(MessageDigests.digest(new BytesArray(data), MessageDigests.md5()))
        );
        assertThat(parseJson(fake.getOpenBodies().get(0)), equalTo(md5));
        assertThat(
            parseJson(fake.getOpenBodies().get(1)),
            equalTo(Map.<String, Object>of("name", "no-md5", "bucket", BUCKET, "storageClass", "NEARLINE"))
        );
        assertThat(fake.getOpenQueries().get(0), containsString("uploadType=resumable"));
        assertThat(fake.getOpenQueries().get(0), containsString("name=with-md5"));
        assertArrayEquals(data, fake.getBlob("with-md5"));
        assertArrayEquals(data, fake.getBlob("no-md5"));
        assertThat(counters().getFiles(), equalTo(2L));
    }

    private static Map<String, Object> parseJson(String json) {
        return org.elasticsearch.common.xcontent.XContentHelper.convertToMap(new BytesArray(json), false, XContentType.JSON).v2();
    }

    // ---- retry and resume ----

    public void testMidPartTimeoutResumesFromTheCommittedOffset() throws IOException {
        final long partSize = 4L * KB256;
        final long committed = KB256;
        final var store = createBlobStore(streamingSettings(partSize), TimeValue.timeValueSeconds(1), null);
        final byte[] data = randomData(3 * partSize);
        final AtomicInteger resets = new AtomicInteger();
        final AtomicBoolean timedOut = new AtomicBoolean();
        final CountDownLatch release = newLatch();
        // the server has committed a quarter of the second part when it stops answering, so the client times out waiting
        fake.faults = call -> call.type() == FakeResumableUploadHandler.Type.PUT
            && call.from() == partSize
            && timedOut.compareAndSet(false, true)
                ? FakeResumableUploadHandler.Fault.commitThenHang(committed, release)
                : FakeResumableUploadHandler.Fault.NONE;

        Loggers.setLevel(GcsStreamingUpload.logger, Level.DEBUG);
        try {
            MockLog.assertThatLogger(() -> {
                try {
                    write(store, "blob", snapshotSource(data, resets, null), data.length, false);
                } catch (IOException e) {
                    throw new AssertionError(e);
                }
            },
                DEBUG_LOGGER,
                new MockLog.PatternSeenEventExpectation(
                    "resume logged at info",
                    DEBUG_LOGGER,
                    Level.INFO,
                    "retrying upload of \\[bucket/blob\\] part \\[1\\].*"
                ),
                new MockLog.PatternSeenEventExpectation(
                    "resume from committed offset",
                    DEBUG_LOGGER,
                    Level.INFO,
                    "resuming upload of \\[bucket/blob\\] part \\[1\\] .* from committed offset \\[" + (partSize + committed) + "\\].*"
                ),
                new MockLog.PatternSeenEventExpectation(
                    "part timing logged at debug",
                    DEBUG_LOGGER,
                    Level.DEBUG,
                    "part \\[0\\] of \\[bucket/blob\\] range .* source_read_ms \\[\\d+\\] socket_write_ms \\[\\d+\\] "
                        + "response_wait_ms \\[\\d+\\].* attempt \\[1\\].*"
                ),
                new MockLog.PatternSeenEventExpectation(
                    "connection rate of a part logged at debug",
                    DEBUG_LOGGER,
                    Level.DEBUG,
                    "part \\[0\\] of \\[bucket/blob\\] range .* request_ms \\[\\d+\\] connection_rate_mib_s \\[(\\d+\\.\\d|n/a)\\] "
                        + "socket_write_rate_mib_s \\[(\\d+\\.\\d|n/a)\\] "
                        + "blocked_write_rate_mib_s \\[(\\d+\\.\\d|n/a)\\] connection unknown"
                ),
                new MockLog.PatternSeenEventExpectation(
                    "file timing logged at debug",
                    DEBUG_LOGGER,
                    Level.DEBUG,
                    "uploaded \\[bucket/blob\\] bytes \\[" + data.length + "\\] in \\[4\\] parts, total_ms \\[\\d+\\]"
                )
            );
        } finally {
            Loggers.setLevel(GcsStreamingUpload.logger, (Level) null);
        }

        assertArrayEquals(data, fake.getBlob("blob"));
        // three parts and then the last 256kb on its own, with the part that timed out sent again from the offset the server had reached
        assertRanges(
            puts("blob"),
            0,
            partSize,
            partSize,
            2 * partSize,
            partSize + committed,
            2 * partSize,
            2 * partSize,
            3 * partSize - KB256,
            3 * partSize - KB256,
            3 * partSize
        );
        assertThat(hashHeaders("blob").getLast(), equalTo(crc32cHeader(data)));
        assertThat(queries("blob").size(), equalTo(1));
        assertThat("the source was rewound to the mark of the part", resets.get(), equalTo(1));
        assertThat(counters().getRetries(), equalTo(1L));
        assertThat(counters().getResumedBytes(), equalTo(committed));
        assertThat(fake.getProtocolErrors(), empty());
    }

    public void testPartialCommitContinuesFromTheCommittedOffset() throws IOException {
        final long partSize = 4L * KB256;
        final long committed = KB256 * between(0, 3);
        final var store = createBlobStore(streamingSettings(partSize));
        final byte[] data = randomData(2 * partSize + 100);
        final AtomicInteger resets = new AtomicInteger();
        final AtomicBoolean done = new AtomicBoolean();
        // the server keeps only some of the first part but answers with a 308 as usual
        fake.faults = call -> call.type() == FakeResumableUploadHandler.Type.PUT && done.compareAndSet(false, true)
            ? FakeResumableUploadHandler.Fault.partialCommit(committed)
            : FakeResumableUploadHandler.Fault.NONE;

        write(store, "blob", snapshotSource(data, resets, null), data.length, false);

        assertArrayEquals(data, fake.getBlob("blob"));
        if (committed == 0) {
            // a 308 without progress counts as a failure, and is resolved by asking the session where it is
            assertRanges(puts("blob"), 0, partSize, 0, partSize, partSize, 2 * partSize, 2 * partSize, 2 * partSize + 100);
            assertThat(queries("blob").size(), equalTo(1));
        } else {
            assertRanges(puts("blob"), 0, partSize, committed, partSize, partSize, 2 * partSize, 2 * partSize, 2 * partSize + 100);
            assertThat(queries("blob"), empty());
            assertThat(counters().getPartialCommits(), equalTo(1L));
            assertThat(counters().getResumedBytes(), equalTo(committed));
        }
        assertThat("the source was rewound once and read ahead to the committed offset", resets.get(), equalTo(1));
        assertThat(counters().getRetries(), equalTo(1L));
        assertThat(fake.getProtocolErrors(), empty());
    }

    public void testServerErrorsAreRetriedAndTheUploadResumes() throws IOException {
        final long partSize = 2L * KB256;
        final var store = createBlobStore(streamingSettings(partSize));
        final byte[] data = randomData(3 * partSize + 1);
        final var fault = randomFrom(
            FakeResumableUploadHandler.Fault.status(500),
            FakeResumableUploadHandler.Fault.status(503),
            FakeResumableUploadHandler.Fault.status(429),
            FakeResumableUploadHandler.Fault.status(408),
            FakeResumableUploadHandler.Fault.commitThenStatus(KB256, 503),
            FakeResumableUploadHandler.Fault.commitThenDrop(0),
            FakeResumableUploadHandler.Fault.commitThenDrop(KB256)
        );
        final long failingFrom = partSize * between(0, 3);
        final AtomicBoolean done = new AtomicBoolean();
        fake.faults = call -> call.type() == FakeResumableUploadHandler.Type.PUT
            && call.from() == failingFrom
            && done.compareAndSet(false, true) ? fault : FakeResumableUploadHandler.Fault.NONE;

        write(store, "blob", snapshotSource(data, new AtomicInteger(), null), data.length, false);

        assertArrayEquals(data, fake.getBlob("blob"));
        assertThat(queries("blob").size(), equalTo(1));
        assertThat(counters().getRetries(), equalTo(1L));
        assertThat(fake.getProtocolErrors(), empty());
        // the part is sent again from wherever the server got to, and the rest follows in order
        final var puts = puts("blob");
        final long resumeFrom = failingFrom + (fault.commit() > 0 ? fault.commit() : 0);
        assertThat(puts.get(puts.size() - 1).end(), equalTo((long) data.length));
        if (resumeFrom < data.length) {
            assertThat(puts.stream().filter(c -> c.from() == resumeFrom).count(), greaterThan(0L));
        }
    }

    public void testFailuresBetweenSuccessfulPartsDoNotAddUp() throws IOException {
        // every other request fails, which is more than the attempts allowed if they were counted over the whole blob
        final var store = createBlobStore(streamingSettings(KB256).put(GcsStreamingUploadSettings.MAX_ATTEMPTS_SETTING.getKey(), 2));
        final byte[] data = randomData(20 * KB256 + 1);
        final AtomicInteger requests = new AtomicInteger();
        fake.faults = call -> call.type() == FakeResumableUploadHandler.Type.PUT && requests.getAndIncrement() % 2 == 0
            ? FakeResumableUploadHandler.Fault.status(503)
            : FakeResumableUploadHandler.Fault.NONE;

        write(store, "blob", snapshotSource(data, new AtomicInteger(), null), data.length, false);

        assertArrayEquals(data, fake.getBlob("blob"));
        assertThat(counters().getRetries(), greaterThan(10L));
    }

    public void testFailureToOpenTheSessionIsRetried() throws IOException {
        final var store = createBlobStore(streamingSettings(KB256));
        final byte[] data = randomData(KB256 + 1);
        final AtomicInteger failures = new AtomicInteger(between(1, 3));
        fake.faults = call -> call.type() == FakeResumableUploadHandler.Type.OPEN && failures.getAndDecrement() > 0
            ? FakeResumableUploadHandler.Fault.status(503)
            : FakeResumableUploadHandler.Fault.NONE;

        write(store, "blob", snapshotSource(data, new AtomicInteger(), null), data.length, false);

        assertArrayEquals(data, fake.getBlob("blob"));
        assertThat(counters().getRetries(), greaterThan(0L));
    }

    public void testLostResponseToTheLastPartIsResolvedByTheSession() throws IOException {
        final long partSize = KB256;
        final var store = createBlobStore(streamingSettings(partSize));
        final byte[] data = randomData(2 * partSize);
        final AtomicBoolean done = new AtomicBoolean();
        // everything is committed, and the object finalized, but the answer never arrives
        fake.faults = call -> call.type() == FakeResumableUploadHandler.Type.PUT
            && call.end() == call.total()
            && done.compareAndSet(false, true)
                ? FakeResumableUploadHandler.Fault.commitThenDrop(-1)
                : FakeResumableUploadHandler.Fault.NONE;

        write(store, "blob", snapshotSource(data, new AtomicInteger(), null), data.length, false);

        assertArrayEquals(data, fake.getBlob("blob"));
        assertRanges(puts("blob"), 0, partSize, partSize, 2 * partSize);
        assertThat(queries("blob").size(), equalTo(1));
    }

    public void testSessionGoneFailsTheWriteAndIsCounted() {
        final long partSize = KB256;
        final var store = createBlobStore(streamingSettings(partSize));
        final byte[] data = randomData(3 * partSize);
        fake.faults = call -> call.type() == FakeResumableUploadHandler.Type.PUT && call.from() == partSize
            ? FakeResumableUploadHandler.Fault.status(410)
            : FakeResumableUploadHandler.Fault.NONE;

        final var e = expectThrows(
            com.google.cloud.storage.StorageException.class,
            () -> write(store, "blob", snapshotSource(data, new AtomicInteger(), null), data.length, false)
        );

        assertThat(e.getCode(), equalTo(410));
        assertThat(e.getMessage(), containsString("session is gone"));
        assertThat(e.getMessage(), containsString("failing the write"));
        assertThat("not retried", puts("blob").size(), equalTo(2));
        assertThat(queries("blob"), empty());
        assertThat(counters().getSessionGone(), equalTo(1L));
        // open, two parts, and the write is an unsuccessful operation with an error among its requests
        assertThat(insertStats(), equalTo(new BlobStoreActionStats(0, 3)));
        assertThat(metricSum(RepositoriesMetrics.METRIC_EXCEPTIONS_TOTAL), equalTo(1L));
        assertThat(metricSum(RepositoriesMetrics.METRIC_UNSUCCESSFUL_OPERATIONS_TOTAL), equalTo(1L));
    }

    public void testRetriesAreBounded() {
        final int maxAttempts = between(2, 4);
        final var store = createBlobStore(
            streamingSettings(KB256).put(GcsStreamingUploadSettings.MAX_ATTEMPTS_SETTING.getKey(), maxAttempts)
        );
        final byte[] data = randomData(2 * KB256);
        final boolean status = randomBoolean();
        fake.faults = call -> call.type() == FakeResumableUploadHandler.Type.OPEN
            ? FakeResumableUploadHandler.Fault.NONE
            : (status ? FakeResumableUploadHandler.Fault.status(503) : FakeResumableUploadHandler.Fault.commitThenDrop(0));

        final Exception e = expectThrows(
            Exception.class,
            () -> write(store, "blob", snapshotSource(data, new AtomicInteger(), null), data.length, false)
        );

        if (status) {
            assertThat(e, org.hamcrest.Matchers.instanceOf(com.google.cloud.storage.StorageException.class));
            assertThat(((com.google.cloud.storage.StorageException) e).getCode(), equalTo(503));
        } else {
            assertThat(e, org.hamcrest.Matchers.instanceOf(IOException.class));
            assertThat(e.getCause(), notNullValue());
        }
        assertThat(e.getMessage(), containsString("giving up after [" + maxAttempts + "] consecutive failures"));
        assertThat(
            fake.getCalls().stream().filter(c -> c.type() != FakeResumableUploadHandler.Type.OPEN).count(),
            equalTo((long) maxAttempts)
        );
        assertThat(fake.hasBlob("blob"), equalTo(false));
    }

    public void testRetryTimeIsBounded() {
        final var store = createBlobStore(
            streamingSettings(KB256).put(GcsStreamingUploadSettings.MAX_ATTEMPTS_SETTING.getKey(), 1000)
                .put(GcsStreamingUploadSettings.MAX_RETRY_TIME_SETTING.getKey(), "50ms")
                .put(GcsStreamingUploadSettings.INITIAL_BACKOFF_SETTING.getKey(), "20ms")
                .put(GcsStreamingUploadSettings.MAX_BACKOFF_SETTING.getKey(), "20ms")
        );
        final byte[] data = randomData(2 * KB256);
        fake.faults = call -> call.type() == FakeResumableUploadHandler.Type.OPEN
            ? FakeResumableUploadHandler.Fault.NONE
            : FakeResumableUploadHandler.Fault.status(503);

        final var e = expectThrows(
            com.google.cloud.storage.StorageException.class,
            () -> write(store, "blob", snapshotSource(data, new AtomicInteger(), null), data.length, false)
        );

        assertThat(e.getMessage(), containsString("giving up after"));
        assertThat(fake.getCalls().size(), lessThan(100));
    }

    public void testClientErrorsAreNotRetried() {
        final var store = createBlobStore(streamingSettings(KB256));
        final byte[] data = randomData(2 * KB256);
        final int status = randomFrom(400, 401, 403, 404);
        fake.faults = call -> call.type() == FakeResumableUploadHandler.Type.PUT
            ? FakeResumableUploadHandler.Fault.status(status)
            : FakeResumableUploadHandler.Fault.NONE;

        final var e = expectThrows(
            com.google.cloud.storage.StorageException.class,
            () -> write(store, "blob", snapshotSource(data, new AtomicInteger(), null), data.length, false)
        );

        assertThat(e.getCode(), equalTo(status));
        assertThat(puts("blob").size(), equalTo(1));
        assertThat(queries("blob"), empty());
    }

    /**
     * The source of a real snapshot: the file is read through Store.VerifyingIndexInput, which checks the checksum in the footer of the
     * file as it is read, and the snapshot verifies it after all the parts were written. The bytes that a retry reads again must not be
     * counted twice, which would fail the verification of a snapshot that was written correctly.
     */
    public void testSnapshotVerificationHoldsAfterBytesWereReadAgain() throws IOException {
        final long partSize = 2L * KB256;
        final var store = createBlobStore(streamingSettings(partSize), TimeValue.timeValueSeconds(1), null);
        final CountDownLatch release = newLatch();
        final AtomicInteger failures = new AtomicInteger();
        // a partial commit, a failure after some bytes were committed, and a timeout, each of which makes the source read bytes again
        fake.faults = call -> {
            if (call.type() != FakeResumableUploadHandler.Type.PUT) {
                return FakeResumableUploadHandler.Fault.NONE;
            }
            return switch (failures.getAndIncrement()) {
                case 0 -> FakeResumableUploadHandler.Fault.partialCommit(KB256);
                case 1 -> FakeResumableUploadHandler.Fault.commitThenStatus(KB256, 503);
                case 2 -> FakeResumableUploadHandler.Fault.commitThenHang(KB256, release);
                default -> FakeResumableUploadHandler.Fault.NONE;
            };
        };
        try (var directory = new org.apache.lucene.store.ByteBuffersDirectory()) {
            final byte[] content = randomData(7 * KB256 + 123);
            try (var output = directory.createOutput("file", org.apache.lucene.store.IOContext.DEFAULT)) {
                output.writeBytes(content, content.length);
                org.apache.lucene.codecs.CodecUtil.writeFooter(output);
            }
            final byte[] fileBytes;
            try (var input = directory.openInput("file", org.apache.lucene.store.IOContext.DEFAULT)) {
                fileBytes = new byte[Math.toIntExact(input.length())];
                input.readBytes(fileBytes, 0, fileBytes.length);
            }
            final AtomicInteger resets = new AtomicInteger();
            try (
                var verifying = new org.elasticsearch.index.store.Store.VerifyingIndexInput(
                    directory.openInput("file", org.apache.lucene.store.IOContext.READONCE)
                ) {
                    @Override
                    public void seek(long pos) throws IOException {
                        resets.incrementAndGet();
                        super.seek(pos);
                    }
                }
            ) {
                final InputStream source = new HookedInputStream(
                    new RateLimitingInputStream(
                        new InputStreamIndexInput(verifying, fileBytes.length),
                        () -> new RateLimiter.SimpleRateLimiter(100_000),
                        nanos -> {}
                    ),
                    null
                );

                write(store, "blob", source, fileBytes.length, false);

                assertArrayEquals(fileBytes, fake.getBlob("blob"));
                assertThat("the source was read again", resets.get(), greaterThan(0));
                assertThat(counters().getRetries(), greaterThan(0L));
                // throws a CorruptIndexException if the checksum of what was read the first time is not that of the footer
                org.elasticsearch.index.store.Store.verify(verifying);
            }
        }
    }

    // ---- fallback to the SDK ----

    public void testFallbackToTheSdkIsLogged() throws IOException {
        // a storage client that cannot stream, which makes the upload fall back to the SDK
        clientWrapper = real -> new MeteredStorage(real.getOptions().getService(), statsCollector) {
            @Override
            boolean supportsStreamingUpload() {
                return false;
            }
        };
        final var store = createBlobStore(streamingSettings(KB256));
        final byte[] data = randomData(2 * KB256);
        final String logger = GoogleCloudStorageBlobStore.class.getName();

        MockLog.assertThatLogger(() -> {
            try {
                write(store, "fallback", snapshotSource(data, new AtomicInteger(), null), data.length, false);
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        },
            logger,
            new MockLog.SeenEventExpectation(
                "the fallback and its reason are logged",
                logger,
                Level.INFO,
                "streaming upload is enabled but blob [bucket/fallback] is uploaded through the SDK instead: "
                    + "the storage client does not use the HTTP transport of the SDK"
            )
        );

        assertArrayEquals(data, fake.getBlob("fallback"));
        assertThat(counters().getFiles(), equalTo(0L));

        // and nothing is logged when streaming is not enabled, or when it is used
        final var off = createBlobStore(streamingSettings(0));
        MockLog.assertThatLogger(() -> {
            try {
                write(off, "off", snapshotSource(data, new AtomicInteger(), null), data.length, false);
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }, logger, new MockLog.PatternNotSeenEventExpectation("no fallback when off", logger, Level.INFO, ".*uploaded through the SDK.*"));
        clientWrapper = null;
        final var on = createBlobStore(streamingSettings(KB256));
        MockLog.assertThatLogger(() -> {
            try {
                write(on, "on", snapshotSource(data, new AtomicInteger(), null), data.length, false);
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        },
            logger,
            new MockLog.PatternNotSeenEventExpectation("no fallback when streamed", logger, Level.INFO, ".*uploaded through the SDK.*")
        );
        assertThat(counters().getFiles(), equalTo(1L));
    }

    public void testReasonWhyStreamingIsUnavailable() {
        final var store = createBlobStore(streamingSettings(KB256));
        final MeteredStorage client;
        try {
            client = store.client();
        } catch (IOException e) {
            throw new AssertionError(e);
        }
        assertTrue(client.supportsStreamingUpload());
        assertThat(
            GoogleCloudStorageBlobStore.getStreamingUploadUnavailableReason(new java.io.ByteArrayInputStream(new byte[1]), client),
            org.hamcrest.Matchers.nullValue()
        );
        final InputStream noMark = new FilterInputStream(new java.io.ByteArrayInputStream(new byte[1])) {
            @Override
            public boolean markSupported() {
                return false;
            }
        };
        assertThat(
            GoogleCloudStorageBlobStore.getStreamingUploadUnavailableReason(noMark, client),
            equalTo("the source does not support mark and reset")
        );
    }

    // ---- timing of the SDK path ----

    private static final String TIMING_LOGGER = "org.elasticsearch.repositories.gcs.ResumableTiming";

    private void writeThroughTheSdk(String name, byte[] data) {
        writeThroughTheSdk(OperationPurpose.SNAPSHOT_DATA, name, data);
    }

    private void writeThroughTheSdk(OperationPurpose purpose, String name, byte[] data) {
        try {
            createBlobStoreForSdk().writeBlob(purpose, name, snapshotSource(data, new AtomicInteger(), null), data.length, false);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private GoogleCloudStorageBlobStore sdkStore;

    private GoogleCloudStorageBlobStore createBlobStoreForSdk() {
        if (sdkStore == null) {
            sdkChunkSize = ByteSizeValue.ofKb(256);
            sdkStore = createBlobStore(streamingSettings(0));
        }
        return sdkStore;
    }

    private List<String> callShapes(String name) {
        return fake.getCalls()
            .stream()
            .filter(c -> name.equals(c.name()))
            .map(c -> c.type() + "[" + c.from() + "," + c.end() + ")")
            .toList();
    }

    public void testSdkPathTimingIsLoggedAtDebug() {
        final byte[] data = randomData(3 * KB256 + 100);
        Loggers.setLevel(ResumableTiming.logger, Level.DEBUG);
        try {
            MockLog.assertThatLogger(
                () -> writeThroughTheSdk("blob", data),
                TIMING_LOGGER,
                new MockLog.PatternSeenEventExpectation(
                    "a full chunk is flushed",
                    TIMING_LOGGER,
                    Level.DEBUG,
                    "piece \\[\\d+\\] of \\[bucket/blob\\] purpose \\[SNAPSHOT_DATA\\] end_offset \\[\\d+\\] "
                        + "flushed_bytes \\[262144\\] flush_ms \\[\\d+\\] source_read_ms \\[\\d+\\] reads \\[\\d+\\]"
                ),
                new MockLog.PatternSeenEventExpectation(
                    "the last chunk is sent when the channel is closed",
                    TIMING_LOGGER,
                    Level.DEBUG,
                    "piece \\[\\d+\\] of \\[bucket/blob\\] purpose \\[SNAPSHOT_DATA\\] last end_offset \\["
                        + data.length
                        + "\\] flushed_bytes \\[100\\] flush_ms \\[\\d+\\] "
                        + "source_read_ms \\[\\d+\\] reads \\[\\d+\\]"
                ),
                new MockLog.PatternSeenEventExpectation(
                    "totals",
                    TIMING_LOGGER,
                    Level.DEBUG,
                    "uploaded \\[bucket/blob\\] purpose \\[SNAPSHOT_DATA\\] bytes \\["
                        + data.length
                        + "\\] pieces \\[\\d+\\] total_ms \\[\\d+\\] source_read_ms \\[\\d+\\] "
                        + "reads \\[\\d+\\] flush_ms \\[\\d+\\]"
                )
            );
        } finally {
            Loggers.setLevel(ResumableTiming.logger, (Level) null);
        }
        assertArrayEquals(data, fake.getBlob("blob"));
        assertThat("the streaming upload is not involved", counters().getFiles(), equalTo(0L));
    }

    public void testSdkPathTimingTellsThePurposeApart() {
        // the same blob store also uploads other things, such as the commits of stateless shards
        final byte[] data = randomData(3 * KB256 + 100);
        final OperationPurpose other = randomValueOtherThan(OperationPurpose.SNAPSHOT_DATA, () -> randomFrom(OperationPurpose.values()));
        Loggers.setLevel(ResumableTiming.logger, Level.DEBUG);
        try {
            MockLog.assertThatLogger(
                () -> writeThroughTheSdk(other, "other", data),
                TIMING_LOGGER,
                new MockLog.PatternSeenEventExpectation(
                    "a piece names the purpose",
                    TIMING_LOGGER,
                    Level.DEBUG,
                    "piece \\[\\d+\\] of \\[bucket/other\\] purpose \\[" + other + "\\] .*"
                ),
                new MockLog.PatternSeenEventExpectation(
                    "the totals name the purpose",
                    TIMING_LOGGER,
                    Level.DEBUG,
                    "uploaded \\[bucket/other\\] purpose \\[" + other + "\\] bytes .*"
                ),
                new MockLog.PatternNotSeenEventExpectation(
                    "and not that of snapshot data",
                    TIMING_LOGGER,
                    Level.DEBUG,
                    ".*purpose \\[SNAPSHOT_DATA\\].*"
                )
            );
        } finally {
            Loggers.setLevel(ResumableTiming.logger, (Level) null);
        }
    }

    public void testSdkPathTimingChangesNothingAtInfo() {
        final byte[] data = randomData(3 * KB256 + 100);
        Loggers.setLevel(ResumableTiming.logger, Level.INFO);
        try {
            assertFalse(ResumableTiming.isEnabled());
            MockLog.assertThatLogger(
                () -> writeThroughTheSdk("info", data),
                TIMING_LOGGER,
                new MockLog.PatternNotSeenEventExpectation("nothing at debug", TIMING_LOGGER, Level.DEBUG, ".*"),
                new MockLog.PatternNotSeenEventExpectation("nothing at info", TIMING_LOGGER, Level.INFO, ".*")
            );
            Loggers.setLevel(ResumableTiming.logger, Level.DEBUG);
            assertTrue(ResumableTiming.isEnabled());
            writeThroughTheSdk("debug", data);
        } finally {
            Loggers.setLevel(ResumableTiming.logger, (Level) null);
        }
        // the same blob is written, with the same requests, whether the timing is logged or not
        assertArrayEquals(data, fake.getBlob("info"));
        assertArrayEquals(data, fake.getBlob("debug"));
        assertThat(callShapes("debug"), equalTo(callShapes("info")));
    }

    // ---- exceptions of the source ----

    public void testIoExceptionOfTheSourceIsRethrownAndNotRetried() {
        final long partSize = 2L * KB256;
        final var store = createBlobStore(streamingSettings(partSize));
        final byte[] data = randomData(3 * partSize);
        final IOException failure = new IOException("source failed");
        // in the second part, after some of its bytes were on their way
        final long failAt = partSize + KB256;
        final var source = snapshotSource(data, new AtomicInteger(), (offset, resets) -> {
            if (offset >= failAt) {
                throw failure;
            }
        });

        final IOException thrown = expectThrows(IOException.class, () -> write(store, "blob", source, data.length, false));

        assertSame(failure, thrown);
        assertThat(queries("blob"), empty());
        assertThat(counters().getRetries(), equalTo(0L));
        assertThat("the part that was being sent is the last request", puts("blob").size(), equalTo(2));
        assertThat(fake.hasBlob("blob"), equalTo(false));
        assertThat(insertStats().operations(), equalTo(0L));
    }

    public void testRuntimeExceptionOfTheSourceIsRethrownAndNotRetried() {
        // a snapshot that is aborted, or a node that is shutting down, makes the source throw like this
        final var store = createBlobStore(streamingSettings(2L * KB256));
        final byte[] data = randomData(6 * KB256);
        final IllegalStateException failure = new IllegalStateException("snapshot aborted");
        final var source = snapshotSource(data, new AtomicInteger(), (offset, resets) -> {
            if (offset >= 3 * KB256) {
                throw failure;
            }
        });

        final var thrown = expectThrows(IllegalStateException.class, () -> write(store, "blob", source, data.length, false));

        assertSame(failure, thrown);
        assertThat(queries("blob"), empty());
        assertThat(counters().getRetries(), equalTo(0L));
        assertThat(puts("blob").size(), equalTo(2));
    }

    public void testSourceExceptionWhileResumingIsNotRetried() {
        final long partSize = 2L * KB256;
        final var store = createBlobStore(streamingSettings(partSize));
        final byte[] data = randomData(4 * KB256);
        final AtomicBoolean done = new AtomicBoolean();
        // the destination fails once, and then the source fails while it is read up to the committed offset
        fake.faults = call -> call.type() == FakeResumableUploadHandler.Type.PUT && done.compareAndSet(false, true)
            ? FakeResumableUploadHandler.Fault.commitThenStatus(KB256, 503)
            : FakeResumableUploadHandler.Fault.NONE;
        final IOException failure = new IOException("source failed while rewinding");
        final var source = snapshotSource(data, new AtomicInteger(), (offset, resets) -> {
            if (resets > 0) {
                throw failure;
            }
        });

        final IOException thrown = expectThrows(IOException.class, () -> write(store, "blob", source, data.length, false));

        assertSame(failure, thrown);
        assertThat("only the part that failed was sent", puts("blob").size(), equalTo(1));
        assertThat(queries("blob").size(), equalTo(1));
    }

    // ---- write stall ----

    public void testStalledWriteIsDisconnectedAndResumed() throws IOException {
        // the server stops reading, so once the socket buffers are full the client blocks in write, which HttpURLConnection never times out
        final long size = 128L * 1024 * 1024;
        final var store = createBlobStore(
            streamingSettings(size).put(GcsStreamingUploadSettings.WRITE_STALL_TIMEOUT_SETTING.getKey(), "300ms")
        );
        fake.keepData = false;
        final CountDownLatch release = newLatch();
        final AtomicBoolean done = new AtomicBoolean();
        fake.faults = call -> call.type() == FakeResumableUploadHandler.Type.PUT && done.compareAndSet(false, true)
            ? FakeResumableUploadHandler.Fault.stopReading(release)
            : FakeResumableUploadHandler.Fault.NONE;

        write(store, "blob", new PatternInputStream(size, null), size, false);

        assertThat(fake.getBlobLength("blob"), equalTo(size));
        assertThat(counters().getWriteStalls(), equalTo(1L));
        assertThat(counters().getRetries(), equalTo(1L));
        // the part that stalled is sent again from the start, and then the last 256kb
        assertRanges(puts("blob"), 0, size - KB256, 0, size - KB256, size - KB256, size);
        assertThat(queries("blob").size(), equalTo(1));
    }

    public void testSlowSourceIsNotAStall() throws IOException {
        // time spent reading the source, for example paused by the snapshot rate limiter, is not a stalled write
        final var store = createBlobStore(
            streamingSettings(4L * KB256).put(GcsStreamingUploadSettings.WRITE_STALL_TIMEOUT_SETTING.getKey(), "300ms")
        );
        final byte[] data = randomData(3 * KB256);
        final AtomicBoolean paused = new AtomicBoolean();
        final var source = snapshotSource(data, new AtomicInteger(), (offset, resets) -> {
            if (offset >= KB256 && paused.compareAndSet(false, true)) {
                safeSleep(1000);
            }
        });

        write(store, "blob", source, data.length, false);

        assertArrayEquals(data, fake.getBlob("blob"));
        assertThat(counters().getWriteStalls(), equalTo(0L));
        assertThat(counters().getRetries(), equalTo(0L));
    }

    // ---- no buffering ----

    public void testBodyIsStreamedNotBuffered() throws IOException {
        // The source blocks after 1MiB until the server has received 1MiB of the body. A client that buffered the part before sending it
        // would never get there, and the source would never be released.
        final long size = 8L * 1024 * 1024;
        final long firstBytes = 1024 * 1024;
        final var store = createBlobStore(streamingSettings(size));
        fake.keepData = false;
        final CountDownLatch serverHasFirstBytes = new CountDownLatch(1);
        fake.bodyProgress = received -> {
            if (received >= firstBytes) {
                serverHasFirstBytes.countDown();
            }
        };
        final AtomicBoolean blocked = new AtomicBoolean();
        final var source = new PatternInputStream(size, (offset, resets) -> {
            if (offset >= firstBytes && blocked.compareAndSet(false, true)) {
                safeAwait(serverHasFirstBytes);
            }
        });

        write(store, "blob", source, size, false);

        assertThat(blocked.get(), equalTo(true));
        assertThat(fake.getBlobLength("blob"), equalTo(size));
        assertThat(fake.getProtocolErrors(), empty());
        // the part, and the last 256kb on their own
        assertThat(puts("blob").size(), equalTo(2));
    }

    public void testHeapUsedByAnUploadDoesNotDependOnThePartSize() throws IOException {
        final ThreadMXBean threads = (ThreadMXBean) ManagementFactory.getThreadMXBean();
        assumeTrue("thread allocation accounting is not supported", threads.isThreadAllocatedMemorySupported());
        threads.setThreadAllocatedMemoryEnabled(true);
        final long size = 64L * 1024 * 1024;
        fake.keepData = false;

        final var streaming = createBlobStore(streamingSettings(size));
        write(streaming, "warmup", new PatternInputStream(2 * KB256, null), 2 * KB256, false);
        final long before = threads.getCurrentThreadAllocatedBytes();
        write(streaming, "streamed", new PatternInputStream(size, null), size, false);
        final long streamedBytes = threads.getCurrentThreadAllocatedBytes() - before;
        assertThat(fake.getBlobLength("streamed"), equalTo(size));

        final var sdk = createBlobStore(streamingSettings(0));
        write(sdk, "warmup-sdk", new PatternInputStream(2 * KB256, null), 2 * KB256, false);
        final long sdkBefore = threads.getCurrentThreadAllocatedBytes();
        write(sdk, "sdk", new PatternInputStream(size, null), size, false);
        final long sdkBytes = threads.getCurrentThreadAllocatedBytes() - sdkBefore;

        logger.info("allocated by the writing thread for a [{}] blob: streaming [{}], sdk [{}]", size, streamedBytes, sdkBytes);
        // one copy buffer plus the HTTP machinery, nowhere near the part size (64MiB here, 100MiB in production)
        assertThat(streamedBytes, lessThan(2L * 1024 * 1024));
        assertThat("the SDK allocates its 16MiB chunk buffer", sdkBytes, greaterThan(ByteSizeUnit.MB.toBytes(16)));
    }

    // ---- stats ----

    public void testRequestsAreCountedInTheRepositoryStats() throws IOException {
        final long partSize = KB256;
        final var store = createBlobStore(streamingSettings(partSize));
        final byte[] data = randomData(3 * partSize + 1);

        write(store, "clean", snapshotSource(data, new AtomicInteger(), null), data.length, false);

        // one open and four parts make one operation of five requests, as a write channel would count it
        assertThat(insertStats(), equalTo(new BlobStoreActionStats(1, 5)));
        assertThat(metricSum(RepositoriesMetrics.METRIC_REQUESTS_TOTAL), equalTo(5L));
        assertThat(metricSum(RepositoriesMetrics.METRIC_OPERATIONS_TOTAL), equalTo(1L));
        assertThat(metricSum(RepositoriesMetrics.METRIC_EXCEPTIONS_TOTAL), equalTo(0L));

        // a throttled part is one more request and one error, and so is the query that follows it
        final AtomicBoolean throttled = new AtomicBoolean();
        fake.faults = call -> call.type() == FakeResumableUploadHandler.Type.PUT
            && call.from() == partSize
            && throttled.compareAndSet(false, true) ? FakeResumableUploadHandler.Fault.status(429) : FakeResumableUploadHandler.Fault.NONE;
        write(store, "throttled", snapshotSource(data, new AtomicInteger(), null), data.length, false);

        assertThat(insertStats(), equalTo(new BlobStoreActionStats(2, 5 + 7)));
        assertThat(metricSum(RepositoriesMetrics.METRIC_REQUESTS_TOTAL), equalTo(5L + 7));
        assertThat(metricSum(RepositoriesMetrics.METRIC_EXCEPTIONS_TOTAL), equalTo(1L));
        assertThat(metricSum(RepositoriesMetrics.METRIC_THROTTLES_TOTAL), equalTo(1L));
        assertThat(counters().getBytes(), equalTo(2L * data.length));
        assertThat(counters().getFiles(), equalTo(2L));
    }

    // ---- against the repository's fixture ----

    public void testAgainstTheFixtureIncludingTheSessionQuery() throws IOException {
        final var fixtureHandler = new GoogleCloudStorageHttpHandler(BUCKET);
        final AtomicBoolean swallowed = new AtomicBoolean();
        final long partSize = 2L * KB256;
        // the fixture processes the second part but its answer is lost, so the client has to ask the session for the committed offset
        httpServer.removeContext("/");
        httpServer.createContext("/", exchange -> {
            final boolean secondPart = exchange.getRequestMethod().equals("PUT")
                && String.valueOf(exchange.getRequestHeaders().getFirst("Content-Range")).startsWith("bytes " + partSize + "-")
                && swallowed.compareAndSet(false, true);
            fixtureHandler.handle(secondPart ? new SwallowedResponseExchange(exchange) : exchange);
            if (secondPart) {
                exchange.close();
            }
        });
        final var store = createBlobStore(streamingSettings(partSize), TimeValue.timeValueSeconds(5), "STANDARD");
        final byte[] data = randomData(5 * partSize + 17);

        write(store, "blob", snapshotSource(data, new AtomicInteger(), null), data.length, true);

        assertThat(fixtureHandler.blobs().get("blob"), equalTo(new BytesArray(data)));
        assertThat(fixtureHandler.getBlobStorageClass("blob"), equalTo("STANDARD"));
        assertThat(swallowed.get(), equalTo(true));
        assertThat(counters().getRetries(), equalTo(1L));
        assertThat(counters().getFiles(), equalTo(1L));
        assertThat(BytesReference.toBytes(fixtureHandler.blobs().get("blob")).length, equalTo(data.length));
    }

    /**
     * Passes the request on to the fixture and drops what the fixture answers.
     */
    @SuppressForbidden(reason = "wraps the exchange of the http server")
    private static final class SwallowedResponseExchange extends HttpExchange {
        private final HttpExchange delegate;

        SwallowedResponseExchange(HttpExchange delegate) {
            this.delegate = delegate;
        }

        @Override
        public Headers getRequestHeaders() {
            return delegate.getRequestHeaders();
        }

        @Override
        public Headers getResponseHeaders() {
            return new Headers();
        }

        @Override
        public URI getRequestURI() {
            return delegate.getRequestURI();
        }

        @Override
        public String getRequestMethod() {
            return delegate.getRequestMethod();
        }

        @Override
        public HttpContext getHttpContext() {
            return delegate.getHttpContext();
        }

        @Override
        public void close() {}

        @Override
        public InputStream getRequestBody() {
            return delegate.getRequestBody();
        }

        @Override
        public OutputStream getResponseBody() {
            return OutputStream.nullOutputStream();
        }

        @Override
        public void sendResponseHeaders(int rCode, long responseLength) {}

        @Override
        public InetSocketAddress getRemoteAddress() {
            return delegate.getRemoteAddress();
        }

        @Override
        public int getResponseCode() {
            return -1;
        }

        @Override
        public InetSocketAddress getLocalAddress() {
            return delegate.getLocalAddress();
        }

        @Override
        public String getProtocol() {
            return delegate.getProtocol();
        }

        @Override
        public Object getAttribute(String name) {
            return delegate.getAttribute(name);
        }

        @Override
        public void setAttribute(String name, Object value) {
            delegate.setAttribute(name, value);
        }

        @Override
        public void setStreams(InputStream i, OutputStream o) {}

        @Override
        public HttpPrincipal getPrincipal() {
            return delegate.getPrincipal();
        }
    }
}
