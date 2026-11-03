/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.objectstore;

import com.sun.net.httpserver.HttpHandler;

import org.apache.logging.log4j.Level;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.repositories.blobstore.ESMockAPIBasedRepositoryIntegTestCase;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitService;
import org.elasticsearch.xpack.stateless.snapshots.StatelessSnapshotSettings;
import org.elasticsearch.xpack.stateless.snapshots.StatelessSnapshotSettings.StatelessSnapshotEnabledStatus;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * Integration tests for {@link org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService} types that rely
 * on mock APIs that emulate cloud-based services.
 */
@SuppressForbidden(reason = "this test uses a HttpServer to emulate a cloud-based storage service")
public abstract class AbstractMockObjectStoreIntegTestCase extends AbstractObjectStoreIntegTestCase {

    private static final TestObjectStoreServer testObjectStoreServer = new TestObjectStoreServer();

    @BeforeClass
    public static void startHttpServer() throws Exception {
        testObjectStoreServer.start();
    }

    @AfterClass
    public static void stopHttpServer() {
        testObjectStoreServer.stop();
    }

    protected static String httpServerUrl() {
        return "http://" + testObjectStoreServer.serverUrl();
    }

    @Before
    public void setUpHttpServer() {
        testObjectStoreServer.setUp(Maps.copyOf(createHttpHandlers(), h -> ESMockAPIBasedRepositoryIntegTestCase.wrap(h, logger)));
    }

    @After
    public void tearDownHttpServer() throws Exception {
        // explicitly clean up the cluster here
        // the cluster needs to be stopped BEFORE the http server is stopped
        try {
            cleanUpCluster();
        } finally {
            testObjectStoreServer.tearDown();
        }
    }

    protected abstract Map<String, HttpHandler> createHttpHandlers();

    /**
     * Whether the fake storage service can be made to refuse copies, see {@link #refuseCopiesAfterTheFirst}. A test class that returns
     * true must implement {@link #refuseCopiesAfterTheFirst}, {@link #blobPathsInFakeStorage} and {@link #accessDeniedText}.
     */
    protected boolean canRefuseCopies() {
        return false;
    }

    /**
     * Makes the fake storage service answer every request to copy a blob on the server, except the first one, with the error that the
     * cloud gives when the caller may not read the source or write the target. The method counts the copy requests in
     * {@code copyRequests}, including the refused ones.
     */
    protected void refuseCopiesAfterTheFirst(AtomicInteger copyRequests) {
        throw new UnsupportedOperationException();
    }

    /**
     * The paths of all the blobs in the fake storage service.
     */
    protected Set<String> blobPathsInFakeStorage() {
        throw new UnsupportedOperationException();
    }

    /**
     * A text that the cloud's access denied error holds, and so the failure of a shard snapshot that was refused a copy.
     */
    protected String accessDeniedText() {
        throw new UnsupportedOperationException();
    }

    /**
     * A shard snapshot that is refused a copy fails with the cloud's error. The failure cleanup then deletes the copies that the shard
     * snapshot made before. The first copy is allowed and the others are refused, so there is a copy to delete.
     */
    public void testAccessDeniedOnCopyFailsTheShardSnapshotAndRunsTheCleanup() throws Exception {
        assumeTrue("the fake storage service can refuse copies", canRefuseCopies());
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
        final var copyRequests = new AtomicInteger();
        refuseCopiesAfterTheFirst(copyRequests);

        // The failed attempt logs the copies it made before its cleanup deletes them. Exactly one copy returned, the allowed one.
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
        assertThat(snapshotInfo.shardFailures().get(0).reason(), containsString(accessDeniedText()));
        // there was a copy that was allowed, and a copy that was refused
        assertThat(copyRequests.get(), greaterThanOrEqualTo(2));
        // the allowed copy was made, as the log line above says, and the failure cleanup deleted it
        assertThat(blobPathsInFakeStorage().stream().filter(path -> path.contains("/c__")).toList(), empty());
    }
}
