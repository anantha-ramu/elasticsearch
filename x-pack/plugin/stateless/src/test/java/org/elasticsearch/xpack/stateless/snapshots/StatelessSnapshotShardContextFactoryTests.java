/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots;

import org.apache.logging.log4j.core.LogEvent;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.ProjectClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingHelper;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IllegalIndexShardStateException;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.LocalPrimarySnapshotShardContext;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.repositories.ShardSnapshotResult;
import org.elasticsearch.repositories.SnapshotIndexCommit;
import org.elasticsearch.repositories.SnapshotShardContext;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotCopiedBlobFeatures;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.xpack.stateless.StatelessPlugin;
import org.elasticsearch.xpack.stateless.commits.BlobFile;
import org.elasticsearch.xpack.stateless.commits.BlobLocation;
import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;
import org.junit.After;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.elasticsearch.xpack.stateless.snapshots.StatelessSnapshotSettings.RELOCATION_DURING_SNAPSHOT_ENABLED_SETTING;
import static org.elasticsearch.xpack.stateless.snapshots.StatelessSnapshotSettings.SERVER_SIDE_COPY_ENABLED_SETTING;
import static org.elasticsearch.xpack.stateless.snapshots.StatelessSnapshotSettings.STATELESS_SNAPSHOT_ENABLED_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.core.Is.isA;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StatelessSnapshotShardContextFactoryTests extends ESTestCase {

    private static final String DATA_FILE = "_0.cfs";

    private TestHarness testHarness;

    @After
    public void closeTestHarness() throws Exception {
        if (testHarness != null) {
            testHarness.close();
        }
    }

    public void testDelegateToSuperClassWhenDisabled() throws IOException {
        testHarness = createTestHarness(Settings.EMPTY);

        final var snapshotShardContextListener = testHarness.asyncCreate(new PlainActionFuture<>());
        final var snapshotShardContext = safeAwait(snapshotShardContextListener);
        assertThat(snapshotShardContext, isA(LocalPrimarySnapshotShardContext.class));
        assertFalse(testHarness.commitClosed().get());

        // Commit is released in both success and failure cases
        final var shardSnapshotResult = new ShardSnapshotResult(
            testHarness.shardGeneration(),
            ByteSizeValue.ofKb(between(1, 42)),
            between(1, 5)
        );
        if (randomBoolean()) {
            snapshotShardContext.onResponse(shardSnapshotResult);
        } else {
            snapshotShardContext.onFailure(new RuntimeException("boom"));
        }
        assertTrue(testHarness.commitClosed().get());
    }

    public void testAcquireCommitFailureIsPropagated() throws IOException {
        testHarness = createTestHarness(
            Settings.builder().put(STATELESS_SNAPSHOT_ENABLED_SETTING.getKey(), "read_from_object_store").build()
        );

        final var expectedException = new AlreadyClosedException("shard closed");
        when(
            testHarness.snapshotsCommitService()
                .acquireAndMaybeRegisterCommitForSnapshot(eq(testHarness.shardId()), any(Snapshot.class), anyBoolean(), any())
        ).thenThrow(expectedException);

        try {
            testHarness.asyncCreate(new PlainActionFuture<>());
            fail("should have failed");
        } catch (AlreadyClosedException e) {
            assertThat(e, sameInstance(expectedException));
        }
    }

    public void testCommitIsReleasedOnShardSnapshotCompletion() throws IOException {
        testHarness = createTestHarness(
            Settings.builder().put(STATELESS_SNAPSHOT_ENABLED_SETTING.getKey(), "read_from_object_store").build()
        );

        final var commitClosed = new AtomicBoolean(false);
        final var indexCommit = mock(IndexCommit.class);
        when(indexCommit.getGeneration()).thenReturn(randomNonNegativeLong());
        final var snapshotIndexCommit = new SnapshotIndexCommit(
            new Engine.IndexCommitRef(indexCommit, () -> assertTrue(commitClosed.compareAndSet(false, true)))
        );
        final long generation = indexCommit.getGeneration();
        when(
            testHarness.snapshotsCommitService()
                .acquireAndMaybeRegisterCommitForSnapshot(eq(testHarness.shardId()), any(Snapshot.class), anyBoolean(), any())
        ).thenReturn(
            new SnapshotsCommitService.SnapshotCommitInfo(
                snapshotIndexCommit,
                Map.of(
                    randomIdentifier(),
                    new BlobLocation(
                        new BlobFile("stateless_commit_" + generation, new PrimaryTermAndGeneration(1L, generation)),
                        randomLongBetween(0, 50),
                        randomLongBetween(1, 900)
                    )
                ),
                randomIdentifier(),
                () -> false
            )
        );

        final var snapshotShardContextListener = testHarness.asyncCreate(new PlainActionFuture<>());
        final var snapshotShardContext = safeAwait(snapshotShardContextListener);
        assertThat(snapshotShardContext, isA(StatelessSnapshotShardContext.class));
        assertFalse(commitClosed.get());

        // Commit is released in both success and failure cases
        final var shardSnapshotResult = new ShardSnapshotResult(
            testHarness.shardGeneration(),
            ByteSizeValue.ofKb(between(1, 42)),
            between(1, 5)
        );
        if (randomBoolean()) {
            snapshotShardContext.onResponse(shardSnapshotResult);
        } else {
            snapshotShardContext.onFailure(new RuntimeException("boom"));
        }
        assertTrue(commitClosed.get());
    }

    public void testAcquireCommitFailureRetriesOnRemoteWhenEnabled() throws IOException {
        testHarness = createTestHarness(Settings.builder().put(STATELESS_SNAPSHOT_ENABLED_SETTING.getKey(), "enabled").build());
        final var retryableException = randomFrom(
            new ShardNotFoundException(testHarness.shardId()),
            new IndexNotFoundException(testHarness.shardId().getIndex()),
            new IllegalIndexShardStateException(testHarness.shardId(), IndexShardState.CLOSED, "shard not in expected state"),
            new NoShardAvailableActionException(testHarness.shardId(), "no shard available"),
            new UnavailableShardsException(testHarness.shardId(), "no shard available"),
            new AlreadyClosedException("already closed")
        );

        when(
            testHarness.snapshotsCommitService()
                .acquireAndMaybeRegisterCommitForSnapshot(eq(testHarness.shardId()), any(Snapshot.class), anyBoolean(), any())
        ).thenThrow(retryableException);

        // Mock client to capture the remote request and respond with commit info
        final var client = mock(Client.class);
        final var projectClient = mock(ProjectClient.class);
        when(testHarness.stateless().getClient()).thenReturn(client);
        when(client.projectClient(any(ProjectId.class))).thenReturn(projectClient);

        final var expectedShardStateId = randomIdentifier();
        doAnswer(invocation -> {
            final ActionListener<GetShardSnapshotCommitInfoResponse> listener = invocation.getArgument(2);
            listener.onResponse(new GetShardSnapshotCommitInfoResponse(Map.of(), expectedShardStateId));
            return null;
        }).when(projectClient)
            .execute(eq(TransportGetShardSnapshotCommitInfoAction.TYPE), any(GetShardSnapshotCommitInfoRequest.class), anyActionListener());

        final var snapshotShardContextListener = testHarness.asyncCreate(new PlainActionFuture<>());
        final var snapshotShardContext = safeAwait(snapshotShardContextListener);
        assertThat(snapshotShardContext, isA(StatelessSnapshotShardContext.class));
        assertThat(snapshotShardContext.stateIdentifier(), equalTo(expectedShardStateId));
    }

    public void testCopiesOnlyWhenEveryNodeHasTheFeatureOnLocalPrimary() throws IOException {
        assertCopiesOnlyWhenEveryNodeHasTheFeature(false);
    }

    public void testCopiesOnlyWhenEveryNodeHasTheFeatureOnRemoteNode() throws IOException {
        assertCopiesOnlyWhenEveryNodeHasTheFeature(true);
    }

    /**
     * Checks the rule that decides whether a shard snapshot copies its files or uploads them. The factory builds a
     * {@link SnapshotShardContext} in two ways. If {@code remote} is false, the primary is on the same node as the factory, and the
     * factory acquires the commit from the local primary. If {@code remote} is true, the primary is on another node, and the factory asks
     * that node for the blob locations of the commit. The shard snapshot must copy only when the setting is on and every node has the
     * cluster feature for copied files. If the setting is on and a node lacks the feature, the shard snapshot must upload its files and log
     * one INFO line that says why.
     */
    private void assertCopiesOnlyWhenEveryNodeHasTheFeature(boolean remote) throws IOException {
        testHarness = createTestHarness(
            Settings.builder()
                // only "enabled" lets the factory ask another node for the commit
                .put(STATELESS_SNAPSHOT_ENABLED_SETTING.getKey(), remote ? "enabled" : "read_from_object_store")
                .put(SERVER_SIDE_COPY_ENABLED_SETTING.getKey(), true)
                .build()
        );
        final var blobLocations = Map.of(
            DATA_FILE,
            new BlobLocation(new BlobFile("stateless_commit_7", new PrimaryTermAndGeneration(1L, 7L)), randomLongBetween(0, 50), 500L)
        );
        if (remote) {
            // the primary is not on this node, so the commit info comes from another node
            when(
                testHarness.snapshotsCommitService()
                    .acquireAndMaybeRegisterCommitForSnapshot(eq(testHarness.shardId()), any(Snapshot.class), anyBoolean(), any())
            ).thenThrow(new ShardNotFoundException(testHarness.shardId()));
            final var client = mock(Client.class);
            final var projectClient = mock(ProjectClient.class);
            when(testHarness.stateless().getClient()).thenReturn(client);
            when(client.projectClient(any(ProjectId.class))).thenReturn(projectClient);
            doAnswer(invocation -> {
                final ActionListener<GetShardSnapshotCommitInfoResponse> listener = invocation.getArgument(2);
                listener.onResponse(new GetShardSnapshotCommitInfoResponse(blobLocations, randomIdentifier()));
                return null;
            }).when(projectClient)
                .execute(
                    eq(TransportGetShardSnapshotCommitInfoAction.TYPE),
                    any(GetShardSnapshotCommitInfoRequest.class),
                    anyActionListener()
                );
        } else {
            final var indexCommit = mock(IndexCommit.class);
            when(indexCommit.getGeneration()).thenReturn(7L);
            when(
                testHarness.snapshotsCommitService()
                    .acquireAndMaybeRegisterCommitForSnapshot(eq(testHarness.shardId()), any(Snapshot.class), anyBoolean(), any())
            ).thenAnswer(
                invocation -> new SnapshotsCommitService.SnapshotCommitInfo(
                    new SnapshotIndexCommit(new Engine.IndexCommitRef(indexCommit, () -> {})),
                    blobLocations,
                    randomIdentifier(),
                    () -> false
                )
            );
        }
        final DiscoveryNode otherNode = DiscoveryNodeUtils.create(randomIdentifier());

        // One node lacks the feature. The shard snapshot uploads, and logs why once.
        setNodeFeatures(otherNode, false);
        try (var mockLog = MockLog.capture(StatelessSnapshotShardContextFactory.class)) {
            mockLog.addExpectation(logMessagesContaining("instead of copying commit blobs", 1));
            // no copy source for the file means that the file is uploaded
            assertThat(createContext().copySource(DATA_FILE), nullValue());
            mockLog.assertAllExpectationsMatched();
        }

        // Every node has the feature. The shard snapshot copies, and logs nothing.
        setNodeFeatures(otherNode, true);
        try (var mockLog = MockLog.capture(StatelessSnapshotShardContextFactory.class)) {
            mockLog.addExpectation(logMessagesContaining("instead of copying commit blobs", 0));
            assertThat(createContext().copySource(DATA_FILE), notNullValue());
            mockLog.assertAllExpectationsMatched();
        }

        // The setting is off, so the shard snapshot uploads and logs nothing, whichever nodes have the feature.
        testHarness.stateless()
            .getClusterService()
            .getClusterSettings()
            .applySettings(Settings.builder().put(SERVER_SIDE_COPY_ENABLED_SETTING.getKey(), false).build());
        for (boolean everyNodeHasFeature : new boolean[] { false, true }) {
            setNodeFeatures(otherNode, everyNodeHasFeature);
            try (var mockLog = MockLog.capture(StatelessSnapshotShardContextFactory.class)) {
                mockLog.addExpectation(logMessagesContaining("instead of copying commit blobs", 0));
                assertThat(createContext().copySource(DATA_FILE), nullValue());
                mockLog.assertAllExpectationsMatched();
            }
        }
    }

    private SnapshotShardContext createContext() throws IOException {
        return safeAwait(testHarness.asyncCreate(new PlainActionFuture<>()));
    }

    /**
     * Sets the cluster state to two nodes: the local node, which always has the cluster feature for copied files, and {@code otherNode},
     * which has the feature only if {@code otherNodeHasFeature} is true.
     */
    private void setNodeFeatures(DiscoveryNode otherNode, boolean otherNodeHasFeature) {
        final ClusterService clusterService = testHarness.stateless().getClusterService();
        final ClusterState state = clusterService.state();
        final Set<String> featureIds = Set.of(SnapshotCopiedBlobFeatures.COPIED_BLOB_RANGES.id());
        ClusterServiceUtils.setState(
            clusterService,
            ClusterState.builder(state)
                .nodes(DiscoveryNodes.builder(state.nodes()).add(otherNode))
                .nodeFeatures(
                    Map.of(state.nodes().getLocalNodeId(), featureIds, otherNode.getId(), otherNodeHasFeature ? featureIds : Set.of())
                )
        );
    }

    /**
     * Expects exactly {@code expectedCount} log messages that contain {@code text}. MockLog's own expectations only check that a message is
     * seen at least once, or never, so they would not catch the same message being logged twice.
     */
    private static MockLog.LoggingExpectation logMessagesContaining(String text, int expectedCount) {
        final var seen = new AtomicInteger();
        return new MockLog.LoggingExpectation() {
            @Override
            public void match(LogEvent event) {
                if (event.getMessage().getFormattedMessage().contains(text)) {
                    seen.incrementAndGet();
                }
            }

            @Override
            public void assertMatched() {
                assertThat(seen.get(), equalTo(expectedCount));
            }
        };
    }

    private record TestHarness(
        StatelessPlugin stateless,
        ShardId shardId,
        SnapshotsCommitService snapshotsCommitService,
        AtomicBoolean commitClosed,
        ShardGeneration shardGeneration,
        IndexShardSnapshotStatus indexShardSnapshotStatus
    ) {

        SubscribableListener<SnapshotShardContext> asyncCreate(ActionListener<ShardSnapshotResult> listener) throws IOException {
            final var factory = new StatelessSnapshotShardContextFactory(stateless());
            final var snapshot = new Snapshot(ProjectId.DEFAULT, randomIdentifier(), new SnapshotId(randomIdentifier(), randomUUID()));

            final IndexId indexId = new IndexId(shardId().getIndexName(), randomUUID());
            final long startTime = randomNonNegativeLong();
            final IndexVersion indexVersion = IndexVersion.current();
            final ClusterState initialState = stateless().getClusterService().state();
            final var snapshotsInProgress = SnapshotsInProgress.EMPTY.withAddedEntry(
                SnapshotsInProgress.startedEntry(
                    snapshot,
                    randomBoolean(),
                    randomBoolean(),
                    Map.of(shardId().getIndexName(), indexId),
                    List.of(),
                    startTime,
                    randomNonNegativeLong(),
                    Map.of(shardId(), new SnapshotsInProgress.ShardSnapshotStatus(initialState.nodes().getLocalNodeId(), shardGeneration)),
                    Map.of(),
                    indexVersion,
                    List.of()
                )
            );
            ClusterServiceUtils.setState(
                stateless().getClusterService(),
                ClusterState.builder(initialState).putCustom(SnapshotsInProgress.TYPE, snapshotsInProgress)
            );

            return factory.asyncCreate(shardId(), snapshot, indexId, indexShardSnapshotStatus(), indexVersion, startTime, listener);
        }

        public void close() throws IOException {
            IOUtils.close(stateless.getClusterService());
        }
    }

    private static TestHarness createTestHarness(Settings settings) throws IOException {
        final var stateless = mock(StatelessPlugin.class);

        final var clusterService = ClusterServiceUtils.createClusterService(
            new DeterministicTaskQueue().getThreadPool(),
            new ClusterSettings(
                settings,
                Sets.addToCopy(
                    ClusterSettings.BUILT_IN_CLUSTER_SETTINGS,
                    STATELESS_SNAPSHOT_ENABLED_SETTING,
                    RELOCATION_DURING_SNAPSHOT_ENABLED_SETTING,
                    StatelessSnapshotSettings.SERVER_SIDE_COPY_ENABLED_SETTING
                )
            )
        );
        when(stateless.getClusterService()).thenReturn(clusterService);
        when(stateless.getFeatureService()).thenReturn(new FeatureService(List.of(new SnapshotCopiedBlobFeatures())));
        when(stateless.shardBlobContainerFunc()).thenReturn((shardId, primaryTerm) -> mock(BlobContainer.class));
        final var indicesService = mock(IndicesService.class);
        when(stateless.getIndicesService()).thenReturn(indicesService);

        final var snapshotsCommitService = mock(SnapshotsCommitService.class);
        when(stateless.getSnapshotsCommitService()).thenReturn(snapshotsCommitService);

        final var shardId = new ShardId(new Index(randomIndexName(), randomUUID()), between(0, 5));

        final var indexService = mock(IndexService.class);
        when(indicesService.indexServiceSafe(eq(shardId.getIndex()))).thenReturn(indexService);
        final var indexShard = mock(IndexShard.class);
        when(indexService.getShard(shardId.id())).thenReturn(indexShard);

        final String nodeId = randomIdentifier();
        final var shardRouting = ShardRoutingHelper.moveToStarted(
            ShardRoutingHelper.initialize(
                ShardRouting.newUnassigned(
                    shardId,
                    true,
                    RecoverySource.EmptyStoreRecoverySource.INSTANCE,
                    new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "foobar"),
                    ShardRouting.Role.INDEX_ONLY,
                    ShardRouting.RecoveryPriority.UNASSIGNED_NEW_PRIMARY
                ),
                nodeId
            )
        );
        when(indexShard.routingEntry()).thenReturn(shardRouting);
        when(indexShard.shardId()).thenReturn(shardId);

        final var closed = new AtomicBoolean(false);
        when(indexShard.state()).thenReturn(IndexShardState.STARTED);
        final IndexCommit indexCommit = mock(IndexCommit.class);
        when(indexCommit.getGeneration()).thenReturn(randomNonNegativeLong());
        final long seqNo = randomLongBetween(1, 100);
        when(indexCommit.getUserData()).thenReturn(
            Map.of(
                Engine.HISTORY_UUID_KEY,
                randomUUID(),
                SequenceNumbers.MAX_SEQ_NO,
                Long.toString(seqNo),
                SequenceNumbers.LOCAL_CHECKPOINT_KEY,
                Long.toString(seqNo)
            )
        );
        when(indexShard.acquireIndexCommitForSnapshot()).thenReturn(
            new Engine.IndexCommitRef(indexCommit, () -> assertTrue(closed.compareAndSet(false, true)))
        );
        when(indexShard.getLastSyncedGlobalCheckpoint()).thenReturn(seqNo);
        when(indexShard.store()).thenReturn(mock(Store.class));
        final var indexMetadata = IndexMetadata.builder(shardId.getIndexName())
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_INDEX_UUID, shardId.getIndex().getUUID())
                    .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                    .build()
            )
            .numberOfShards(shardId.id() + 1)
            .numberOfReplicas(0)
            .build();
        final var indexSettings = new IndexSettings(indexMetadata, Settings.EMPTY);
        when(indexShard.indexSettings()).thenReturn(indexSettings);
        final var shardGeneration = ShardGeneration.newGeneration();
        final var shardSnapshotStatus = IndexShardSnapshotStatus.newInitializing(shardGeneration, randomLongBetween(1, Long.MAX_VALUE));
        return new TestHarness(stateless, shardId, snapshotsCommitService, closed, shardGeneration, shardSnapshotStatus);
    }
}
