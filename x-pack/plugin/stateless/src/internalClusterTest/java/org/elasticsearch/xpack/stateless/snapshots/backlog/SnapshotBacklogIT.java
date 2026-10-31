/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots.backlog;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.support.BlobMetadata;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.MergePolicyConfig;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.ProjectRepo;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.repositories.ShardGenerations;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xpack.core.ilm.OperationMode;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleMetadata;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicy;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicyMetadata;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.elasticsearch.xpack.stateless.commits.HollowShardsService;
import org.elasticsearch.xpack.stateless.snapshots.StatelessSnapshotSettings.StatelessSnapshotEnabledStatus;
import org.elasticsearch.xpack.stateless.snapshots.backlog.ShardBaselineCache.Measured;
import org.elasticsearch.xpack.stateless.snapshots.backlog.ShardBaselineCache.Result;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase.createFullSnapshot;
import static org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase.getRepositoryData;
import static org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase.getRepositoryOnMaster;
import static org.elasticsearch.xpack.stateless.snapshots.StatelessSnapshotSettings.RELOCATION_DURING_SNAPSHOT_ENABLED_SETTING;
import static org.elasticsearch.xpack.stateless.snapshots.StatelessSnapshotSettings.STATELESS_SNAPSHOT_ENABLED_SETTING;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;

/**
 * Checks the backlog an index node measures against what the next snapshot of the shard actually uploads: the total length of the
 * data blobs ({@code __*}) the snapshot adds to the shard's folder in the repository. Files kept inside the shard's backup metadata
 * ({@code .si}, {@code segments_N}) are never data blobs, and the backlog leaves them out too.
 * <p>
 * Before measuring, each case flushes and waits for the shard's persisted checkpoint to catch up, as a snapshot flushes before taking
 * its commit: that way the measured commit and the snapshot's commit are the same, with the same state id. Background merges are off,
 * so nothing changes the commit in between. These cases run through the stateless {@code IndexEngine}, which the unit tests can't.
 * <p>
 * SLM itself isn't loaded: the tracker only reads its policies from the cluster state, so {@link SlmMetadataPlugin} registers that
 * metadata and the test puts the policies in directly.
 */
public class SnapshotBacklogIT extends AbstractStatelessPluginIntegTestCase {

    private static final String REPOSITORY = "repo";
    private static final String OTHER_REPOSITORY = "other-repo";
    private static final String INDEX = "index";

    /**
     * Registers the SLM metadata, as {@code XPackClientPlugin} and the SLM plugin do in production.
     */
    public static class SlmMetadataPlugin extends Plugin {
        @Override
        public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
            return List.of(
                new NamedWriteableRegistry.Entry(
                    Metadata.ProjectCustom.class,
                    SnapshotLifecycleMetadata.TYPE,
                    SnapshotLifecycleMetadata::new
                ),
                new NamedWriteableRegistry.Entry(
                    NamedDiff.class,
                    SnapshotLifecycleMetadata.TYPE,
                    SnapshotLifecycleMetadata.SnapshotLifecycleMetadataDiff::new
                )
            );
        }

        @Override
        public List<NamedXContentRegistry.Entry> getNamedXContent() {
            return List.of(
                new NamedXContentRegistry.Entry(
                    Metadata.ProjectCustom.class,
                    new ParseField(SnapshotLifecycleMetadata.TYPE),
                    parser -> SnapshotLifecycleMetadata.PARSER.parse(parser, null)
                )
            );
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(SlmMetadataPlugin.class);
        plugins.add(TestTelemetryPlugin.class);
        // Registers index.merge.enabled.
        plugins.add(InternalSettingsPlugin.class);
        return plugins;
    }

    @Override
    protected boolean addMockFsRepository() {
        return false;
    }

    /**
     * Takes snapshots the way serverless does, measures the backlog, and keeps shards from going hollow unless a case asks for it.
     */
    private static Settings nodeSettings(boolean hollowShards) {
        final Settings.Builder settings = Settings.builder()
            .put(STATELESS_SNAPSHOT_ENABLED_SETTING.getKey(), StatelessSnapshotEnabledStatus.ENABLED)
            .put(RELOCATION_DURING_SNAPSHOT_ENABLED_SETTING.getKey(), true)
            .put(SnapshotBacklogTracker.ENABLED_SETTING.getKey(), true)
            .put(HollowShardsService.STATELESS_HOLLOW_INDEX_SHARDS_ENABLED.getKey(), hollowShards);
        if (hollowShards) {
            settings.put(HollowShardsService.SETTING_HOLLOW_INGESTION_TTL.getKey(), TimeValue.timeValueMillis(1));
            settings.put(HollowShardsService.SETTING_HOLLOW_INGESTION_DS_NON_WRITE_TTL.getKey(), TimeValue.timeValueMillis(1));
        }
        return settings.build();
    }

    private static Settings indexSettingsWithoutMerges(int shards) {
        return indexSettings(shards, 0).put(MergePolicyConfig.INDEX_MERGE_ENABLED, false).build();
    }

    public void testFigureMatchesTheNextSnapshotAfterWrites() throws Exception {
        startMasterOnlyNode(nodeSettings(false));
        final String indexNode = startIndexNode(nodeSettings(false));
        createRepository(REPOSITORY, "fs");
        putPolicies(REPOSITORY);
        final int shards = between(1, 3);
        createIndex(INDEX, indexSettingsWithoutMerges(shards));
        ensureGreen(INDEX);

        // The first snapshot of a new index uploads every file, and the repository has no generation for its shards yet.
        writeAndSettle(between(1, 3));
        assertFiguresMatchNextSnapshot(indexNode, REPOSITORY, shards, "snap-1");

        for (int round = 2; round <= 3; round++) {
            writeAndSettle(between(1, 3));
            assertFiguresMatchNextSnapshot(indexNode, REPOSITORY, shards, "snap-" + round);
        }

        // With no writes since, the state id matches the last snapshot: nothing to upload.
        assertFiguresMatchNextSnapshot(indexNode, REPOSITORY, shards, "snap-unchanged", 0L);
    }

    public void testAMergeWithTheSameStateIdCountsNothing() throws Exception {
        startMasterOnlyNode(nodeSettings(false));
        final String indexNode = startIndexNode(nodeSettings(false));
        createRepository(REPOSITORY, "fs");
        putPolicies(REPOSITORY);
        // Many segments are allowed per tier, so nothing merges until the test asks.
        createIndex(INDEX, indexSettings(1, 0).put(MergePolicyConfig.INDEX_MERGE_POLICY_SEGMENTS_PER_TIER_SETTING.getKey(), 100.0).build());
        ensureGreen(INDEX);
        writeAndSettle(6);
        assertFiguresMatchNextSnapshot(indexNode, REPOSITORY, 1, "snap-1");
        final IndexShard shard = findIndexShard(resolveIndex(INDEX), 0);
        final int segmentsBefore = committedSegments(shard);

        // A merge the merge policy picks rewrites the files but leaves the state id as it was, so the snapshot reuses the last one
        // and uploads nothing. A force merge to a number of segments would record a new force-merge id, and so a new state id.
        updateIndexSettings(Settings.builder().put(MergePolicyConfig.INDEX_MERGE_POLICY_SEGMENTS_PER_TIER_SETTING.getKey(), 2.0), INDEX);
        // With no number of segments, this only lets the merge policy pick merges, which run in the background.
        assertThat(indicesAdmin().prepareForceMerge(INDEX).get().getFailedShards(), equalTo(0));
        assertBusy(() -> {
            flush(INDEX);
            assertThat(committedSegments(shard), lessThan(segmentsBefore));
        });
        settle();
        assertFiguresMatchNextSnapshot(indexNode, REPOSITORY, 1, "snap-2", 0L);
    }

    public void testTwoRepositoriesAreMeasuredSeparately() throws Exception {
        startMasterOnlyNode(nodeSettings(false));
        final String indexNode = startIndexNode(nodeSettings(false));
        createRepository(REPOSITORY, "fs");
        createRepository(OTHER_REPOSITORY, "fs");
        putPolicies(REPOSITORY, OTHER_REPOSITORY);
        createIndex(INDEX, indexSettingsWithoutMerges(1));
        ensureGreen(INDEX);
        writeAndSettle(2);
        assertFiguresMatchNextSnapshot(indexNode, REPOSITORY, 1, "snap-1");

        // The other repository has nothing yet, so it still counts every file, whatever the first one holds.
        writeAndSettle(1);
        assertFiguresMatchNextSnapshot(indexNode, OTHER_REPOSITORY, 1, "other-snap-1");
        assertFiguresMatchNextSnapshot(indexNode, REPOSITORY, 1, "snap-2");
    }

    public void testRelocationAndRestart() throws Exception {
        startMasterOnlyNode(nodeSettings(false));
        final String firstNode = startIndexNode(nodeSettings(false));
        createRepository(REPOSITORY, "fs");
        putPolicies(REPOSITORY);
        createIndex(INDEX, indexSettingsWithoutMerges(1));
        ensureGreen(INDEX);
        writeAndSettle(2);
        assertFiguresMatchNextSnapshot(firstNode, REPOSITORY, 1, "snap-1");
        writeAndSettle(1);

        // The shard moves to another node, which loads its own baseline.
        final String secondNode = startIndexNode(nodeSettings(false));
        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", firstNode), INDEX);
        internalCluster().awaitNodeVacated(INDEX, firstNode);
        ensureGreen(INDEX);
        settle();
        assertFiguresMatchNextSnapshot(secondNode, REPOSITORY, 1, "snap-2");
        assertThat(metadataReads(secondNode), greaterThan(0L));

        // The node restarts: it measures again from nothing.
        writeAndSettle(1);
        internalCluster().restartNode(secondNode);
        ensureGreen(INDEX);
        settle();
        assertFiguresMatchNextSnapshot(secondNode, REPOSITORY, 1, "snap-3");
        // After the restart the node had nothing in memory, so it read the shard's backup metadata again.
        assertThat(metadataReads(secondNode), greaterThan(0L));
    }

    public void testHollowShardCountsZeroWithoutReading() throws Exception {
        startMasterOnlyNode(nodeSettings(true));
        final String firstNode = startIndexNode(nodeSettings(true));
        createRepository(REPOSITORY, "fs");
        putPolicies(REPOSITORY);
        createIndex(INDEX, indexSettingsWithoutMerges(1));
        ensureGreen(INDEX);
        writeAndSettle(2);
        assertFiguresMatchNextSnapshot(firstNode, REPOSITORY, 1, "snap-1");

        // A shard relocated with no recent writes arrives hollow, once it is hollowable on the node it leaves.
        final HollowShardsService firstNodeHollowShards = internalCluster().getInstance(HollowShardsService.class, firstNode);
        final IndexShard firstCopy = findIndexShard(resolveIndex(INDEX), 0);
        assertBusy(() -> assertTrue(firstNodeHollowShards.isHollowableIndexShard(firstCopy)));
        final String secondNode = startIndexNode(nodeSettings(true));
        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", firstNode), INDEX);
        internalCluster().awaitNodeVacated(INDEX, firstNode);
        ensureGreen(INDEX);
        final ShardId shardId = new ShardId(resolveIndex(INDEX), 0);
        final HollowShardsService hollowShards = internalCluster().getInstance(HollowShardsService.class, secondNode);
        assertBusy(() -> assertTrue(hollowShards.isHollowShard(shardId)));

        // The second node never held the shard other than hollow, so it never read its backup metadata, not even in a background
        // evaluation before this one.
        assertThat(measure(secondNode, REPOSITORY, shardId, expectedGeneration(REPOSITORY, shardId)), equalTo(0L));
        assertThat(metadataReads(secondNode), equalTo(0L));
    }

    public void testFinishedSnapshotGenerationIsUsedUntilTheMasterAnswers() throws Exception {
        final String master = startMasterOnlyNode(nodeSettings(false));
        final String indexNode = startIndexNode(nodeSettings(false));
        createRepository(REPOSITORY, "fs");
        putPolicies(REPOSITORY);
        createIndex(INDEX, indexSettingsWithoutMerges(1));
        ensureGreen(INDEX);
        writeAndSettle(2);
        assertFiguresMatchNextSnapshot(indexNode, REPOSITORY, 1, "snap-1");
        final ShardId shardId = new ShardId(resolveIndex(INDEX), 0);
        final ShardGeneration beforeSnapshot = expectedGeneration(REPOSITORY, shardId);

        // From now on the master holds the index node's requests, so the node's answer stays at the repository generation it had.
        final AtomicBoolean holding = new AtomicBoolean(true);
        final List<CheckedRunnable<Exception>> held = new ArrayList<>();
        MockTransportService.getInstance(master)
            .addRequestHandlingBehavior(TransportGetShardGenerationsAction.NAME, (handler, request, channel, task) -> {
                synchronized (held) {
                    if (holding.get()) {
                        held.add(() -> handler.messageReceived(request, channel, task));
                        return;
                    }
                }
                handler.messageReceived(request, channel, task);
            });
        writeAndSettle(1);
        createFullSnapshot(logger, REPOSITORY, "snap-2");
        final ShardGeneration afterSnapshot = expectedGeneration(REPOSITORY, shardId);
        assertThat(afterSnapshot, not(equalTo(beforeSnapshot)));

        // Once the index node has applied the state in which the snapshot ended, the snapshot's generation is no longer in its cluster
        // state, and its answer from the master is out of date: it asked again, and that request is held.
        final long repositoryGeneration = getRepositoryData(getRepositoryOnMaster(REPOSITORY)).getGenId();
        final ClusterService indexNodeClusterService = internalCluster().getInstance(ClusterService.class, indexNode);
        assertBusy(() -> {
            final ClusterState state = indexNodeClusterService.state();
            assertThat(SnapshotsInProgress.get(state).forRepo(ProjectId.DEFAULT, REPOSITORY), empty());
            assertThat(
                RepositoriesMetadata.get(state.metadata().getProject()).repository(REPOSITORY).generation(),
                equalTo(repositoryGeneration)
            );
            synchronized (held) {
                assertThat(held, not(empty()));
            }
        });

        // The node measures against the generation the finished snapshot wrote, remembered from the cluster state.
        assertThat(measure(indexNode, REPOSITORY, shardId, afterSnapshot), equalTo(0L));
        synchronized (held) {
            assertTrue(holding.get());
        }

        synchronized (held) {
            holding.set(false);
            for (CheckedRunnable<Exception> request : held) {
                request.run();
            }
        }
        writeAndSettle(1);
        assertFiguresMatchNextSnapshot(indexNode, REPOSITORY, 1, "snap-3");
    }

    // Puts one SLM policy per repository in the default project's metadata. Also used by SnapshotBacklogMasterIT.
    static void putPolicies(String... repositories) {
        final Map<String, SnapshotLifecyclePolicyMetadata> policies = new HashMap<>();
        for (String repository : repositories) {
            final String id = "policy-" + repository;
            policies.put(
                id,
                SnapshotLifecyclePolicyMetadata.builder()
                    .setPolicy(new SnapshotLifecyclePolicy(id, "snap", "0 0/30 * * * ?", repository, null, null))
                    .setVersion(1)
                    .setModifiedDate(0)
                    .build()
            );
        }
        safeAwait(
            (ActionListener<Void> listener) -> internalCluster().getCurrentMasterNodeInstance(ClusterService.class)
                .submitUnbatchedStateUpdateTask("put snapshot lifecycle policies", new ClusterStateUpdateTask() {
                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        return currentState.copyAndUpdateProject(
                            ProjectId.DEFAULT,
                            project -> project.putCustom(
                                SnapshotLifecycleMetadata.TYPE,
                                new SnapshotLifecycleMetadata(policies, OperationMode.RUNNING, null)
                            )
                        );
                    }

                    @Override
                    public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                        listener.onResponse(null);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        listener.onFailure(e);
                    }
                })
        );
    }

    private static long metadataReads(String node) {
        final TestTelemetryPlugin telemetry = getTelemetryPlugin(node);
        telemetry.collect();
        return telemetry.getLongCounterMeasurement(BlobStoreMetadataReader.READS_METRIC).stream().mapToLong(m -> m.getLong()).sum();
    }

    private static int committedSegments(IndexShard shard) {
        return shard.withEngine(engine -> engine.getLastCommittedSegmentInfos().size());
    }

    private void writeAndSettle(int flushes) throws Exception {
        for (int i = 0; i < flushes; i++) {
            indexDocs(INDEX, between(10, 100));
            flush(INDEX);
        }
        settle();
    }

    /**
     * Flushes, and waits until each primary's persisted checkpoint has caught up with its writes, so its latest commit has the state
     * id the next snapshot will give it.
     */
    private void settle() throws Exception {
        flush(INDEX);
        final var index = resolveIndex(INDEX);
        final int shards = clusterService().state().metadata().getProject().index(index).getNumberOfShards();
        for (int shard = 0; shard < shards; shard++) {
            final IndexShard indexShard = findIndexShard(index, shard);
            assertBusy(() -> assertThat(indexShard.getLastSyncedGlobalCheckpoint(), equalTo(indexShard.seqNoStats().getMaxSeqNo())));
        }
    }

    private void assertFiguresMatchNextSnapshot(String node, String repository, int shards, String snapshot) throws Exception {
        assertFiguresMatchNextSnapshot(node, repository, shards, snapshot, null);
    }

    /**
     * Measures every shard of the index on the node, takes a snapshot, and checks each figure against what the snapshot uploaded.
     *
     * @param expected if not null, what every figure must also be
     */
    private void assertFiguresMatchNextSnapshot(String node, String repository, int shards, String snapshot, Long expected)
        throws Exception {
        final var index = resolveIndex(INDEX);
        final Map<ShardId, Long> figures = new HashMap<>();
        final Map<ShardId, Map<String, Long>> blobsBefore = new HashMap<>();
        for (int shard = 0; shard < shards; shard++) {
            final ShardId shardId = new ShardId(index, shard);
            figures.put(shardId, measure(node, repository, shardId, expectedGeneration(repository, shardId)));
            blobsBefore.put(shardId, dataBlobs(repository, shardId));
        }
        createFullSnapshot(logger, repository, snapshot);
        for (Map.Entry<ShardId, Long> shardAndFigure : figures.entrySet()) {
            final ShardId shardId = shardAndFigure.getKey();
            final Map<String, Long> before = blobsBefore.get(shardId);
            long uploaded = 0;
            for (Map.Entry<String, Long> blob : dataBlobs(repository, shardId).entrySet()) {
                if (before.containsKey(blob.getKey()) == false) {
                    uploaded += blob.getValue();
                }
            }
            logger.info(
                "--> [{}] {}: measured [{}], snapshot [{}] uploaded [{}]",
                repository,
                shardId,
                shardAndFigure.getValue(),
                snapshot,
                uploaded
            );
            assertThat(shardId + " in " + repository, shardAndFigure.getValue(), equalTo(uploaded));
            if (expected != null) {
                assertThat(uploaded, equalTo(expected));
            } else {
                assertThat(uploaded, greaterThan(0L));
            }
        }
    }

    /**
     * Evaluates on the node until the shard is measured against the given generation, and gives the figure.
     */
    private long measure(String node, String repository, ShardId shardId, ShardGeneration generation) throws Exception {
        final SnapshotBacklogTracker tracker = internalCluster().getInstance(SnapshotBacklogTracker.class, node);
        final ThreadPool threadPool = internalCluster().getInstance(ThreadPool.class, node);
        final ProjectRepo projectRepo = new ProjectRepo(ProjectId.DEFAULT, repository);
        final AtomicReference<Result> result = new AtomicReference<>();
        assertBusy(() -> {
            safeGet(threadPool.executor(ThreadPool.Names.SNAPSHOT_META).submit(tracker::evaluate));
            result.set(tracker.getResults().getOrDefault(projectRepo, Map.of()).get(shardId));
            assertThat(result.get(), instanceOf(Measured.class));
            assertThat(((Measured) result.get()).generation(), equalTo(generation));
        });
        return ((Measured) result.get()).bytes();
    }

    /**
     * The generation the next snapshot of the shard starts from, with no snapshot running: the repository's, or
     * {@link ShardGenerations#NEW_SHARD_GEN} if it has nothing of the shard.
     */
    private static ShardGeneration expectedGeneration(String repository, ShardId shardId) {
        final RepositoryData repositoryData = getRepositoryData(getRepositoryOnMaster(repository));
        final IndexId indexId = repositoryData.getIndices().get(shardId.getIndexName());
        if (indexId == null) {
            return ShardGenerations.NEW_SHARD_GEN;
        }
        final ShardGeneration generation = repositoryData.shardGenerations().getShardGen(indexId, shardId.id());
        return generation == null ? ShardGenerations.NEW_SHARD_GEN : generation;
    }

    // The data blobs in the shard's folder of the repository, by name, with their lengths.
    private static Map<String, Long> dataBlobs(String repository, ShardId shardId) {
        final BlobStoreRepository blobStoreRepository = getRepositoryOnMaster(repository);
        final IndexId indexId = getRepositoryData(blobStoreRepository).getIndices().get(shardId.getIndexName());
        if (indexId == null) {
            return Map.of();
        }
        final ThreadPool threadPool = internalCluster().getCurrentMasterNodeInstance(ThreadPool.class);
        return safeGet(threadPool.executor(ThreadPool.Names.SNAPSHOT_META).submit(() -> {
            final Map<String, Long> blobs = new HashMap<>();
            for (BlobMetadata blob : blobStoreRepository.shardContainer(indexId, shardId.id())
                .listBlobsByPrefix(OperationPurpose.SNAPSHOT_DATA, BlobStoreRepository.UPLOADED_DATA_BLOB_PREFIX)
                .values()) {
                blobs.put(blob.name(), blob.length());
            }
            return blobs;
        }));
    }
}
