/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots.backlog;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.MergePolicyConfig;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.ProjectRepo;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.elasticsearch.xpack.stateless.MetricQuality;
import org.elasticsearch.xpack.stateless.StatelessMockRepositoryPlugin;
import org.elasticsearch.xpack.stateless.StatelessMockRepositoryStrategy;
import org.elasticsearch.xpack.stateless.commits.HollowShardsService;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;
import org.elasticsearch.xpack.stateless.snapshots.StatelessSnapshotSettings.StatelessSnapshotEnabledStatus;
import org.elasticsearch.xpack.stateless.snapshots.backlog.ShardBaselineCache.Measured;
import org.elasticsearch.xpack.stateless.snapshots.backlog.ShardBaselineCache.Result;
import org.elasticsearch.xpack.stateless.snapshots.backlog.SnapshotBacklogMetricsService.ShardFigure;
import org.elasticsearch.xpack.stateless.snapshots.backlog.SnapshotBacklogMetricsService.Status;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase.createFullSnapshot;
import static org.elasticsearch.xpack.stateless.snapshots.StatelessSnapshotSettings.RELOCATION_DURING_SNAPSHOT_ENABLED_SETTING;
import static org.elasticsearch.xpack.stateless.snapshots.StatelessSnapshotSettings.STATELESS_SNAPSHOT_ENABLED_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

/**
 * Checks that the figures the index nodes measure reach the master's {@link SnapshotBacklogMetricsService}. Each primary's figure on
 * the master is the one the node holding that primary measured. A figure from a copy the routing table no longer names, after a
 * relocation or once a node has left, is never used.
 * <p>
 * Each test runs evaluations itself instead of waiting for the 30 second schedule. The scheduled evaluations still run, so every
 * check that reads the latest report is retried with {@code assertBusy}. Merges are off, so a figure doesn't change once the index is
 * written. SLM itself isn't loaded: the policies are put into the cluster state directly, as in {@link SnapshotBacklogIT}.
 */
public class SnapshotBacklogMasterIT extends AbstractStatelessPluginIntegTestCase {

    private static final String REPOSITORY = "repo";
    private static final ProjectRepo PROJECT_REPO = new ProjectRepo(ProjectId.DEFAULT, REPOSITORY);
    private static final String INDEX = "index";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(SnapshotBacklogIT.SlmMetadataPlugin.class);
        plugins.add(TestTelemetryPlugin.class);
        // Registers index.merge.enabled.
        plugins.add(InternalSettingsPlugin.class);
        // The mock object store, whose reads a test can block.
        plugins.add(StatelessMockRepositoryPlugin.class);
        return plugins;
    }

    @Override
    protected boolean addMockFsRepository() {
        return false;
    }

    // Measures the backlog, and keeps shards from going hollow.
    private static Settings backlogNodeSettings() {
        return Settings.builder()
            .put(SnapshotBacklogTracker.ENABLED_SETTING.getKey(), true)
            .put(HollowShardsService.STATELESS_HOLLOW_INDEX_SHARDS_ENABLED.getKey(), false)
            .build();
    }

    public void testMasterKeepsWhatEachNodeMeasured() throws Exception {
        final String master = startMasterOnlyNode(backlogNodeSettings());
        final String firstNode = startIndexNode(backlogNodeSettings());
        final String secondNode = startIndexNode(backlogNodeSettings());
        createRepository(REPOSITORY, "fs");
        SnapshotBacklogIT.putPolicies(REPOSITORY);
        // one primary on each index node
        createIndex(
            INDEX,
            indexSettings(2, 0).put("index.routing.allocation.total_shards_per_node", 1)
                .put(MergePolicyConfig.INDEX_MERGE_ENABLED, false)
                .build()
        );
        ensureGreen(INDEX);
        write();

        assertBusy(() -> {
            evaluate(firstNode);
            evaluate(secondNode);
            final Map<ShardId, ShardFigure> figures = masterFigures();
            assertThat(figures.size(), equalTo(2));
            for (Map.Entry<ShardId, ShardFigure> shardAndFigure : figures.entrySet()) {
                // the figure the node holding the primary measured, with its generation, nothing else
                final Measured measured = measuredOn(nodeHoldingPrimary(shardAndFigure.getKey()), shardAndFigure.getKey());
                assertThat(shardAndFigure.getValue(), equalTo(new ShardFigure(Status.MEASURED, measured.generation(), measured.bytes())));
                assertThat(measured.bytes(), greaterThan(0L));
            }
            assertThat(SnapshotBacklogMetricsService.quality(figures.values()), equalTo(MetricQuality.EXACT));
        });

        // The master metrics show the same total. Retried, in case a scheduled report arrives between the two reads.
        final TestTelemetryPlugin telemetry = getTelemetryPlugin(master);
        assertBusy(() -> {
            final long total = masterFigures().values().stream().mapToLong(ShardFigure::bytes).sum();
            telemetry.resetMeter();
            telemetry.collect();
            final List<Measurement> size = telemetry.getLongAsyncGaugeMeasurement(SnapshotBacklogMetricsService.SIZE_METRIC);
            assertThat(size.size(), equalTo(1));
            assertThat(size.get(0).getLong(), equalTo(total));
            assertThat(size.get(0).attributes(), equalTo(Map.of("repo_name", REPOSITORY)));
        });
    }

    public void testRelocatedShardIsNotReportedUntilTheNewNodeReports() throws Exception {
        startMasterOnlyNode(backlogNodeSettings());
        final String firstNode = startIndexNode(backlogNodeSettings());
        createRepository(REPOSITORY, "fs");
        SnapshotBacklogIT.putPolicies(REPOSITORY);
        createIndex(INDEX, indexSettings(1, 0).put(MergePolicyConfig.INDEX_MERGE_ENABLED, false).build());
        ensureGreen(INDEX);
        write();
        final ShardId shardId = new ShardId(resolveIndex(INDEX), 0);
        assertBusy(() -> {
            evaluate(firstNode);
            assertThat(masterFigures().get(shardId).status(), equalTo(Status.MEASURED));
        });

        // The master drops every report of the second node until the test lets them through.
        final String secondNode = startIndexNode(backlogNodeSettings());
        final String secondNodeId = nodeId(secondNode);
        final AtomicInteger dropped = new AtomicInteger();
        final MockTransportService masterTransport = MockTransportService.getInstance(internalCluster().getMasterName());
        masterTransport.addRequestHandlingBehavior(TransportPublishSnapshotBacklogAction.NAME, (handler, request, channel, task) -> {
            if (((PublishSnapshotBacklogRequest) request).nodeId().equals(secondNodeId)) {
                dropped.incrementAndGet();
                channel.sendResponse(ActionResponse.Empty.INSTANCE);
            } else {
                handler.messageReceived(request, channel, task);
            }
        });

        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", firstNode), INDEX);
        internalCluster().awaitNodeVacated(INDEX, firstNode);
        ensureGreen(INDEX);
        // The first node no longer has the shard, and the second node's reports don't arrive: the figure is not there.
        assertBusy(() -> {
            evaluate(firstNode);
            evaluate(secondNode);
            assertThat(dropped.get(), greaterThan(0));
            measuredOn(secondNode, shardId);
        });
        assertThat(masterFigures().get(shardId), equalTo(ShardFigure.without(Status.NOT_REPORTED)));
        assertThat(masterFigures().get(shardId).quality(), equalTo(MetricQuality.MINIMUM));

        // Let the reports through: the figure is the one the second node measured.
        masterTransport.clearAllRules();
        assertBusy(() -> {
            evaluate(secondNode);
            final Measured measured = measuredOn(secondNode, shardId);
            assertThat(masterFigures().get(shardId), equalTo(new ShardFigure(Status.MEASURED, measured.generation(), measured.bytes())));
        });
    }

    public void testANodeThatLeavesIsForgotten() throws Exception {
        startMasterOnlyNode(backlogNodeSettings());
        final String firstNode = startIndexNode(backlogNodeSettings());
        createRepository(REPOSITORY, "fs");
        SnapshotBacklogIT.putPolicies(REPOSITORY);
        createIndex(INDEX, indexSettings(1, 0).put(MergePolicyConfig.INDEX_MERGE_ENABLED, false).build());
        ensureGreen(INDEX);
        write();
        final ShardId shardId = new ShardId(resolveIndex(INDEX), 0);
        final String firstNodeId = nodeId(firstNode);
        assertBusy(() -> {
            evaluate(firstNode);
            assertThat(masterFigures().get(shardId).status(), equalTo(Status.MEASURED));
        });
        assertThat(masterService().getReportingNodes(), hasItem(firstNodeId));

        final String secondNode = startIndexNode(backlogNodeSettings());
        internalCluster().stopNode(firstNode);
        assertBusy(() -> assertThat(masterService().getReportingNodes(), not(hasItem(firstNodeId))));

        // The shard starts on the second node, which reports it.
        ensureGreen(INDEX);
        assertBusy(() -> {
            evaluate(secondNode);
            final Measured measured = measuredOn(secondNode, shardId);
            assertThat(masterFigures().get(shardId), equalTo(new ShardFigure(Status.MEASURED, measured.generation(), measured.bytes())));
        });
    }

    public void testHeldFigureStaysThroughWritesAndARelocationDuringABackup() throws Exception {
        // Snapshots read the shard's data from the object store, which the test can block, and keep running when the shard relocates.
        final Settings settings = Settings.builder()
            .put(backlogNodeSettings())
            .put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK)
            .put(STATELESS_SNAPSHOT_ENABLED_SETTING.getKey(), StatelessSnapshotEnabledStatus.ENABLED)
            .put(RELOCATION_DURING_SNAPSHOT_ENABLED_SETTING.getKey(), true)
            .build();
        startMasterOnlyNode(settings);
        final String firstNode = startIndexNode(settings);
        createRepository(REPOSITORY, "fs");
        SnapshotBacklogIT.putPolicies(REPOSITORY);
        createIndex(INDEX, indexSettings(1, 0).put(MergePolicyConfig.INDEX_MERGE_ENABLED, false).build());
        ensureGreen(INDEX);
        write();
        createFullSnapshot(logger, REPOSITORY, "snap-1");
        write();
        final ShardId shardId = new ShardId(resolveIndex(INDEX), 0);
        final ShardGeneration startGeneration = SnapshotBacklogIT.expectedGeneration(REPOSITORY, shardId);
        assertBusy(() -> {
            evaluate(firstNode);
            assertThat(masterFigures().get(shardId).status(), equalTo(Status.MEASURED));
            assertThat(masterFigures().get(shardId).generation(), equalTo(startGeneration));
        });
        final ShardFigure beforeTheStart = masterFigures().get(shardId);
        final String secondNode = startIndexNode(settings);

        // The second snapshot blocks while reading the shard's data on the first node, so the shard stays active in the snapshot.
        final CountDownLatch reading = new CountDownLatch(1);
        final CountDownLatch unblock = new CountDownLatch(1);
        setNodeRepositoryStrategy(firstNode, new StatelessMockRepositoryStrategy() {
            @Override
            public InputStream blobContainerReadBlob(
                CheckedSupplier<InputStream, IOException> originalSupplier,
                OperationPurpose purpose,
                String blobName
            ) throws IOException {
                blockSnapshotRead(purpose);
                return super.blobContainerReadBlob(originalSupplier, purpose, blobName);
            }

            @Override
            public InputStream blobContainerReadBlob(
                CheckedSupplier<InputStream, IOException> originalSupplier,
                OperationPurpose purpose,
                String blobName,
                long position,
                long length
            ) throws IOException {
                blockSnapshotRead(purpose);
                return super.blobContainerReadBlob(originalSupplier, purpose, blobName, position, length);
            }

            private void blockSnapshotRead(OperationPurpose purpose) {
                if (purpose == OperationPurpose.SNAPSHOT_DATA) {
                    reading.countDown();
                    safeAwait(unblock);
                }
            }
        });
        final var snapshot = clusterAdmin().prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, REPOSITORY, "snap-2")
            .setWaitForCompletion(true)
            .execute();
        safeAwait(reading);
        try {
            assertBusy(() -> assertThat(masterFigures().get(shardId), equalTo(held(beforeTheStart))));

            // New writes raise the node's own figure, but the master keeps the figure from the start.
            write();
            assertBusy(() -> {
                evaluate(firstNode);
                assertThat(measuredOn(firstNode, shardId).bytes(), greaterThan(beforeTheStart.bytes()));
            });
            assertThat(masterFigures().get(shardId), equalTo(held(beforeTheStart)));

            // The master also keeps that figure when the shard relocates: the upload carries on from the first node.
            updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", firstNode), INDEX);
            internalCluster().awaitNodeVacated(INDEX, firstNode);
            ensureGreen(INDEX);
            assertBusy(() -> {
                evaluate(secondNode);
                measuredOn(secondNode, shardId);
            });
            assertThat(masterFigures().get(shardId), equalTo(held(beforeTheStart)));
        } finally {
            // released even if a check fails, so the blocked snapshot thread doesn't hold up the cluster's shutdown
            unblock.countDown();
        }

        // Once the snapshot ends, the figure is live again: measured by the second node against the new generation.
        assertThat(safeGet(snapshot).getSnapshotInfo().state(), equalTo(SnapshotState.SUCCESS));
        final ShardGeneration nextGeneration = SnapshotBacklogIT.expectedGeneration(REPOSITORY, shardId);
        assertThat(nextGeneration, not(equalTo(startGeneration)));
        assertBusy(() -> {
            evaluate(secondNode);
            final Measured measured = measuredOn(secondNode, shardId);
            assertThat(measured.generation(), equalTo(nextGeneration));
            assertThat(masterFigures().get(shardId), equalTo(new ShardFigure(Status.MEASURED, nextGeneration, measured.bytes())));
        });
    }

    private static ShardFigure held(ShardFigure measured) {
        return new ShardFigure(Status.HELD, measured.generation(), measured.bytes());
    }

    private void write() {
        indexDocs(INDEX, between(10, 100));
        flush(INDEX);
    }

    // Runs one evaluation on the node, on the pool the tracker runs on; the node sends its report at the end.
    private static void evaluate(String node) {
        final SnapshotBacklogTracker tracker = internalCluster().getInstance(SnapshotBacklogTracker.class, node);
        final ThreadPool threadPool = internalCluster().getInstance(ThreadPool.class, node);
        safeGet(threadPool.executor(ThreadPool.Names.SNAPSHOT_META).submit(tracker::evaluate));
    }

    // The figure the node measured for the shard. While the shard isn't measured, the assertion fails, so assertBusy tries again.
    private static Measured measuredOn(String node, ShardId shardId) {
        final Result result = internalCluster().getInstance(SnapshotBacklogTracker.class, node)
            .getResults()
            .getOrDefault(PROJECT_REPO, Map.of())
            .get(shardId);
        assertThat(result, instanceOf(Measured.class));
        return (Measured) result;
    }

    private static SnapshotBacklogMetricsService masterService() {
        return internalCluster().getCurrentMasterNodeInstance(SnapshotBacklogMetricsService.class);
    }

    private static Map<ShardId, ShardFigure> masterFigures() {
        final ClusterState state = internalCluster().getCurrentMasterNodeInstance(ClusterService.class).state();
        return masterService().getFigures(state).getOrDefault(PROJECT_REPO, Map.of());
    }

    private static String nodeHoldingPrimary(ShardId shardId) {
        final ClusterState state = internalCluster().getCurrentMasterNodeInstance(ClusterService.class).state();
        final String nodeId = state.routingTable(ProjectId.DEFAULT).shardRoutingTable(shardId).primaryShard().currentNodeId();
        return state.nodes().get(nodeId).getName();
    }

    private static String nodeId(String node) {
        return internalCluster().getInstance(ClusterService.class, node).localNode().getId();
    }
}
