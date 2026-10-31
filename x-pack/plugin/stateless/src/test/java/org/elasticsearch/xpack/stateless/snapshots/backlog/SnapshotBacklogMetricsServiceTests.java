/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots.backlog;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.ProjectRepo;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ilm.OperationMode;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleMetadata;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicy;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicyMetadata;
import org.elasticsearch.xpack.stateless.MetricQuality;
import org.elasticsearch.xpack.stateless.snapshots.backlog.PublishSnapshotBacklogRequest.ShardBacklog;
import org.elasticsearch.xpack.stateless.snapshots.backlog.SnapshotBacklogMetricsService.ShardFigure;
import org.elasticsearch.xpack.stateless.snapshots.backlog.SnapshotBacklogMetricsService.Status;
import org.junit.Before;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.equalTo;

public class SnapshotBacklogMetricsServiceTests extends ESTestCase {

    private static final ProjectRepo REPOSITORY = new ProjectRepo(ProjectId.DEFAULT, "repo");
    private static final Index INDEX = new Index("index", "index-uuid");
    private static final ShardId SHARD = new ShardId(INDEX, 0);
    private static final ShardId OTHER_SHARD = new ShardId(INDEX, 1);
    private static final ShardGeneration GENERATION = new ShardGeneration("generation");

    private static final DiscoveryNode MASTER = DiscoveryNodeUtils.builder("master").ephemeralId("master-run-1").build();
    private static final DiscoveryNode NODE_A = DiscoveryNodeUtils.builder("a").ephemeralId("a-run-1").build();
    private static final DiscoveryNode NODE_B = DiscoveryNodeUtils.builder("b").ephemeralId("b-run-1").build();

    private final AtomicLong nanos = new AtomicLong();
    private final RecordingMeterRegistry meterRegistry = new RecordingMeterRegistry();
    private ClusterState applied = ClusterState.EMPTY_STATE;
    private final SnapshotBacklogMetricsService service = new SnapshotBacklogMetricsService(() -> applied, nanos::get, meterRegistry);

    private final ShardRouting onA = started(SHARD, NODE_A);
    private final ShardRouting otherOnA = started(OTHER_SHARD, NODE_A);

    @Before
    public void enable() {
        service.setEnabled(true);
    }

    public void testMeasuredFigure() {
        apply(state(List.of(NODE_A), onA, otherOnA));
        assertTrue(service.update(report(NODE_A, 1, SHARD, measured(onA, 100), OTHER_SHARD, measured(otherOnA, 0))));

        assertThat(
            figures(),
            equalTo(
                Map.of(
                    SHARD,
                    new ShardFigure(Status.MEASURED, GENERATION, 100),
                    OTHER_SHARD,
                    new ShardFigure(Status.MEASURED, GENERATION, 0)
                )
            )
        );
        assertThat(SnapshotBacklogMetricsService.quality(figures().values()), equalTo(MetricQuality.EXACT));
        assertThat(sizeGauge(), equalTo(Map.of("repo", 100L)));
        assertThat(shardsGauge(), equalTo(counts(Map.of(Status.MEASURED, 2L))));
    }

    public void testMeasuredZeroIsNotNotReported() {
        apply(state(List.of(NODE_A), onA, otherOnA));
        // the report has a measured 0 for one shard, and nothing for the other
        assertTrue(service.update(report(NODE_A, 1, SHARD, measured(onA, 0))));

        assertThat(figures().get(SHARD), equalTo(new ShardFigure(Status.MEASURED, GENERATION, 0)));
        assertThat(figures().get(OTHER_SHARD), equalTo(ShardFigure.without(Status.NOT_REPORTED)));
        assertThat(SnapshotBacklogMetricsService.quality(figures().values()), equalTo(MetricQuality.MINIMUM));
    }

    public void testNotMeasuredShardIsMinimum() {
        apply(state(List.of(NODE_A), onA));
        assertTrue(service.update(report(NODE_A, 1, SHARD, ShardBacklog.notMeasured(onA.allocationId().getId()))));

        assertThat(figures(), equalTo(Map.of(SHARD, ShardFigure.without(Status.NOT_MEASURED))));
        assertThat(figures().get(SHARD).quality(), equalTo(MetricQuality.MINIMUM));
        assertThat(sizeGauge(), equalTo(Map.of("repo", 0L)));
        assertThat(shardsGauge(), equalTo(counts(Map.of(Status.NOT_MEASURED, 1L))));
    }

    public void testReportFromUnknownNodeIsIgnored() {
        final ShardRouting onB = started(SHARD, NODE_B);
        apply(state(List.of(NODE_A), onA));
        assertFalse(service.update(report(NODE_B, 1, SHARD, measured(onB, 100))));

        // B joins and holds the very copy the report named: nothing was kept from before B joined.
        apply(state(List.of(NODE_A, NODE_B), onB));
        assertThat(figures().get(SHARD), equalTo(ShardFigure.without(Status.NOT_REPORTED)));
    }

    public void testReportFromAnEarlierRunOfANodeIsIgnored() {
        apply(state(List.of(NODE_A), onA));
        assertFalse(service.update(report("a", "a-run-0", 5, Map.of(REPOSITORY, Map.of(SHARD, measured(onA, 100))))));
        assertThat(figures().get(SHARD), equalTo(ShardFigure.without(Status.NOT_REPORTED)));

        // The report of the node's current run is kept. When the node restarts, its seq no starts again from 1.
        assertTrue(service.update(report(NODE_A, 5, SHARD, measured(onA, 100))));
        final DiscoveryNode restarted = DiscoveryNodeUtils.builder("a").ephemeralId("a-run-2").build();
        final ShardRouting afterRestart = started(SHARD, restarted);
        apply(state(List.of(restarted), afterRestart));
        assertFalse(service.update(report(NODE_A, 6, SHARD, measured(onA, 100))));
        assertTrue(service.update(report(restarted, 1, SHARD, measured(afterRestart, 200))));
        assertThat(figures().get(SHARD), equalTo(new ShardFigure(Status.MEASURED, GENERATION, 200)));
    }

    public void testOlderReportIsIgnored() {
        apply(state(List.of(NODE_A), onA));
        assertTrue(service.update(report(NODE_A, 2, SHARD, measured(onA, 200))));
        assertFalse(service.update(report(NODE_A, 1, SHARD, measured(onA, 100))));
        assertFalse(service.update(report(NODE_A, 2, SHARD, measured(onA, 300))));

        assertThat(figures().get(SHARD), equalTo(new ShardFigure(Status.MEASURED, GENERATION, 200)));
    }

    public void testReportOfANodeThatLeftIsDropped() {
        apply(state(List.of(NODE_A, NODE_B), onA));
        assertTrue(service.update(report(NODE_A, 1, SHARD, measured(onA, 100))));

        apply(state(List.of(NODE_B), unassigned(SHARD)));
        assertThat(figures().get(SHARD), equalTo(ShardFigure.without(Status.UNASSIGNED)));

        // Even if the node came back with the same run and the same shard copy, its earlier report is gone.
        apply(state(List.of(NODE_A, NODE_B), onA));
        assertThat(figures().get(SHARD), equalTo(ShardFigure.without(Status.NOT_REPORTED)));
    }

    public void testReportRacingTheNodeLeavingIsNotKept() {
        final ClusterState withNode = state(List.of(NODE_A), onA);
        final ClusterState withoutNode = state(List.of(), unassigned(SHARD));
        assertRacingReportIsNotKept(withNode, withoutNode);
    }

    public void testReportRacingADemotionIsNotKept() {
        final ClusterState asMaster = state(List.of(NODE_A), onA);
        final ClusterState demoted = ClusterState.builder(asMaster)
            .nodes(DiscoveryNodes.builder(asMaster.nodes()).masterNodeId(NODE_A.getId()))
            .build();
        assertRacingReportIsNotKept(asMaster, demoted);
    }

    /**
     * The store reads the cluster state once before keeping the report and once after. Here the second read sees the change, as when
     * the new state is set between the two but its listener hasn't run yet: the report must be dropped by the store itself.
     */
    private void assertRacingReportIsNotKept(ClusterState before, ClusterState after) {
        final AtomicInteger reads = new AtomicInteger();
        final Supplier<ClusterState> racing = () -> reads.getAndIncrement() == 0 ? before : after;
        final var racingService = new SnapshotBacklogMetricsService(racing, nanos::get, new RecordingMeterRegistry());
        racingService.setEnabled(true);
        racingService.clusterChanged(new ClusterChangedEvent("test", before, ClusterState.EMPTY_STATE));

        assertFalse(racingService.update(report(NODE_A, 1, SHARD, measured(onA, 100))));
        assertThat(reads.get(), equalTo(2));
        // Back to the first state, without any listener call: the report was not kept.
        assertThat(racingService.getFigures(before).get(REPOSITORY).get(SHARD), equalTo(ShardFigure.without(Status.NOT_REPORTED)));
    }

    public void testFigureOfAnEarlierCopyIsNotUsed() {
        apply(state(List.of(NODE_A), onA));
        assertTrue(service.update(report(NODE_A, 1, SHARD, measured(onA, 100))));

        // The shard moves away and comes back to the same node as a new copy before the node reports again.
        final ShardRouting backOnA = started(SHARD, NODE_A);
        apply(state(List.of(NODE_A), backOnA));
        assertThat(figures().get(SHARD), equalTo(ShardFigure.without(Status.NOT_REPORTED)));

        assertTrue(service.update(report(NODE_A, 2, SHARD, measured(backOnA, 300))));
        assertThat(figures().get(SHARD), equalTo(new ShardFigure(Status.MEASURED, GENERATION, 300)));
    }

    public void testRelocation() {
        final ShardRouting relocating = onA.relocate(NODE_B.getId(), 0, ShardRouting.RecoveryPriority.RELOCATION_CAN_REMAIN_NO);
        apply(state(List.of(NODE_A, NODE_B), relocating));
        assertTrue(service.update(report(NODE_A, 1, SHARD, measured(onA, 100))));
        // While the shard relocates, the source copy is still the primary.
        assertThat(figures().get(SHARD), equalTo(new ShardFigure(Status.MEASURED, GENERATION, 100)));

        // Once the target is the primary, the source's figure no longer counts, until the target reports.
        final ShardRouting onB = relocating.getTargetRelocatingShard().moveToStarted(0);
        apply(state(List.of(NODE_A, NODE_B), onB));
        assertThat(figures().get(SHARD), equalTo(ShardFigure.without(Status.NOT_REPORTED)));
        assertTrue(service.update(report(NODE_B, 1, SHARD, ShardBacklog.notMeasured(onB.allocationId().getId()))));
        assertThat(figures().get(SHARD), equalTo(ShardFigure.without(Status.NOT_MEASURED)));
        final ShardGeneration measuredOnB = new ShardGeneration("measured-on-b");
        assertTrue(service.update(report(NODE_B, 2, SHARD, new ShardBacklog(onB.allocationId().getId(), measuredOnB, 150))));
        assertThat(figures().get(SHARD), equalTo(new ShardFigure(Status.MEASURED, measuredOnB, 150)));
    }

    public void testStaleReportIsMinimum() {
        apply(state(List.of(NODE_A), onA));
        assertTrue(service.update(report(NODE_A, 1, SHARD, measured(onA, 100))));

        nanos.addAndGet(SnapshotBacklogMetricsService.STALE_AFTER.nanos());
        assertThat(figures().get(SHARD), equalTo(new ShardFigure(Status.MEASURED, GENERATION, 100)));
        nanos.incrementAndGet();
        assertThat(figures().get(SHARD), equalTo(ShardFigure.without(Status.STALE)));
        assertThat(shardsGauge(), equalTo(counts(Map.of(Status.STALE, 1L))));

        assertTrue(service.update(report(NODE_A, 2, SHARD, measured(onA, 100))));
        assertThat(figures().get(SHARD), equalTo(new ShardFigure(Status.MEASURED, GENERATION, 100)));
    }

    public void testUnassignedPrimaryIsMinimum() {
        apply(state(List.of(NODE_A), unassigned(SHARD), otherOnA));
        assertTrue(service.update(report(NODE_A, 1, OTHER_SHARD, measured(otherOnA, 100))));

        assertThat(figures().get(SHARD), equalTo(ShardFigure.without(Status.UNASSIGNED)));
        assertThat(SnapshotBacklogMetricsService.quality(figures().values()), equalTo(MetricQuality.MINIMUM));
        assertThat(shardsGauge(), equalTo(counts(Map.of(Status.MEASURED, 1L, Status.UNASSIGNED, 1L))));
    }

    public void testOnlyTrackedRepositories() {
        final ProjectRepo notNamedByAPolicy = new ProjectRepo(ProjectId.DEFAULT, "registered-only");
        apply(state(List.of(NODE_A), onA));
        assertTrue(
            service.update(
                report(
                    "a",
                    "a-run-1",
                    1,
                    Map.of(REPOSITORY, Map.of(SHARD, measured(onA, 100)), notNamedByAPolicy, Map.of(SHARD, measured(onA, 200)))
                )
            )
        );

        assertThat(service.getFigures(applied).keySet(), equalTo(Set.of(REPOSITORY)));
        assertThat(sizeGauge(), equalTo(Map.of("repo", 100L)));
    }

    public void testIndexWithoutRoutingTableIsLeftOut() {
        final ClusterState state = state(List.of(NODE_A), onA);
        final IndexMetadata closed = IndexMetadata.builder("closed")
            .settings(indexSettings(IndexVersion.current(), "closed-uuid", 1, 0))
            .state(IndexMetadata.State.CLOSE)
            .build();
        apply(
            ClusterState.builder(state)
                .metadata(Metadata.builder(state.metadata()).put(ProjectMetadata.builder(state.metadata().getProject()).put(closed, false)))
                .build()
        );
        assertThat(figures().keySet(), equalTo(Set.of(SHARD)));
    }

    public void testDemotedMasterForgetsReports() {
        final ClusterState asMaster = state(List.of(NODE_A), onA);
        apply(asMaster);
        assertTrue(service.update(report(NODE_A, 1, SHARD, measured(onA, 100))));

        final ClusterState demoted = ClusterState.builder(asMaster)
            .nodes(DiscoveryNodes.builder(asMaster.nodes()).masterNodeId(NODE_A.getId()))
            .build();
        apply(demoted);
        assertThat(service.getFigures(applied), anEmptyMap());
        assertThat(sizeGauge(), anEmptyMap());
        assertFalse(service.update(report(NODE_A, 2, SHARD, measured(onA, 100))));

        // Elected master again: the store starts empty.
        apply(asMaster);
        assertThat(figures().get(SHARD), equalTo(ShardFigure.without(Status.NOT_REPORTED)));
    }

    public void testStateNotRecoveredForgetsReports() {
        final ClusterState recovered = state(List.of(NODE_A), onA);
        apply(recovered);
        assertTrue(service.update(report(NODE_A, 1, SHARD, measured(onA, 100))));

        apply(
            ClusterState.builder(recovered).blocks(ClusterBlocks.builder().addGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)).build()
        );
        assertThat(service.getFigures(applied), anEmptyMap());

        apply(recovered);
        assertThat(figures().get(SHARD), equalTo(ShardFigure.without(Status.NOT_REPORTED)));
    }

    public void testTurningOffForgetsAndIgnores() {
        apply(state(List.of(NODE_A), onA));
        assertTrue(service.update(report(NODE_A, 1, SHARD, measured(onA, 100))));

        service.setEnabled(false);
        assertThat(service.getFigures(applied), anEmptyMap());
        assertThat(sizeGauge(), anEmptyMap());
        assertFalse(service.update(report(NODE_A, 2, SHARD, measured(onA, 100))));

        // Turned on again: the store starts empty.
        service.setEnabled(true);
        assertThat(figures().get(SHARD), equalTo(ShardFigure.without(Status.NOT_REPORTED)));
    }

    private Map<ShardId, ShardFigure> figures() {
        return service.getFigures(applied).get(REPOSITORY);
    }

    private void apply(ClusterState state) {
        final ClusterState previous = applied;
        // as in ClusterApplierService: the new state is set before the listeners are called
        applied = ClusterState.builder(state).version(previous.version() + 1).build();
        service.clusterChanged(new ClusterChangedEvent("test", applied, previous));
    }

    private static ShardRouting started(ShardId shardId, DiscoveryNode node) {
        return TestShardRouting.newShardRouting(shardId, node.getId(), true, ShardRoutingState.STARTED);
    }

    private static ShardRouting unassigned(ShardId shardId) {
        return TestShardRouting.newShardRouting(shardId, null, true, ShardRoutingState.UNASSIGNED);
    }

    private static ShardBacklog measured(ShardRouting copy, long bytes) {
        return new ShardBacklog(copy.allocationId().getId(), GENERATION, bytes);
    }

    private static PublishSnapshotBacklogRequest report(DiscoveryNode node, long seqNo, ShardId shardId, ShardBacklog backlog) {
        return report(node.getId(), node.getEphemeralId(), seqNo, Map.of(REPOSITORY, Map.of(shardId, backlog)));
    }

    private static PublishSnapshotBacklogRequest report(
        DiscoveryNode node,
        long seqNo,
        ShardId shardId,
        ShardBacklog backlog,
        ShardId otherShardId,
        ShardBacklog otherBacklog
    ) {
        return report(node.getId(), node.getEphemeralId(), seqNo, Map.of(REPOSITORY, Map.of(shardId, backlog, otherShardId, otherBacklog)));
    }

    private static PublishSnapshotBacklogRequest report(
        String nodeId,
        String ephemeralId,
        long seqNo,
        Map<ProjectRepo, Map<ShardId, ShardBacklog>> shards
    ) {
        return new PublishSnapshotBacklogRequest(TEST_REQUEST_TIMEOUT, nodeId, ephemeralId, seqNo, shards);
    }

    private Map<String, Long> sizeGauge() {
        final Map<String, Long> byRepository = new HashMap<>();
        for (Measurement measurement : measurements(SnapshotBacklogMetricsService.SIZE_METRIC)) {
            byRepository.put((String) measurement.attributes().get("repo_name"), measurement.getLong());
        }
        return byRepository;
    }

    private Map<String, Long> shardsGauge() {
        final Map<String, Long> byStatus = new HashMap<>();
        for (Measurement measurement : measurements(SnapshotBacklogMetricsService.SHARDS_METRIC)) {
            assertThat(measurement.attributes().get("repo_name"), equalTo("repo"));
            byStatus.put((String) measurement.attributes().get("status"), measurement.getLong());
        }
        return byStatus;
    }

    private static Map<String, Long> counts(Map<Status, Long> nonZero) {
        final Map<String, Long> counts = new HashMap<>();
        for (Status status : Status.values()) {
            counts.put(status.label(), nonZero.getOrDefault(status, 0L));
        }
        return counts;
    }

    private List<Measurement> measurements(String name) {
        meterRegistry.getRecorder().resetCalls();
        meterRegistry.getRecorder().collect();
        return meterRegistry.getRecorder().getMeasurements(InstrumentType.LONG_ASYNC_GAUGE, name);
    }

    /**
     * A cluster state in which this node is the elected master, the given index nodes are in the cluster, and the default project has
     * the index with the given primaries, a repository named by an SLM policy, and a repository no policy names.
     */
    private static ClusterState state(List<DiscoveryNode> indexNodes, ShardRouting... primaries) {
        final DiscoveryNodes.Builder nodes = DiscoveryNodes.builder().add(MASTER).localNodeId(MASTER.getId()).masterNodeId(MASTER.getId());
        indexNodes.forEach(nodes::add);
        final List<RepositoryMetadata> repositories = new ArrayList<>();
        for (String name : List.of("repo", "registered-only")) {
            repositories.add(new RepositoryMetadata(name, name + "-uuid", "fs", Settings.EMPTY));
        }
        final SnapshotLifecyclePolicyMetadata policy = SnapshotLifecyclePolicyMetadata.builder()
            .setPolicy(new SnapshotLifecyclePolicy("policy", "snap", "0 0/30 * * * ?", "repo", null, null))
            .setVersion(1)
            .setModifiedDate(0)
            .build();
        final IndexMetadata indexMetadata = IndexMetadata.builder(INDEX.getName())
            .settings(indexSettings(IndexVersion.current(), INDEX.getUUID(), primaries.length, 0))
            .build();
        final IndexRoutingTable.Builder indexRoutingTable = IndexRoutingTable.builder(INDEX);
        for (ShardRouting primary : primaries) {
            indexRoutingTable.addShard(primary);
        }
        return ClusterState.builder(ClusterName.DEFAULT)
            .nodes(nodes)
            .metadata(
                Metadata.builder()
                    .put(
                        ProjectMetadata.builder(ProjectId.DEFAULT)
                            .putCustom(RepositoriesMetadata.TYPE, new RepositoriesMetadata(repositories))
                            .putCustom(
                                SnapshotLifecycleMetadata.TYPE,
                                new SnapshotLifecycleMetadata(Map.of("policy", policy), OperationMode.RUNNING, null)
                            )
                            .put(indexMetadata, false)
                    )
            )
            .routingTable(RoutingTable.builder().add(indexRoutingTable))
            .build();
    }
}
