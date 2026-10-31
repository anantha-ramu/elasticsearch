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
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress.ShardSnapshotStatus;
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
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.ProjectRepo;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.repositories.ShardSnapshotResult;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
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

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.equalTo;

/**
 * The hold during a running backup ({@link SnapshotBacklogHolds}), through the master store that uses it: one index node holds two
 * primaries, the node's reports are fed to the store, and running snapshots are put in the cluster state.
 */
public class SnapshotBacklogHoldsTests extends ESTestCase {

    private static final ProjectRepo REPOSITORY = new ProjectRepo(ProjectId.DEFAULT, "repo");
    private static final Index INDEX = new Index("index", "index-uuid");
    private static final ShardId SHARD = new ShardId(INDEX, 0);
    private static final ShardId OTHER_SHARD = new ShardId(INDEX, 1);
    private static final ShardGeneration FIRST = new ShardGeneration("first");
    private static final ShardGeneration SECOND = new ShardGeneration("second");

    private static final DiscoveryNode MASTER = DiscoveryNodeUtils.builder("master").ephemeralId("master-run").build();
    private static final DiscoveryNode NODE = DiscoveryNodeUtils.builder("node").ephemeralId("node-run").build();

    private final ShardRouting primary = TestShardRouting.newShardRouting(SHARD, NODE.getId(), true, ShardRoutingState.STARTED);
    private final ShardRouting otherPrimary = TestShardRouting.newShardRouting(OTHER_SHARD, NODE.getId(), true, ShardRoutingState.STARTED);
    private final AtomicLong seqNo = new AtomicLong();
    private ClusterState applied = ClusterState.EMPTY_STATE;
    private final SnapshotBacklogMetricsService service = new SnapshotBacklogMetricsService(
        () -> applied,
        () -> 0L,
        new RecordingMeterRegistry()
    );

    @Before
    public void enable() {
        service.setEnabled(true);
    }

    public void testSingleBackupHoldsTheFigureFromBeforeTheStart() {
        apply(state());
        report(FIRST, 100);

        apply(state(snapshot("snap-1", Map.of(SHARD, active(FIRST)))));
        assertThat(figure(), equalTo(new ShardFigure(Status.HELD, FIRST, 100)));

        // New writes don't change the held figure while the shard's snapshot runs.
        report(FIRST, 300);
        assertThat(figure(), equalTo(new ShardFigure(Status.HELD, FIRST, 100)));

        // Once the snapshot ends, the shard counts its live figure again.
        apply(state());
        assertThat(figure(), equalTo(new ShardFigure(Status.MEASURED, FIRST, 300)));
    }

    public void testOverlapWaitsForAFigureAtTheNewGeneration() {
        apply(state());
        report(FIRST, 100);
        apply(
            state(snapshot("snap-1", Map.of(SHARD, active(FIRST), OTHER_SHARD, active(FIRST))), snapshot("snap-2", Map.of(SHARD, queued())))
        );
        assertThat(figure(), equalTo(new ShardFigure(Status.HELD, FIRST, 100)));

        // The shard finishes in the first snapshot and starts in the second in one update, from the generation the first one wrote.
        apply(
            state(
                snapshot("snap-1", Map.of(SHARD, succeeded(SECOND), OTHER_SHARD, active(FIRST))),
                snapshot("snap-2", Map.of(SHARD, active(SECOND)))
            )
        );
        assertThat(figure(), equalTo(ShardFigure.without(Status.HOLD_PENDING)));
        assertThat(figure().quality(), equalTo(MetricQuality.MINIMUM));

        // A figure measured against the earlier generation is never held.
        report(FIRST, 400);
        assertThat(figure(), equalTo(ShardFigure.without(Status.HOLD_PENDING)));

        // The first figure measured against the new generation is held, and later ones don't change it.
        report(SECOND, 50);
        assertThat(figure(), equalTo(new ShardFigure(Status.HELD, SECOND, 50)));
        report(SECOND, 70);
        assertThat(figure(), equalTo(new ShardFigure(Status.HELD, SECOND, 50)));
    }

    public void testFinishedShardCountsZeroUntilTheBackupEnds() {
        apply(state());
        report(FIRST, 100);
        apply(state(snapshot("snap-1", Map.of(SHARD, succeeded(SECOND), OTHER_SHARD, active(FIRST)))));

        assertThat(figure(), equalTo(ShardFigure.without(Status.BACKED_UP)));
        // A finished shard is exact: it doesn't make the totals minimum.
        assertThat(figure().quality(), equalTo(MetricQuality.EXACT));
    }

    public void testFailedShardCountsZeroUntilTheBackupEnds() {
        apply(state());
        report(FIRST, 100);
        apply(state(snapshot("snap-1", Map.of(SHARD, failed(FIRST), OTHER_SHARD, active(FIRST)))));

        assertThat(figure(), equalTo(ShardFigure.without(Status.BACKED_UP)));
    }

    public void testQueuedShardUsesTheLiveFigure() {
        apply(state());
        report(FIRST, 100);
        // Queued with nothing active, for example behind a running deletion: no start generation yet.
        apply(state(snapshot("snap-1", Map.of(SHARD, queued()))));

        assertThat(figure(), equalTo(new ShardFigure(Status.MEASURED, FIRST, 100)));
    }

    public void testShardFinishedInOneBackupAndQueuedInAnotherUsesTheLiveFigure() {
        apply(state());
        report(FIRST, 100);
        // Finished in the first snapshot, queued in the second with nothing active: not finished in every snapshot that includes it.
        apply(
            state(
                snapshot("snap-1", Map.of(SHARD, succeeded(SECOND), OTHER_SHARD, active(FIRST))),
                snapshot("snap-2", Map.of(SHARD, queued()))
            )
        );

        assertThat(figure(), equalTo(new ShardFigure(Status.MEASURED, FIRST, 100)));
    }

    public void testAPendingHoldIsFilledOnce() {
        final SnapshotBacklogHolds holds = new SnapshotBacklogHolds();
        final ClusterState running = state(snapshot("snap-1", Map.of(SHARD, active(SECOND))));
        // No figure at the start generation yet: the hold is pending.
        holds.onClusterState(running, false, (projectRepo, shardId) -> new ShardFigure(Status.MEASURED, FIRST, 100));
        assertThat(holds.getFigures(running, REPOSITORY).get(SHARD), equalTo(ShardFigure.without(Status.HOLD_PENDING)));

        holds.onReport((projectRepo, shardId) -> new ShardFigure(Status.MEASURED, SECOND, 50));
        holds.onReport((projectRepo, shardId) -> new ShardFigure(Status.MEASURED, SECOND, 70));
        // A later state with the same snapshot doesn't fill the hold again either.
        holds.onClusterState(running, false, (projectRepo, shardId) -> new ShardFigure(Status.MEASURED, SECOND, 90));

        assertThat(holds.getFigures(running, REPOSITORY).get(SHARD), equalTo(new ShardFigure(Status.HELD, SECOND, 50)));
    }

    public void testActiveShardWithoutAHoldYetIsPending() {
        apply(state());
        report(FIRST, 100);
        // The store reads a state whose listener hasn't run yet, as the cluster service makes a new state visible first.
        final ClusterState notYetApplied = state(snapshot("snap-1", Map.of(SHARD, active(FIRST))));

        assertThat(service.getFigures(notYetApplied).get(REPOSITORY).get(SHARD), equalTo(ShardFigure.without(Status.HOLD_PENDING)));
    }

    public void testAfterFailoverActiveShardsAreNotHeld() {
        // The first state this master processes already has the shard active.
        apply(state(snapshot("snap-1", Map.of(SHARD, active(FIRST)))));
        report(FIRST, 100);
        assertThat(figure(), equalTo(ShardFigure.without(Status.HOLD_LOST)));
        assertThat(figure().quality(), equalTo(MetricQuality.MINIMUM));

        // A shard that becomes active after that is held as usual.
        apply(state(snapshot("snap-1", Map.of(SHARD, active(FIRST))), snapshot("snap-2", Map.of(OTHER_SHARD, active(FIRST)))));
        assertThat(service.getFigures(applied).get(REPOSITORY).get(OTHER_SHARD), equalTo(new ShardFigure(Status.HELD, FIRST, 200)));
        assertThat(figure(), equalTo(ShardFigure.without(Status.HOLD_LOST)));
    }

    public void testTurningOnDuringABackupIsLikeAFailover() {
        apply(state());
        report(FIRST, 100);
        service.setEnabled(false);
        apply(state(snapshot("snap-1", Map.of(SHARD, active(FIRST)))));

        // Turning the setting on comes with a new cluster state, the first the holds see.
        service.setEnabled(true);
        apply(state(snapshot("snap-1", Map.of(SHARD, active(FIRST)))));
        report(FIRST, 100);
        assertThat(figure(), equalTo(ShardFigure.without(Status.HOLD_LOST)));
    }

    public void testClonesAreIgnored() {
        apply(state());
        report(FIRST, 100);
        final SnapshotsInProgress.Entry clone = SnapshotsInProgress.startClone(
            new Snapshot(ProjectId.DEFAULT, "repo", new SnapshotId("clone", "clone-uuid")),
            new SnapshotId("source", "source-uuid"),
            Map.of(INDEX.getName(), new IndexId(INDEX.getName(), "index-id")),
            0,
            1,
            IndexVersion.current()
        );
        apply(state(clone));

        assertThat(figure(), equalTo(new ShardFigure(Status.MEASURED, FIRST, 100)));
    }

    public void testHoldsAreDroppedWhenTheBackupEnds() {
        final SnapshotBacklogHolds holds = new SnapshotBacklogHolds();
        final ClusterState running = state(snapshot("snap-1", Map.of(SHARD, active(FIRST), OTHER_SHARD, active(FIRST))));
        holds.onClusterState(running, false, (projectRepo, shardId) -> new ShardFigure(Status.MEASURED, FIRST, 100));
        assertThat(holds.size(), equalTo(2));

        holds.onClusterState(state(snapshot("snap-1", Map.of(SHARD, succeeded(SECOND), OTHER_SHARD, active(FIRST)))), false, (p, s) -> {
            throw new AssertionError("no new hold");
        });
        assertThat(holds.size(), equalTo(1));

        holds.onClusterState(state(), false, (p, s) -> { throw new AssertionError("no new hold"); });
        assertThat(holds.size(), equalTo(0));
    }

    private ShardFigure figure() {
        return service.getFigures(applied).get(REPOSITORY).get(SHARD);
    }

    private void apply(ClusterState state) {
        final ClusterState previous = applied;
        applied = ClusterState.builder(state).version(previous.version() + 1).build();
        service.clusterChanged(new ClusterChangedEvent("test", applied, previous));
    }

    // The node reports both primaries, measured against the given generation: the first with the given bytes, the second with 200.
    private void report(ShardGeneration generation, long bytes) {
        assertTrue(
            service.update(
                new PublishSnapshotBacklogRequest(
                    TEST_REQUEST_TIMEOUT,
                    NODE.getId(),
                    NODE.getEphemeralId(),
                    seqNo.incrementAndGet(),
                    Map.of(
                        REPOSITORY,
                        Map.of(
                            SHARD,
                            new ShardBacklog(primary.allocationId().getId(), generation, bytes),
                            OTHER_SHARD,
                            new ShardBacklog(otherPrimary.allocationId().getId(), generation, 200)
                        )
                    )
                )
            )
        );
    }

    private static ShardSnapshotStatus active(ShardGeneration startGeneration) {
        return new ShardSnapshotStatus(NODE.getId(), startGeneration);
    }

    private static ShardSnapshotStatus queued() {
        return ShardSnapshotStatus.UNASSIGNED_QUEUED;
    }

    private static ShardSnapshotStatus succeeded(ShardGeneration newGeneration) {
        return ShardSnapshotStatus.success(NODE.getId(), new ShardSnapshotResult(newGeneration, ByteSizeValue.ZERO, 1));
    }

    private static ShardSnapshotStatus failed(ShardGeneration startGeneration) {
        return new ShardSnapshotStatus(NODE.getId(), SnapshotsInProgress.ShardState.FAILED, startGeneration, "failed");
    }

    private static SnapshotsInProgress.Entry snapshot(String name, Map<ShardId, ShardSnapshotStatus> shards) {
        return SnapshotsInProgress.Entry.snapshot(
            new Snapshot(ProjectId.DEFAULT, "repo", new SnapshotId(name, name + "-uuid")),
            false,
            false,
            SnapshotsInProgress.State.STARTED,
            Map.of(INDEX.getName(), new IndexId(INDEX.getName(), "index-id")),
            List.of(),
            List.of(),
            0,
            1,
            shards,
            null,
            Map.of(),
            IndexVersion.current()
        );
    }

    /**
     * A cluster state in which this node is the elected master, the index node holds both primaries, an SLM policy names the
     * repository, and the given snapshots are running.
     */
    private ClusterState state(SnapshotsInProgress.Entry... running) {
        SnapshotsInProgress snapshots = SnapshotsInProgress.EMPTY;
        for (SnapshotsInProgress.Entry entry : running) {
            snapshots = snapshots.withAddedEntry(entry);
        }
        final SnapshotLifecyclePolicyMetadata policy = SnapshotLifecyclePolicyMetadata.builder()
            .setPolicy(new SnapshotLifecyclePolicy("policy", "snap", "0 0/30 * * * ?", "repo", null, null))
            .setVersion(1)
            .setModifiedDate(0)
            .build();
        return ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(MASTER).add(NODE).localNodeId(MASTER.getId()).masterNodeId(MASTER.getId()))
            .metadata(
                Metadata.builder()
                    .put(
                        ProjectMetadata.builder(ProjectId.DEFAULT)
                            .putCustom(
                                RepositoriesMetadata.TYPE,
                                new RepositoriesMetadata(List.of(new RepositoryMetadata("repo", "repo-uuid", "fs", Settings.EMPTY)))
                            )
                            .putCustom(
                                SnapshotLifecycleMetadata.TYPE,
                                new SnapshotLifecycleMetadata(Map.of("policy", policy), OperationMode.RUNNING, null)
                            )
                            .put(
                                IndexMetadata.builder(INDEX.getName())
                                    .settings(indexSettings(IndexVersion.current(), INDEX.getUUID(), 2, 0))
                                    .build(),
                                false
                            )
                    )
            )
            .routingTable(RoutingTable.builder().add(IndexRoutingTable.builder(INDEX).addShard(primary).addShard(otherPrimary)))
            .putCustom(SnapshotsInProgress.TYPE, snapshots)
            .build();
    }
}
