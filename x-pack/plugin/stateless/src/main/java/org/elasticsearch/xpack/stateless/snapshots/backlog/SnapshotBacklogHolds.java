/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots.backlog;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress.ShardSnapshotStatus;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.ProjectRepo;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.xpack.stateless.snapshots.backlog.SnapshotBacklogMetricsService.ShardFigure;
import org.elasticsearch.xpack.stateless.snapshots.backlog.SnapshotBacklogMetricsService.Status;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;

/**
 * The snapshot backlog figures the master holds while backups run, used by {@link SnapshotBacklogMetricsService}.
 * <p>
 * A backup takes the data a shard had when the shard's snapshot started. Data written after that is uploaded by the next backup.
 * So while a shard's snapshot runs, its figure must stay what it was at the start; otherwise new writes would raise the need in the
 * middle of a backup. For each repository and primary shard, using the running snapshots in the cluster state:
 * <ul>
 *     <li><b>Active</b> in a running snapshot (started, not queued, not finished; {@link ShardSnapshotStatus#isActive()}): the figure
 *     held for that snapshot. A repository runs at most one snapshot of a shard at a time; later snapshots queue the shard.</li>
 *     <li><b>Finished</b> in every running snapshot that includes it: 0 ({@link Status#BACKED_UP}) until those snapshots end. A failed
 *     shard counts as finished: the next backup uploads its data.</li>
 *     <li>Otherwise, including a shard that is only queued: no figure from here, so the caller uses the live figure. A queued shard
 *     has no start generation yet, so there is nothing to hold it against.</li>
 * </ul>
 * Which figure is held, for a shard that becomes active in a snapshot:
 * <ul>
 *     <li>The live figure, if it was measured against the generation of the shard's backup metadata that the snapshot started from
 *     ({@link ShardSnapshotStatus#generation()}). In a single backup this is the last report from before the start.</li>
 *     <li>Otherwise none yet ({@link Status#HOLD_PENDING}: 0, minimum), and the first live figure measured against that generation is
 *     held when it arrives. This happens when snapshots overlap: a queued shard starts in the same cluster state update that finishes
 *     it in the earlier snapshot, from the generation that update wrote, so no figure for that generation exists yet.</li>
 *     <li>A figure measured against any other generation is never held.</li>
 *     <li>Once held, the figure doesn't change until the shard finishes or the snapshot ends: not on new writes, not when the shard
 *     relocates (the upload may carry on from the old node), not when a node leaves (the upload starts again on another node).</li>
 * </ul>
 * A shard that is already active when the master starts holding figures (after a master failover, or when the feature is turned on)
 * gets no held figure ({@link Status#HOLD_LOST}: 0, minimum) until it finishes. The master can't know what its figure was at the start,
 * and a figure measured now can include data written since.
 * <p>
 * Why the order of updates is safe: a hold is added by the cluster state listener, and filled either there or when a report arrives.
 * Each side first stores what it has (the pending hold, or the report) and then looks for the other, and filling is a compare-and-set
 * from pending to held. So a report and the shard becoming active can arrive in either order without the hold being missed or filled
 * twice.
 */
final class SnapshotBacklogHolds {

    private static final ShardFigure PENDING = ShardFigure.without(Status.HOLD_PENDING);
    private static final ShardFigure LOST = ShardFigure.without(Status.HOLD_LOST);
    private static final ShardFigure BACKED_UP = ShardFigure.without(Status.BACKED_UP);

    // One shard in one running snapshot.
    private record Key(Snapshot snapshot, ShardId shardId) {
        ProjectRepo projectRepo() {
            return new ProjectRepo(snapshot.getProjectId(), snapshot.getRepository());
        }
    }

    /**
     * What is held for one shard in one snapshot.
     *
     * @param startGeneration the generation of the shard's backup metadata the snapshot started from
     * @param figure          {@link Status#HELD}, {@link Status#HOLD_PENDING} or {@link Status#HOLD_LOST}
     */
    private record Hold(ShardGeneration startGeneration, ShardFigure figure) {}

    private final Map<Key, Hold> holds = new ConcurrentHashMap<>();

    /**
     * Follows the running snapshots: forgets the holds of shards that are no longer active, and adds one for each shard that has just
    * become active. Call it with every cluster state whose running snapshots changed, in order. It follows the snapshots of every
     * repository, not only those SLM policies name, so a policy added during a snapshot needs nothing special.
     *
     * @param firstState   whether this is the first state since the master started holding figures; shards already active in it get
     *                     {@link Status#HOLD_LOST}
     * @param liveFigures  the live figure of a shard in a repository, as {@link SnapshotBacklogMetricsService} sees it at that moment
     */
    void onClusterState(ClusterState state, boolean firstState, BiFunction<ProjectRepo, ShardId, ShardFigure> liveFigures) {
        final Map<Key, ShardGeneration> active = new HashMap<>();
        SnapshotsInProgress.get(state).asStream().filter(entry -> entry.isClone() == false).forEach(entry -> {
            for (Map.Entry<ShardId, ShardSnapshotStatus> shardAndStatus : entry.shards().entrySet()) {
                if (shardAndStatus.getValue().isActive()) {
                    active.put(new Key(entry.snapshot(), shardAndStatus.getKey()), shardAndStatus.getValue().generation());
                }
            }
        });
        holds.keySet().retainAll(active.keySet());
        for (Map.Entry<Key, ShardGeneration> keyAndGeneration : active.entrySet()) {
            final Key key = keyAndGeneration.getKey();
            final Hold added = new Hold(keyAndGeneration.getValue(), firstState ? LOST : PENDING);
            // the hold is stored before looking for a figure, see the class Javadoc
            if (holds.putIfAbsent(key, added) == null && firstState == false) {
                tryFill(key, added, liveFigures);
            }
        }
    }

    /**
     * Fills every pending hold whose shard now has a live figure measured against the snapshot's start generation. Call it after a
     * report is stored. Pending holds only exist while snapshots overlap, so there are few.
     */
    void onReport(BiFunction<ProjectRepo, ShardId, ShardFigure> liveFigures) {
        for (Map.Entry<Key, Hold> keyAndHold : holds.entrySet()) {
            if (keyAndHold.getValue().figure().status() == Status.HOLD_PENDING) {
                tryFill(keyAndHold.getKey(), keyAndHold.getValue(), liveFigures);
            }
        }
    }

    private void tryFill(Key key, Hold pending, BiFunction<ProjectRepo, ShardId, ShardFigure> liveFigures) {
        final ShardFigure live = liveFigures.apply(key.projectRepo(), key.shardId());
        if (live.status() == Status.MEASURED && live.generation().equals(pending.startGeneration())) {
            holds.replace(key, pending, new Hold(pending.startGeneration(), new ShardFigure(Status.HELD, live.generation(), live.bytes())));
        }
    }

    /**
     * The figures that running snapshots decide, for the given cluster state and repository: one for each shard that is active in a
     * snapshot, or finished in every snapshot that includes it. A shard left out uses its live figure.
     * <p>
     * Whether a shard is active comes only from the given state. An active shard without a hold yet counts as
     * {@link Status#HOLD_PENDING}: the given state can be newer than the last one {@link #onClusterState} saw, since the cluster
     * service makes a new state visible before it calls the listeners.
     */
    Map<ShardId, ShardFigure> getFigures(ClusterState state, ProjectRepo projectRepo) {
        final Map<ShardId, ShardFigure> figures = new HashMap<>();
        final Set<ShardId> queued = new HashSet<>();
        for (SnapshotsInProgress.Entry entry : SnapshotsInProgress.get(state).forRepo(projectRepo)) {
            if (entry.isClone()) {
                continue;
            }
            for (Map.Entry<ShardId, ShardSnapshotStatus> shardAndStatus : entry.shards().entrySet()) {
                final ShardId shardId = shardAndStatus.getKey();
                final ShardSnapshotStatus status = shardAndStatus.getValue();
                if (status.isActive()) {
                    final Hold hold = holds.get(new Key(entry.snapshot(), shardId));
                    figures.put(shardId, hold == null ? PENDING : hold.figure());
                } else if (status.state() == SnapshotsInProgress.ShardState.QUEUED) {
                    queued.add(shardId);
                } else {
                    // finished: only counts as backed up if no other snapshot has the shard active or queued
                    figures.putIfAbsent(shardId, BACKED_UP);
                }
            }
        }
        for (ShardId shardId : queued) {
            figures.remove(shardId, BACKED_UP);
        }
        return figures;
    }

    void clear() {
        holds.clear();
    }

    // visible for testing: how many holds are kept
    int size() {
        return holds.size();
    }
}
