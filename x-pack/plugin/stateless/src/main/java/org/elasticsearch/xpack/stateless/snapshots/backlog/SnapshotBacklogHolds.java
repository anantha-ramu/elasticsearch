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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.ProjectRepo;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.repositories.ShardGenerations;
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
 * Why: a backup uploads the data a shard had when the shard's snapshot started. Data written after that start is uploaded by the
 * next backup. So while a shard's snapshot runs, the shard's figure must stay what it was at the start. Otherwise new writes would
 * raise the need in the middle of a backup.
 * <p>
 * What it keeps: one hold for each shard that is active in a running snapshot, keyed by the snapshot and the shard. A hold records
 * the generation of the shard's backup metadata that the snapshot started from (the start generation), and the held figure, or that
 * there is none yet. Holds are dropped when the shard stops being active, and all are dropped when this node stops holding figures.
 * <p>
 * How each shard of a repository is counted, from the running snapshots in the cluster state:
 * <ul>
 *     <li><b>Active</b> in a running snapshot (started, not queued, not finished; {@link ShardSnapshotStatus#isActive()}): the figure
 *     held for that snapshot. A repository runs at most one snapshot of a shard at a time; later snapshots queue the shard.</li>
 *     <li><b>Finished</b> in every running snapshot that includes the shard: 0 ({@link Status#BACKED_UP}) until those snapshots end.
 *     A failed shard counts as finished, because the next backup uploads its data.</li>
 *     <li>Otherwise, including a shard that is only queued: no figure from the holds, so the caller uses the live figure. A queued
 *     shard has no start generation yet, so no figure can be matched to the start of its snapshot.</li>
 * </ul>
 * Which figure is held, once a shard becomes active in a snapshot:
 * <ul>
 *     <li>The live figure, if the node measured that figure against the start generation ({@link ShardSnapshotStatus#generation()}).
 *     When one snapshot runs at a time, that figure comes from the last report before the snapshot started.</li>
 *     <li>Otherwise none yet ({@link Status#HOLD_PENDING}: 0, minimum). The first live figure measured against the start generation is
 *     held when a report brings one. Waiting happens when two snapshots of the repository overlap. The cluster state update that
 *     finishes the shard in the earlier snapshot also starts the shard in the later snapshot, from the generation the earlier
 *     snapshot just wrote. No report measured against that generation exists yet.</li>
 *     <li>A figure measured against any other generation is never held.</li>
 *     <li>The first snapshot of a new index starts from {@link ShardGenerations#NEW_SHARD_GEN}, and so does the node's figure for a
 *     shard the repository doesn't know. A shard the repository doesn't know in an index it does know, such as a new shard after a
 *     split, starts from no generation ({@code null}): the snapshot then starts from nothing, as with {@code NEW_SHARD_GEN}, so a
 *     figure measured against {@code NEW_SHARD_GEN} is held for it.</li>
 *     <li>Once held, the figure doesn't change until the shard finishes or the snapshot ends: not on new writes, not when the shard
 *     relocates (the upload may carry on from the old node), not when a node leaves (the upload starts again on another node).</li>
 *     <li>A shard that is already active when the master starts holding figures (after a master failover, or when the feature is
 *     turned on) gets no figure ({@link Status#HOLD_LOST}: 0, minimum) until the shard finishes. The master can't know the shard's
 *     figure from when the snapshot started, and a figure measured later can include data written during the snapshot.</li>
 * </ul>
 * Why concurrent updates are safe: the cluster state listener adds a hold and may fill it, and a report arriving on another thread
 * may fill it later. The listener stores the pending hold before it looks up the live figure, and the report handler stores the
 * report before it looks for pending holds. Filling is a compare-and-set from the pending hold to the held figure. So a report and
 * the shard becoming active can arrive in either order, and a hold is never missed, filled twice, or brought back after being dropped.
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
     * @param startGeneration the generation of the shard's backup metadata the snapshot started from, or {@code null} for none
     * @param figure          {@link Status#HELD}, {@link Status#HOLD_PENDING} or {@link Status#HOLD_LOST}
     */
    private record Hold(@Nullable ShardGeneration startGeneration, ShardFigure figure) {

        // The generation a figure must have been measured against to be held: a snapshot with no start generation starts from nothing.
        ShardGeneration matchingGeneration() {
            return startGeneration == null ? ShardGenerations.NEW_SHARD_GEN : startGeneration;
        }
    }

    private final Map<Key, Hold> holds = new ConcurrentHashMap<>();

    /**
     * Updates the holds to match the running snapshots in the given cluster state: drops the holds of shards that are no longer
     * active, and adds a hold for each shard that has just become active. Call {@code onClusterState} with every cluster state whose
     * running snapshots changed, in order. Every repository's snapshots are followed, not only those of repositories SLM policies
     * name, so a policy added during a snapshot needs no special handling.
     *
     * @param firstState   whether the given state is the first since the master started holding figures; shards already active in
     *                     that state get {@link Status#HOLD_LOST}
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
     * Fills the pending holds of the shards in a node's report that now have a live figure measured against their snapshot's start
     * generation. Call {@code onReport} after storing the report. The work is bounded by the report's size and the number of running
     * snapshots, not by the number of pending holds: holds of a repository no node reports stay pending for a whole snapshot.
     *
     * @param state    the latest applied cluster state, read after the report was stored
     * @param reported the shards in the report, per repository
     */
    void onReport(
        ClusterState state,
        Map<ProjectRepo, ? extends Map<ShardId, ?>> reported,
        BiFunction<ProjectRepo, ShardId, ShardFigure> liveFigures
    ) {
        for (Map.Entry<ProjectRepo, ? extends Map<ShardId, ?>> repositoryAndShards : reported.entrySet()) {
            for (SnapshotsInProgress.Entry entry : SnapshotsInProgress.get(state).forRepo(repositoryAndShards.getKey())) {
                if (entry.isClone()) {
                    continue;
                }
                for (ShardId shardId : repositoryAndShards.getValue().keySet()) {
                    final Key key = new Key(entry.snapshot(), shardId);
                    final Hold hold = holds.get(key);
                    if (hold != null && hold.figure().status() == Status.HOLD_PENDING) {
                        tryFill(key, hold, liveFigures);
                    }
                }
            }
        }
    }

    private void tryFill(Key key, Hold pending, BiFunction<ProjectRepo, ShardId, ShardFigure> liveFigures) {
        final ShardFigure live = liveFigures.apply(key.projectRepo(), key.shardId());
        if (live.status() == Status.MEASURED && live.generation().equals(pending.matchingGeneration())) {
            holds.replace(key, pending, new Hold(pending.startGeneration(), new ShardFigure(Status.HELD, live.generation(), live.bytes())));
        }
    }

    /**
     * The figures that running snapshots decide, for the given cluster state and repository: one for each shard that is active in a
     * snapshot, or finished in every snapshot that includes it. A shard left out uses its live figure.
     * <p>
     * Whether a shard is active comes only from the given state. An active shard without a hold yet counts as
     * {@link Status#HOLD_PENDING}: the given cluster state can be newer than the last cluster state passed to
     * {@link #onClusterState}, because the cluster service makes a new state visible before it calls the listeners.
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
