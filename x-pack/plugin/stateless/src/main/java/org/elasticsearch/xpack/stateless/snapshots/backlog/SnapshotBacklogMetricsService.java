/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots.backlog;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.repositories.ProjectRepo;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.xpack.stateless.MetricQuality;
import org.elasticsearch.xpack.stateless.snapshots.backlog.PublishSnapshotBacklogRequest.ShardBacklog;

import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * Keeps, on the elected master, the latest snapshot backlog report of each index node ({@link SnapshotBacklogTracker},
 * {@link SnapshotBacklogPublisher}), and gives a figure for every primary shard and every repository a snapshot lifecycle (SLM) policy
 * names. The service also publishes the totals as master metrics.
 * <p>
 * What it keeps: one report per node, each one the node's whole latest result, replaced as a whole by the node's next report. Nothing
 * per shard is kept in step with the routing table. Instead, when the figures are read ({@link #getFigures}), each primary in the
 * routing table is looked up in the report of the node that holds it. The figure is used only if it is for the same copy of the shard,
 * that is, the same allocation id. So a figure from before a relocation, a restart or any other reassignment is never used, even if
 * the shard has since come back to the same node.
 * <p>
 * Each primary gets one {@link Status}, checked in this order, the first that applies. First, while a snapshot of the repository runs
 * ({@link SnapshotBacklogHolds} explains why and how):
 * <ul>
 *     <li>{@link Status#HELD}: the shard's snapshot is running, and the figure from when the snapshot started is held;</li>
 *     <li>{@link Status#HOLD_PENDING}: the shard's snapshot is running, and no figure from when the snapshot started is known yet;</li>
 *     <li>{@link Status#HOLD_LOST}: the shard's snapshot was already running when this node started holding figures;</li>
 *     <li>{@link Status#BACKED_UP}: the shard has finished in every running snapshot that includes the shard, so the shard counts
 *     0.</li>
 * </ul>
 * Otherwise, the live figure, from the reports:
 * <ul>
 *     <li>{@link Status#UNASSIGNED}: the primary is not on any node;</li>
 *     <li>{@link Status#NOT_REPORTED}: its node has sent no report that is still kept;</li>
 *     <li>{@link Status#STALE}: its node's latest report is older than {@link #STALE_AFTER}, so the node has stopped reporting;</li>
 *     <li>{@link Status#NOT_REPORTED}: the report has no entry for this copy of the shard, for example because the shard arrived on
 *     the node after the node's last evaluation;</li>
 *     <li>{@link Status#NOT_MEASURED}: the node reported the shard, but couldn't measure the backlog yet, for example while the node
 *     loads the shard's backup metadata;</li>
 *     <li>{@link Status#MEASURED}: the node measured the shard. Only a measured figure is exact. The figure keeps the generation of
 *     the shard's backup metadata that the node measured against.</li>
 * </ul>
 * Only {@link Status#MEASURED} and {@link Status#HELD} count bytes. {@link Status#BACKED_UP} counts 0 and is exact. Every other status
 * counts 0 bytes and is {@link MetricQuality#MINIMUM}: 0 is the one value that is always low enough, and "minimum" tells the
 * autoscaler not to scale down on it. A figure kept from before a relocation, or from a node that stopped reporting, could count too
 * much, since a snapshot may have finished since.
 * <p>
 * When the store is safe to use: the store holds nothing unless this node is the elected master, the cluster state is recovered (so
 * the routing table and the nodes are known), and {@link SnapshotBacklogTracker#ENABLED_SETTING} is on. The store forgets everything
 * when any of these conditions stops being true, and forgets a node's report when the node leaves the cluster. A report that arrives
 * while a condition changes is checked again after it is stored, so the report never stays behind (see {@link #update}). A new master
 * starts empty, so every shard is {@link Status#NOT_REPORTED} until the nodes' next reports, at most one evaluation interval later.
 * <p>
 * The store does not apply the SLM policies' schedules, results or index lists. The caller applies those rules, which is why
 * {@link #getFigures} gives a figure for every primary of the repository's project.
 */
public class SnapshotBacklogMetricsService implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(SnapshotBacklogMetricsService.class);

    /**
     * How long a node's report counts after the master received it: three evaluations, so two lost reports in a row are tolerated.
     */
    static final TimeValue STALE_AFTER = TimeValue.timeValueSeconds(3 * SnapshotBacklogTracker.EVALUATION_INTERVAL.seconds());

    static final String SIZE_METRIC = "es.stateless.snapshot_backlog.master.size";
    static final String SHARDS_METRIC = "es.stateless.snapshot_backlog.master.shards.current";

    /**
     * Where the figure of one primary shard in one repository comes from. See the class Javadoc for when each one applies.
     */
    public enum Status {
        MEASURED,
        NOT_MEASURED,
        NOT_REPORTED,
        STALE,
        UNASSIGNED,
        HELD,
        HOLD_PENDING,
        HOLD_LOST,
        BACKED_UP;

        public MetricQuality quality() {
            return switch (this) {
                case MEASURED, HELD, BACKED_UP -> MetricQuality.EXACT;
                case NOT_MEASURED, NOT_REPORTED, STALE, UNASSIGNED, HOLD_PENDING, HOLD_LOST -> MetricQuality.MINIMUM;
            };
        }

        boolean hasFigure() {
            return this == MEASURED || this == HELD;
        }

        String label() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    /**
     * The figure of one primary shard in one repository.
     *
     * @param generation the generation of the shard's backup metadata the figure was measured against; {@code null} unless measured
     *                   or held
     * @param bytes      the bytes the next snapshot of the shard would upload; 0 unless measured or held
     */
    public record ShardFigure(Status status, @Nullable ShardGeneration generation, long bytes) {

        private static final Map<Status, ShardFigure> WITHOUT_FIGURE = Map.of(
            Status.NOT_MEASURED,
            new ShardFigure(Status.NOT_MEASURED, null, 0),
            Status.NOT_REPORTED,
            new ShardFigure(Status.NOT_REPORTED, null, 0),
            Status.STALE,
            new ShardFigure(Status.STALE, null, 0),
            Status.UNASSIGNED,
            new ShardFigure(Status.UNASSIGNED, null, 0),
            Status.HOLD_PENDING,
            new ShardFigure(Status.HOLD_PENDING, null, 0),
            Status.HOLD_LOST,
            new ShardFigure(Status.HOLD_LOST, null, 0),
            Status.BACKED_UP,
            new ShardFigure(Status.BACKED_UP, null, 0)
        );

        public ShardFigure {
            assert status.hasFigure() == (generation != null) : status + " with generation " + generation;
            assert status.hasFigure() || bytes == 0 : status + " with " + bytes + " bytes";
        }

        static ShardFigure without(Status status) {
            return WITHOUT_FIGURE.get(status);
        }

        public MetricQuality quality() {
            return status.quality();
        }
    }

    // One node's latest report, as the master received it.
    private record NodeReport(String ephemeralId, long seqNo, long receivedNanos, Map<ProjectRepo, Map<ShardId, ShardBacklog>> shards) {}

    private final Supplier<ClusterState> clusterState;
    private final LongSupplier relativeTimeNanos;
    private final Map<String, NodeReport> reports = new ConcurrentHashMap<>();
    private final SnapshotBacklogHolds holds = new SnapshotBacklogHolds();
    private volatile boolean enabled;
    // Whether the holds have followed the running snapshots since an earlier cluster state. False until the first cluster state after
    // this node becomes master or the setting is turned on; shards already active in that first state can't be held. Changed only on
    // the cluster applier thread, which also applies settings (ClusterApplierService applies the settings before calling the
    // listeners), so setEnabled and clusterChanged never run at the same time.
    private volatile boolean holding;

    @SuppressWarnings("this-escape")
    public SnapshotBacklogMetricsService(ClusterService clusterService, LongSupplier relativeTimeNanos, MeterRegistry meterRegistry) {
        this(clusterService::state, relativeTimeNanos, meterRegistry);
        clusterService.getClusterSettings().initializeAndWatch(SnapshotBacklogTracker.ENABLED_SETTING, this::setEnabled);
        clusterService.addListener(this);
    }

    /**
     * @param clusterState the latest applied cluster state, the one {@link ClusterService#state()} gives
     */
    @SuppressWarnings("this-escape")
    SnapshotBacklogMetricsService(Supplier<ClusterState> clusterState, LongSupplier relativeTimeNanos, MeterRegistry meterRegistry) {
        this.clusterState = clusterState;
        this.relativeTimeNanos = relativeTimeNanos;
        registerMetrics(meterRegistry);
    }

    void setEnabled(boolean enabled) {
        // set the flag first, so a report stored after the clear sees it when it checks again (see update)
        this.enabled = enabled;
        holding = false;
        if (enabled == false) {
            reports.clear();
            holds.clear();
        }
    }

    /**
     * Keeps a node's report, unless the report is older than the kept report, or comes from a node that isn't in the cluster (or from an
     * earlier run of that node), or this node isn't keeping reports.
     * <p>
     * After storing the report, {@code update} checks the latest cluster state and setting again, and drops the report if either
     * changed. The cluster state listener and the setting watcher forget reports only after the new state or setting is in place. So
     * either the second check sees the change, or the listener or watcher runs after the report was stored and forgets the report then.
     * A report that arrives as a node leaves, as this node stops being master, or as the setting is turned off never stays behind.
     *
     * @return whether the report was kept
     */
    boolean update(PublishSnapshotBacklogRequest request) {
        if (isAccepted(request) == false) {
            logger.debug("ignoring snapshot backlog report {}, the node isn't in the cluster or this node isn't keeping reports", request);
            return false;
        }
        final NodeReport report = new NodeReport(
            request.nodeEphemeralId(),
            request.seqNo(),
            relativeTimeNanos.getAsLong(),
            request.shards()
        );
        final NodeReport kept = reports.compute(
            request.nodeId(),
            (nodeId, previous) -> previous == null
                || previous.ephemeralId().equals(report.ephemeralId()) == false
                || previous.seqNo() < report.seqNo() ? report : previous
        );
        if (kept != report) {
            logger.debug("ignoring snapshot backlog report {}, a newer one from the node is kept", request);
            return false;
        }
        if (isAccepted(request) == false) {
            reports.remove(request.nodeId(), report);
            logger.debug("dropping snapshot backlog report {}, the node left or this node stopped keeping reports", request);
            return false;
        }
        // the report is stored before looking for pending holds, see SnapshotBacklogHolds
        final ClusterState state = clusterState.get();
        final long now = relativeTimeNanos.getAsLong();
        holds.onReport(state, request.shards(), (projectRepo, shardId) -> getLiveFigure(state, projectRepo, shardId, now));
        return true;
    }

    private boolean isAccepted(PublishSnapshotBacklogRequest request) {
        final ClusterState state = clusterState.get();
        if (isKeepingReports(state) == false) {
            return false;
        }
        final DiscoveryNode node = state.nodes().get(request.nodeId());
        return node != null && node.getEphemeralId().equals(request.nodeEphemeralId());
    }

    private boolean isKeepingReports(ClusterState state) {
        return enabled
            && state.nodes().isLocalNodeElectedMaster()
            && state.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK) == false;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.localNodeMaster() == false || event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            reports.clear();
            holds.clear();
            holding = false;
            return;
        }
        if (event.nodesChanged()) {
            // a node that left, or restarted, has nothing to report for its new run yet
            reports.entrySet().removeIf(nodeAndReport -> {
                final DiscoveryNode node = event.state().nodes().get(nodeAndReport.getKey());
                return node == null || node.getEphemeralId().equals(nodeAndReport.getValue().ephemeralId()) == false;
            });
        }
        if (enabled
            && (holding == false
                || event.previousState().custom(SnapshotsInProgress.TYPE) != event.state().custom(SnapshotsInProgress.TYPE))) {
            final long now = relativeTimeNanos.getAsLong();
            holds.onClusterState(
                event.state(),
                holding == false,
                (projectRepo, shardId) -> getLiveFigure(event.state(), projectRepo, shardId, now)
            );
            holding = true;
        }
    }

    /**
     * The figures for the given cluster state: for each repository an SLM policy names, each primary shard of the repository's project.
     * Shards of indices without a routing table, such as closed ones, are left out. Empty unless this node keeps reports.
     */
    public Map<ProjectRepo, Map<ShardId, ShardFigure>> getFigures(ClusterState state) {
        if (isKeepingReports(state) == false) {
            return Map.of();
        }
        final long now = relativeTimeNanos.getAsLong();
        final Map<ProjectRepo, Map<ShardId, ShardFigure>> figures = new HashMap<>();
        for (ProjectRepo projectRepo : SnapshotBacklogTracker.trackedRepositories(state)) {
            final RoutingTable routingTable = state.globalRoutingTable().routingTables().get(projectRepo.projectId());
            if (routingTable == null) {
                continue;
            }
            // shards active or finished in a running snapshot get their figure from the holds instead of the live reports
            final Map<ShardId, ShardFigure> backups = holds.getFigures(state, projectRepo);
            final Map<ShardId, ShardFigure> repositoryFigures = new HashMap<>();
            for (IndexRoutingTable indexRoutingTable : routingTable) {
                for (int shard = 0; shard < indexRoutingTable.size(); shard++) {
                    final ShardRouting primary = indexRoutingTable.shard(shard).primaryShard();
                    final ShardFigure backup = backups.get(primary.shardId());
                    repositoryFigures.put(primary.shardId(), backup != null ? backup : getFigure(projectRepo, primary, now));
                }
            }
            figures.put(projectRepo, Map.copyOf(repositoryFigures));
        }
        return Map.copyOf(figures);
    }

    // The live figure of a shard by its id, for the holds.
    private ShardFigure getLiveFigure(ClusterState state, ProjectRepo projectRepo, ShardId shardId, long now) {
        final RoutingTable routingTable = state.globalRoutingTable().routingTables().get(projectRepo.projectId());
        final IndexRoutingTable indexRoutingTable = routingTable == null ? null : routingTable.index(shardId.getIndex());
        if (indexRoutingTable == null || shardId.id() >= indexRoutingTable.size()) {
            return ShardFigure.without(Status.NOT_REPORTED);
        }
        return getFigure(projectRepo, indexRoutingTable.shard(shardId.id()).primaryShard(), now);
    }

    // The live figure of a primary: what its node last reported for it.
    private ShardFigure getFigure(ProjectRepo projectRepo, ShardRouting primary, long now) {
        if (primary.unassigned()) {
            return ShardFigure.without(Status.UNASSIGNED);
        }
        final NodeReport report = reports.get(primary.currentNodeId());
        if (report == null) {
            return ShardFigure.without(Status.NOT_REPORTED);
        }
        if (now - report.receivedNanos() > STALE_AFTER.nanos()) {
            return ShardFigure.without(Status.STALE);
        }
        final ShardBacklog backlog = report.shards().getOrDefault(projectRepo, Map.of()).get(primary.shardId());
        if (backlog == null || backlog.allocationId().equals(primary.allocationId().getId()) == false) {
            return ShardFigure.without(Status.NOT_REPORTED);
        }
        if (backlog.isMeasured() == false) {
            return ShardFigure.without(Status.NOT_MEASURED);
        }
        return new ShardFigure(Status.MEASURED, backlog.generation(), backlog.bytes());
    }

    // visible for testing: the ids of the nodes whose report is kept
    Set<String> getReportingNodes() {
        return Set.copyOf(reports.keySet());
    }

    /**
     * The quality of a total over the given figures: exact only if every figure is exact.
     */
    public static MetricQuality quality(Collection<ShardFigure> figures) {
        for (ShardFigure figure : figures) {
            if (figure.quality() == MetricQuality.MINIMUM) {
                return MetricQuality.MINIMUM;
            }
        }
        return MetricQuality.EXACT;
    }

    private void registerMetrics(MeterRegistry meterRegistry) {
        meterRegistry.registerLongAsyncGauge(
            SIZE_METRIC,
            "Bytes the next snapshot of every measured primary would upload, as the master sees it",
            "bytes",
            measurement -> {
                for (Map.Entry<ProjectRepo, Map<ShardId, ShardFigure>> repositoryAndFigures : getFigures(clusterState.get()).entrySet()) {
                    long bytes = 0;
                    for (ShardFigure figure : repositoryAndFigures.getValue().values()) {
                        bytes += figure.bytes();
                    }
                    measurement.record(bytes, Map.of("repo_name", repositoryAndFigures.getKey().name()));
                }
            }
        );
        meterRegistry.registerLongAsyncGauge(
            SHARDS_METRIC,
            "Primary shards per status of their snapshot backlog figure, as the master sees it",
            "shards",
            measurement -> {
                for (Map.Entry<ProjectRepo, Map<ShardId, ShardFigure>> repositoryAndFigures : getFigures(clusterState.get()).entrySet()) {
                    final Map<Status, Long> byStatus = new EnumMap<>(Status.class);
                    for (ShardFigure figure : repositoryAndFigures.getValue().values()) {
                        byStatus.merge(figure.status(), 1L, Long::sum);
                    }
                    // every status is recorded, 0 included, so the share of time with shards in each status can be computed
                    for (Status status : Status.values()) {
                        measurement.record(
                            byStatus.getOrDefault(status, 0L),
                            Map.of("repo_name", repositoryAndFigures.getKey().name(), "status", status.label())
                        );
                    }
                }
            }
        );
    }
}
