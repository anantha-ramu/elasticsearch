/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots.backlog;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.repositories.ProjectRepo;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transports;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleMetadata;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicyMetadata;
import org.elasticsearch.xpack.stateless.commits.HollowShardsService;
import org.elasticsearch.xpack.stateless.snapshots.backlog.ShardBaselineCache.LatestCommits;
import org.elasticsearch.xpack.stateless.snapshots.backlog.ShardBaselineCache.Measured;
import org.elasticsearch.xpack.stateless.snapshots.backlog.ShardBaselineCache.NotMeasured;
import org.elasticsearch.xpack.stateless.snapshots.backlog.ShardBaselineCache.Reason;
import org.elasticsearch.xpack.stateless.snapshots.backlog.ShardBaselineCache.Result;
import org.elasticsearch.xpack.stateless.snapshots.backlog.ShardBaselineCache.ShardCopy;

import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

/**
 * Measures the snapshot backlog on an index node: for each primary shard on the node and each repository a snapshot lifecycle
 * (SLM) policy of the shard's project names, the bytes the next snapshot of the shard would upload. The results are kept in memory
 * ({@link #getResults}) and published as node metrics.
 * <p>
 * Every {@link #EVALUATION_INTERVAL}, on the snapshot-meta pool, it:
 * <ol>
 *     <li>finds the repositories SLM policies name, per project, and the started primaries on this node;</li>
 *     <li>drops from the {@link ShardBaselineCache} the baselines of repositories and shard copies it no longer measures;</li>
 *     <li>asks the master where each shard's backup metadata is, if the answer it has is out of date ({@link NextShardGenerations});</li>
 *     <li>measures each shard against the generation its next snapshot will start from. A hollow shard counts 0 without reading
 *     anything: hollow shards have had no writes for a while, and their data was almost always backed up since.</li>
 * </ol>
 * A shard is "not measured", with a reason, until its generation is known and its baseline is loaded. Every measured figure carries
 * the generation it was measured against, so the master can tell which snapshot it belongs to.
 * <p>
 * The cluster state listener keeps {@link NextShardGenerations} up to date with every cluster state, in order, and asks the master
 * again as soon as a tracked repository changes or primaries arrive, so a new answer is usually there before the next evaluation.
 * <p>
 * It is off unless {@link #ENABLED_SETTING} is set. While off, it sends nothing, reads nothing, reports nothing and logs nothing.
 * Turning it off drops everything, including the cache.
 * <p>
 * The requests to the master run in the system context, so on a secured cluster they are sent as the internal system user, which may
 * run every {@code internal:} action.
 */
public final class SnapshotBacklogTracker implements ClusterStateListener, IndexEventListener {

    private static final Logger logger = LogManager.getLogger(SnapshotBacklogTracker.class);

    public static final Setting<Boolean> ENABLED_SETTING = Setting.boolSetting(
        "stateless.snapshot.backlog.enabled",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    static final TimeValue EVALUATION_INTERVAL = TimeValue.timeValueSeconds(30);

    static final String BACKLOG_METRIC = "es.stateless.snapshot_backlog.size";
    static final String MEASURED_SHARDS_METRIC = "es.stateless.snapshot_backlog.shards.measured.current";
    static final String NOT_MEASURED_SHARDS_METRIC = "es.stateless.snapshot_backlog.shards.not_measured.current";
    static final String BASELINES_METRIC = "es.stateless.snapshot_backlog.baselines.size";

    /**
     * A primary shard on this node.
     *
     * @param started whether the shard is started, so its commits can be taken
     * @param hollow  whether the shard is hollow ({@link HollowShardsService#isHollowShard})
     */
    record LocalPrimary(ShardCopy copy, BooleanSupplier started, BooleanSupplier hollow, LatestCommits commits) {
        ShardId shardId() {
            return copy.shardId();
        }
    }

    // What the tracker keeps while it is on. A new one is made each time it is turned on, so late work from an earlier one is ignored.
    private static final class Period {
        final ShardBaselineCache cache;
        // Set once, before the period is published.
        NextShardGenerations generations;
        // The latest cluster state the listener processed. Evaluations use it rather than the cluster service's, so that they never
        // see a state whose running snapshots the generations haven't taken in.
        @Nullable
        volatile ClusterState state;
        volatile Map<ProjectRepo, Map<ShardId, Result>> results = Map.of();
        @Nullable
        Scheduler.Cancellable task;

        Period(ShardBaselineCache cache) {
            this.cache = cache;
        }
    }

    private final ThreadPool threadPool;
    private final Executor executor;
    private final Supplier<List<LocalPrimary>> localPrimaries;
    private final NextShardGenerations.Sender sender;
    private final ShardBaselineCache.MetadataReader reader;
    private final long budgetBytes;

    @Nullable
    private volatile Period period;

    /**
     * @param localPrimaries the primary shards on this node, in any state
     */
    SnapshotBacklogTracker(
        ThreadPool threadPool,
        Supplier<List<LocalPrimary>> localPrimaries,
        NextShardGenerations.Sender sender,
        ShardBaselineCache.MetadataReader reader,
        long budgetBytes,
        MeterRegistry meterRegistry
    ) {
        this.threadPool = threadPool;
        this.executor = threadPool.executor(ThreadPool.Names.SNAPSHOT_META);
        this.localPrimaries = localPrimaries;
        this.sender = sender;
        this.reader = reader;
        this.budgetBytes = budgetBytes;
        registerMetrics(meterRegistry);
    }

    /**
     * Creates the tracker for an index node and hooks it up to the cluster: it follows the setting and the cluster state, and stops
     * when the node stops. It must also be registered as an index event listener, to forget shards as they close.
     */
    public static SnapshotBacklogTracker create(
        ClusterService clusterService,
        ThreadPool threadPool,
        Client client,
        IndicesService indicesService,
        RepositoriesService repositoriesService,
        HollowShardsService hollowShardsService,
        MeterRegistry meterRegistry
    ) {
        final SnapshotBacklogTracker tracker = new SnapshotBacklogTracker(
            threadPool,
            () -> localPrimaries(indicesService, hollowShardsService),
            systemContextSender(client),
            new BlobStoreMetadataReader(repositoriesService, meterRegistry),
            ShardBaselineCache.memoryBudget(JvmInfo.jvmInfo().getMem().getHeapMax().getBytes()),
            meterRegistry
        );
        clusterService.addLifecycleListener(new LifecycleListener() {
            @Override
            public void beforeStop() {
                tracker.setEnabled(false);
            }
        });
        clusterService.getClusterSettings().initializeAndWatch(ENABLED_SETTING, tracker::setEnabled);
        clusterService.addListener(tracker);
        return tracker;
    }

    /**
     * Sends the requests to the master in the system context: the action is internal, and serves the node, not any user's request.
     */
    static NextShardGenerations.Sender systemContextSender(Client client) {
        final ThreadContext threadContext = client.threadPool().getThreadContext();
        return (request, listener) -> {
            try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
                threadContext.markAsSystemContext();
                client.execute(TransportGetShardGenerationsAction.TYPE, request, listener);
            }
        };
    }

    private static List<LocalPrimary> localPrimaries(IndicesService indicesService, HollowShardsService hollowShardsService) {
        final List<LocalPrimary> primaries = new ArrayList<>();
        for (IndexService indexService : indicesService) {
            for (IndexShard shard : indexService) {
                final ShardRouting routing = shard.routingEntry();
                if (routing != null && routing.primary()) {
                    primaries.add(
                        new LocalPrimary(
                            new ShardCopy(shard.shardId(), routing.allocationId().getId()),
                            () -> shard.state() == IndexShardState.STARTED,
                            () -> hollowShardsService.isHollowShard(shard.shardId()),
                            new IndexShardLatestCommits(shard)
                        )
                    );
                }
            }
        }
        return primaries;
    }

    /**
     * Turns measuring on or off. Turning it on starts the evaluations; turning it off stops them and drops everything.
     */
    synchronized void setEnabled(boolean enabled) {
        final Period current = period;
        if (enabled && current == null) {
            final Period started = new Period(new ShardBaselineCache(reader, executor, threadPool::relativeTimeInMillis, budgetBytes));
            started.generations = new NextShardGenerations(
                sender,
                threadPool::relativeTimeInMillis,
                () -> executor.execute(() -> requestGenerations(started, false))
            );
            period = started;
            started.task = threadPool.scheduleWithFixedDelay(() -> evaluate(started), EVALUATION_INTERVAL, executor);
        } else if (enabled == false && current != null) {
            period = null;
            if (current.task != null) {
                current.task.cancel();
            }
            current.cache.close();
            current.results = Map.of();
        }
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        final Period current = period;
        if (current == null) {
            return;
        }
        final ClusterState state = event.state();
        final boolean relevant = current.state == null
            || event.previousState().custom(SnapshotsInProgress.TYPE) != state.custom(SnapshotsInProgress.TYPE)
            || event.metadataChanged()
            || event.routingTableChanged();
        if (relevant) {
            current.generations.onClusterState(state, shardIdsByRepository(state, localPrimaries.get()));
        }
        current.state = state;
        if (relevant && NextShardGenerations.isSupported(state)) {
            executor.execute(() -> requestGenerations(current, false));
        }
    }

    @Override
    public void afterIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard, Settings indexSettings) {
        final Period current = period;
        if (current != null && indexShard != null && indexShard.routingEntry() != null) {
            current.cache.removeShard(new ShardCopy(shardId, indexShard.routingEntry().allocationId().getId()));
        }
    }

    /**
     * The latest results: for each tracked repository, each started primary on this node, measured or not. Empty while off.
     */
    Map<ProjectRepo, Map<ShardId, Result>> getResults() {
        final Period current = period;
        return current == null ? Map.of() : current.results;
    }

    /**
     * Runs one evaluation now. Tests call it rather than wait for the scheduled one. It must run on a thread that may block.
     */
    void evaluate() {
        final Period current = period;
        if (current != null) {
            evaluate(current);
        }
    }

    private void evaluate(Period current) {
        assert Transports.assertNotTransportThread("measuring the snapshot backlog may read from disk");
        assert ClusterApplierService.assertNotClusterStateUpdateThread("measuring the snapshot backlog may read from disk");
        final ClusterState state = current.state;
        if (period != current || state == null) {
            return;
        }
        final Map<ProjectRepo, List<LocalPrimary>> byRepository = byRepository(state, localPrimaries.get());

        final Set<ShardCopy> retained = new HashSet<>();
        for (List<LocalPrimary> primaries : byRepository.values()) {
            for (LocalPrimary primary : primaries) {
                if (primary.hollow().getAsBoolean() == false) {
                    retained.add(primary.copy());
                }
            }
        }
        current.cache.retain(byRepository.keySet(), retained);

        final boolean supported = NextShardGenerations.isSupported(state);
        if (supported) {
            for (Map.Entry<ProjectRepo, List<LocalPrimary>> repositoryAndPrimaries : byRepository.entrySet()) {
                current.generations.maybeRequest(repositoryAndPrimaries.getKey(), shardIds(repositoryAndPrimaries.getValue()), true);
            }
        }

        final Map<ProjectRepo, Map<ShardId, Result>> results = new HashMap<>();
        for (Map.Entry<ProjectRepo, List<LocalPrimary>> repositoryAndPrimaries : byRepository.entrySet()) {
            final ProjectRepo projectRepo = repositoryAndPrimaries.getKey();
            final Map<ShardId, Result> repositoryResults = new HashMap<>();
            for (LocalPrimary primary : repositoryAndPrimaries.getValue()) {
                if (primary.started().getAsBoolean()) {
                    repositoryResults.put(
                        primary.shardId(),
                        supported ? measure(current, projectRepo, primary) : notMeasured(Reason.CLUSTER_TOO_OLD)
                    );
                }
            }
            results.put(projectRepo, Map.copyOf(repositoryResults));
        }
        if (period == current) {
            current.results = Map.copyOf(results);
        }
    }

    private static Result measure(Period current, ProjectRepo projectRepo, LocalPrimary primary) {
        final NextShardGenerations.Choice choice = current.generations.choose(projectRepo, primary.shardId());
        if (choice == null) {
            return notMeasured(Reason.NO_GENERATION);
        }
        if (primary.hollow().getAsBoolean()) {
            return new Measured(choice.generation(), 0);
        }
        return current.cache.measure(projectRepo, primary.copy(), choice.indexId(), choice.generation(), primary.commits());
    }

    private static Result notMeasured(Reason reason) {
        return new NotMeasured(reason);
    }

    private void requestGenerations(Period current, boolean fromEvaluation) {
        final ClusterState state = current.state;
        if (period != current || state == null || NextShardGenerations.isSupported(state) == false) {
            return;
        }
        for (Map.Entry<ProjectRepo, List<ShardId>> repositoryAndShards : shardIdsByRepository(state, localPrimaries.get()).entrySet()) {
            current.generations.maybeRequest(repositoryAndShards.getKey(), repositoryAndShards.getValue(), fromEvaluation);
        }
    }

    /**
     * The repositories named by the SLM policies of each project, if they are registered.
     */
    static Set<ProjectRepo> trackedRepositories(ClusterState state) {
        final Set<ProjectRepo> repositories = new HashSet<>();
        for (ProjectMetadata project : state.metadata().projects().values()) {
            final SnapshotLifecycleMetadata lifecycleMetadata = project.custom(SnapshotLifecycleMetadata.TYPE);
            if (lifecycleMetadata == null) {
                continue;
            }
            final RepositoriesMetadata registered = RepositoriesMetadata.get(project);
            for (SnapshotLifecyclePolicyMetadata policy : lifecycleMetadata.getSnapshotConfigurations().values()) {
                final String repository = policy.getPolicy().getRepository();
                if (registered.repository(repository) != null) {
                    repositories.add(new ProjectRepo(project.id(), repository));
                }
            }
        }
        return repositories;
    }

    // The primaries on this node to measure for each tracked repository: those of indices in the repository's project.
    private static Map<ProjectRepo, List<LocalPrimary>> byRepository(ClusterState state, List<LocalPrimary> primaries) {
        final Set<ProjectRepo> repositories = trackedRepositories(state);
        if (repositories.isEmpty()) {
            return Map.of();
        }
        final Map<ProjectRepo, List<LocalPrimary>> byRepository = new HashMap<>();
        for (ProjectRepo repository : repositories) {
            byRepository.put(repository, new ArrayList<>());
        }
        for (LocalPrimary primary : primaries) {
            final var project = state.metadata().lookupProject(primary.shardId().getIndex());
            if (project.isEmpty()) {
                continue;
            }
            for (ProjectRepo repository : repositories) {
                if (repository.projectId().equals(project.get().id())) {
                    byRepository.get(repository).add(primary);
                }
            }
        }
        return byRepository;
    }

    private static Map<ProjectRepo, List<ShardId>> shardIdsByRepository(ClusterState state, List<LocalPrimary> primaries) {
        final Map<ProjectRepo, List<ShardId>> shardIds = new HashMap<>();
        for (Map.Entry<ProjectRepo, List<LocalPrimary>> repositoryAndPrimaries : byRepository(state, primaries).entrySet()) {
            shardIds.put(repositoryAndPrimaries.getKey(), shardIds(repositoryAndPrimaries.getValue()));
        }
        return shardIds;
    }

    private static List<ShardId> shardIds(Collection<LocalPrimary> primaries) {
        return primaries.stream().map(LocalPrimary::shardId).toList();
    }

    private void registerMetrics(MeterRegistry meterRegistry) {
        meterRegistry.registerLongAsyncGauge(
            BACKLOG_METRIC,
            "Bytes the next snapshot of the measured primaries would upload",
            "bytes",
            measurement -> {
                for (Map.Entry<ProjectRepo, Map<ShardId, Result>> repositoryAndResults : getResults().entrySet()) {
                    long bytes = 0;
                    for (Result result : repositoryAndResults.getValue().values()) {
                        if (result instanceof Measured measured) {
                            bytes += measured.bytes();
                        }
                    }
                    measurement.record(bytes, Map.of("repo_name", repositoryAndResults.getKey().name()));
                }
            }
        );
        meterRegistry.registerLongAsyncGauge(
            MEASURED_SHARDS_METRIC,
            "Primaries whose snapshot backlog is measured",
            "unit",
            measurement -> {
                for (Map.Entry<ProjectRepo, Map<ShardId, Result>> repositoryAndResults : getResults().entrySet()) {
                    final long measured = repositoryAndResults.getValue()
                        .values()
                        .stream()
                        .filter(result -> result instanceof Measured)
                        .count();
                    measurement.record(measured, Map.of("repo_name", repositoryAndResults.getKey().name()));
                }
            }
        );
        meterRegistry.registerLongAsyncGauge(
            NOT_MEASURED_SHARDS_METRIC,
            "Primaries whose snapshot backlog is not measured, by reason",
            "unit",
            measurement -> {
                for (Map.Entry<ProjectRepo, Map<ShardId, Result>> repositoryAndResults : getResults().entrySet()) {
                    final Map<Reason, Long> byReason = new EnumMap<>(Reason.class);
                    for (Result result : repositoryAndResults.getValue().values()) {
                        if (result instanceof NotMeasured notMeasured) {
                            byReason.merge(notMeasured.reason(), 1L, Long::sum);
                        }
                    }
                    for (Map.Entry<Reason, Long> reasonAndCount : byReason.entrySet()) {
                        measurement.record(
                            reasonAndCount.getValue(),
                            Map.of("repo_name", repositoryAndResults.getKey().name(), "reason", reasonAndCount.getKey().name())
                        );
                    }
                }
            }
        );
        meterRegistry.registerLongAsyncGauge(
            BASELINES_METRIC,
            "Memory used by the baselines of the snapshot backlog",
            "bytes",
            measurement -> {
                final Period current = period;
                if (current != null) {
                    measurement.record(current.cache.ramBytesUsed());
                }
            }
        );
    }
}
