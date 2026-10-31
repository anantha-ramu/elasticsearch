/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots.backlog;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.version.CompatibilityVersions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshots;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.ProjectRepo;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.xpack.core.ilm.OperationMode;
import org.elasticsearch.xpack.core.security.authz.privilege.SystemPrivilege;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleMetadata;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicy;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicyMetadata;
import org.elasticsearch.xpack.stateless.snapshots.backlog.PublishSnapshotBacklogRequest.ShardBacklog;
import org.elasticsearch.xpack.stateless.snapshots.backlog.ShardBaseline.LuceneFile;
import org.elasticsearch.xpack.stateless.snapshots.backlog.ShardBaselineCache.LatestCommit;
import org.elasticsearch.xpack.stateless.snapshots.backlog.ShardBaselineCache.Measured;
import org.elasticsearch.xpack.stateless.snapshots.backlog.ShardBaselineCache.NotMeasured;
import org.elasticsearch.xpack.stateless.snapshots.backlog.ShardBaselineCache.Reason;
import org.elasticsearch.xpack.stateless.snapshots.backlog.ShardBaselineCache.ShardCopy;
import org.elasticsearch.xpack.stateless.snapshots.backlog.SnapshotBacklogTracker.LocalPrimary;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class SnapshotBacklogTrackerTests extends ESTestCase {

    private static final ProjectId PROJECT = ProjectId.DEFAULT;
    private static final ProjectRepo REPOSITORY = new ProjectRepo(PROJECT, "repo");
    private static final Index INDEX = new Index("index", "index-uuid");
    private static final ShardId SHARD = new ShardId(INDEX, 0);
    private static final IndexId INDEX_ID = new IndexId("index", "index-id-in-repository");
    private static final ShardGeneration RECORDED = new ShardGeneration("recorded");
    private static final long FILE_LENGTH = 100;

    private record Sent(GetShardGenerationsRequest request, ActionListener<GetShardGenerationsResponse> listener) {}

    private record Published(ClusterState state, Map<ProjectRepo, Map<ShardId, ShardBacklog>> report) {}

    private final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
    private final RecordingMeterRegistry meterRegistry = new RecordingMeterRegistry();
    private final List<Sent> sent = new ArrayList<>();
    private final List<ShardGeneration> reads = new ArrayList<>();
    private final AtomicInteger commitsTaken = new AtomicInteger();
    private final List<LocalPrimary> primaries = new ArrayList<>();
    private final List<Published> published = new ArrayList<>();
    private final SnapshotBacklogTracker tracker = new SnapshotBacklogTracker(
        taskQueue.getThreadPool(),
        () -> List.copyOf(primaries),
        (request, listener) -> sent.add(new Sent(request, listener)),
        (repository, indexId, shardId, generation, currentFiles) -> {
            reads.add(generation);
            return ShardBaseline.of(BlobStoreIndexShardSnapshots.EMPTY, currentFiles);
        },
        (state, report) -> published.add(new Published(state, report)),
        Long.MAX_VALUE,
        meterRegistry
    );
    private ClusterState lastState = ClusterState.EMPTY_STATE;

    public void testMeasuresAgainstTheMastersGeneration() {
        tracker.setEnabled(true);
        primaries.add(primary(SHARD, false));
        apply(state(TransportVersion.current(), project(PROJECT, List.of("repo"), List.of("repo"), INDEX)));
        assertThat(sent, hasSize(1));
        assertThat(sent.get(0).request().shardIds(), equalTo(List.of(SHARD)));

        tracker.evaluate();
        assertThat(tracker.getResults(), equalTo(Map.of(REPOSITORY, Map.of(SHARD, new NotMeasured(Reason.NO_GENERATION)))));

        answer(0, Map.of(SHARD, RECORDED));
        tracker.evaluate();
        assertThat(tracker.getResults(), equalTo(Map.of(REPOSITORY, Map.of(SHARD, new NotMeasured(Reason.LOADING)))));
        taskQueue.runAllRunnableTasks();
        assertThat(reads, equalTo(List.of(RECORDED)));

        tracker.evaluate();
        assertThat(tracker.getResults(), equalTo(Map.of(REPOSITORY, Map.of(SHARD, new Measured(RECORDED, FILE_LENGTH)))));
        assertThat(gauge(SnapshotBacklogTracker.BACKLOG_METRIC), equalTo(List.of(FILE_LENGTH)));
    }

    public void testNoRequestWhileClusterIsTooOld() {
        tracker.setEnabled(true);
        primaries.add(primary(SHARD, false));
        final TransportVersion older = TransportVersionUtils.getPreviousVersion(NextShardGenerations.SHARD_GENERATIONS_ACTION);
        apply(state(older, project(PROJECT, List.of("repo"), List.of("repo"), INDEX)));
        tracker.evaluate();
        taskQueue.runAllRunnableTasks();

        assertThat(sent, empty());
        assertThat(tracker.getResults(), equalTo(Map.of(REPOSITORY, Map.of(SHARD, new NotMeasured(Reason.CLUSTER_TOO_OLD)))));
        assertThat(
            notMeasuredGauge(),
            equalTo(Map.of(Map.<String, Object>of("repo_name", "repo", "reason", Reason.CLUSTER_TOO_OLD.name()), 1L))
        );

        // Once every node is upgraded, the master is asked.
        apply(state(TransportVersion.current(), project(PROJECT, List.of("repo"), List.of("repo"), INDEX)));
        assertThat(sent, hasSize(1));
    }

    public void testHollowShardCountsZeroWithoutReading() {
        tracker.setEnabled(true);
        primaries.add(primary(SHARD, true));
        apply(state(TransportVersion.current(), project(PROJECT, List.of("repo"), List.of("repo"), INDEX)));
        answer(0, Map.of(SHARD, RECORDED));

        tracker.evaluate();
        taskQueue.runAllRunnableTasks();
        tracker.evaluate();

        assertThat(tracker.getResults(), equalTo(Map.of(REPOSITORY, Map.of(SHARD, new Measured(RECORDED, 0)))));
        assertThat(reads, empty());
        assertThat(commitsTaken.get(), equalTo(0));
    }

    public void testOnlyRepositoriesOfPoliciesInTheShardsProject() {
        tracker.setEnabled(true);
        final ProjectId otherProject = ProjectId.fromId("other-project");
        final Index otherIndex = new Index("other-index", "other-index-uuid");
        final ShardId otherShard = new ShardId(otherIndex, 0);
        primaries.add(primary(SHARD, true));
        primaries.add(primary(otherShard, true));
        apply(
            state(
                TransportVersion.current(),
                // Two policies use the same repository; another repository is registered but no policy uses it; a policy names a
                // repository that isn't registered.
                project(PROJECT, List.of("repo", "unused"), List.of("repo", "repo", "missing"), INDEX),
                project(otherProject, List.of("other-repo"), List.of("other-repo"), otherIndex)
            )
        );
        final ProjectRepo otherRepository = new ProjectRepo(otherProject, "other-repo");
        assertThat(sent, hasSize(2));
        for (Sent request : sent) {
            final ShardId expected = request.request().projectRepo().equals(REPOSITORY) ? SHARD : otherShard;
            assertThat(request.request().shardIds(), equalTo(List.of(expected)));
        }

        tracker.evaluate();

        assertThat(
            tracker.getResults(),
            equalTo(
                Map.of(
                    REPOSITORY,
                    Map.of(SHARD, new NotMeasured(Reason.NO_GENERATION)),
                    otherRepository,
                    Map.of(otherShard, new NotMeasured(Reason.NO_GENERATION))
                )
            )
        );
    }

    public void testRemovedPolicyDropsBaselines() {
        tracker.setEnabled(true);
        primaries.add(primary(SHARD, false));
        apply(state(TransportVersion.current(), project(PROJECT, List.of("repo"), List.of("repo"), INDEX)));
        answer(0, Map.of(SHARD, RECORDED));
        tracker.evaluate();
        taskQueue.runAllRunnableTasks();
        assertThat(gauge(SnapshotBacklogTracker.BASELINES_METRIC).get(0) > 0, equalTo(true));

        // The policy is deleted: the repository is no longer measured, and its baselines are dropped.
        apply(state(TransportVersion.current(), project(PROJECT, List.of("repo"), List.of(), INDEX)));
        tracker.evaluate();

        assertThat(tracker.getResults(), anEmptyMap());
        assertThat(gauge(SnapshotBacklogTracker.BASELINES_METRIC), equalTo(List.of(0L)));
    }

    public void testAShardThatIsNotStartedIsNotMeasured() {
        tracker.setEnabled(true);
        final LocalPrimary recovering = new LocalPrimary(new ShardCopy(SHARD, "copy"), () -> false, () -> false, this::takeCommit);
        primaries.add(recovering);
        apply(state(TransportVersion.current(), project(PROJECT, List.of("repo"), List.of("repo"), INDEX)));
        // It is still asked about, so its generation is known once it starts.
        assertThat(sent, hasSize(1));

        tracker.evaluate();

        assertThat(tracker.getResults(), equalTo(Map.of(REPOSITORY, Map.of())));
    }

    public void testTurningOffClosesTheCacheAndSendsNothing() {
        tracker.setEnabled(true);
        primaries.add(primary(SHARD, false));
        apply(state(TransportVersion.current(), project(PROJECT, List.of("repo"), List.of("repo"), INDEX)));
        answer(0, Map.of(SHARD, RECORDED));
        // A load is queued, and hasn't run yet when measuring is turned off.
        tracker.evaluate();
        assertThat(tracker.getResults(), equalTo(Map.of(REPOSITORY, Map.of(SHARD, new NotMeasured(Reason.LOADING)))));

        tracker.setEnabled(false);
        taskQueue.runAllRunnableTasks();

        assertThat(reads, empty());
        assertThat(tracker.getResults(), anEmptyMap());
        assertThat(gauge(SnapshotBacklogTracker.BASELINES_METRIC), empty());
        assertThat(gauge(SnapshotBacklogTracker.BACKLOG_METRIC), empty());
        apply(state(TransportVersion.current(), project(PROJECT, List.of("repo"), List.of("repo"), INDEX)));
        tracker.evaluate();
        // The scheduled evaluation was cancelled too.
        while (taskQueue.hasDeferredTasks()) {
            taskQueue.advanceTime();
            taskQueue.runAllRunnableTasks();
        }
        assertThat(sent, hasSize(1));
        assertThat(reads, empty());

        // Turned on again, it starts with nothing.
        tracker.setEnabled(true);
        apply(state(TransportVersion.current(), project(PROJECT, List.of("repo"), List.of("repo"), INDEX)));
        tracker.evaluate();
        assertThat(sent, hasSize(2));
        assertThat(tracker.getResults(), equalTo(Map.of(REPOSITORY, Map.of(SHARD, new NotMeasured(Reason.NO_GENERATION)))));
    }

    public void testEvaluatesEveryThirtySeconds() {
        tracker.setEnabled(true);
        primaries.add(primary(SHARD, false));
        apply(state(TransportVersion.current(), project(PROJECT, List.of("repo"), List.of("repo"), INDEX)));
        assertThat(tracker.getResults(), anEmptyMap());

        assertThat(taskQueue.getLatestDeferredExecutionTime(), equalTo(SnapshotBacklogTracker.EVALUATION_INTERVAL.millis()));
        taskQueue.advanceTime();
        taskQueue.runAllRunnableTasks();

        assertThat(tracker.getResults(), equalTo(Map.of(REPOSITORY, Map.of(SHARD, new NotMeasured(Reason.NO_GENERATION)))));
    }

    public void testRequestIsSentInSystemContext() {
        final AtomicBoolean systemContext = new AtomicBoolean();
        final List<String> headers = new ArrayList<>();
        final var threadPool = taskQueue.getThreadPool();
        final var client = new NoOpClient(threadPool) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                assertThat(action, equalTo(TransportGetShardGenerationsAction.TYPE));
                systemContext.set(threadPool.getThreadContext().isSystemContext());
                headers.addAll(threadPool.getThreadContext().getHeaders().keySet());
            }
        };
        final ThreadContext threadContext = threadPool.getThreadContext();
        try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
            threadContext.putHeader("caller-header", "value");
            SnapshotBacklogTracker.systemContextSender(client)
                .send(new GetShardGenerationsRequest(TEST_REQUEST_TIMEOUT, REPOSITORY, List.of(SHARD)), ActionListener.noop());
            // The caller's context is back afterwards.
            assertFalse(threadContext.isSystemContext());
            assertThat(threadContext.getHeader("caller-header"), equalTo("value"));
        }

        assertTrue(systemContext.get());
        assertThat(headers, empty());
        // On a secured cluster the system user runs it, and may run every internal action.
        assertTrue(SystemPrivilege.INSTANCE.predicate().test(TransportGetShardGenerationsAction.NAME));
    }

    public void testMeasuredAndNotMeasuredShardsAreReported() {
        tracker.setEnabled(true);
        final ShardId neverBackedUp = new ShardId(INDEX, 1);
        final ShardId arrivedLater = new ShardId(INDEX, 2);
        primaries.add(primary(SHARD, false));
        primaries.add(primary(neverBackedUp, false));
        apply(state(TransportVersion.current(), project(PROJECT, List.of("repo"), List.of("repo"), INDEX)));
        // The repository has nothing of the second shard, and the third arrives after the request was sent.
        answer(0, Map.of(SHARD, RECORDED));
        primaries.add(primary(arrivedLater, false));
        tracker.evaluate();

        assertThat(
            notMeasuredGauge(),
            equalTo(
                Map.of(
                    Map.<String, Object>of("repo_name", "repo", "reason", Reason.LOADING.name()),
                    2L,
                    Map.<String, Object>of("repo_name", "repo", "reason", Reason.NO_GENERATION.name()),
                    1L
                )
            )
        );
        // The evaluation asked again, about all three.
        assertThat(sent, hasSize(2));
        assertThat(sent.get(1).request().shardIds(), hasSize(3));

        taskQueue.runAllRunnableTasks();
        tracker.evaluate();
        assertThat(gauge(SnapshotBacklogTracker.MEASURED_SHARDS_METRIC), equalTo(List.of(2L)));
        // Nothing of the second shard is backed up, so its whole commit counts.
        assertThat(gauge(SnapshotBacklogTracker.BACKLOG_METRIC), equalTo(List.of(2 * FILE_LENGTH)));
        // It needed no read.
        assertThat(reads, equalTo(List.of(RECORDED)));
    }

    public void testEvaluationPublishesItsResults() {
        tracker.setEnabled(true);
        primaries.add(primary(SHARD, false));
        apply(state(TransportVersion.current(), project(PROJECT, List.of("repo"), List.of("repo"), INDEX)));
        tracker.evaluate();

        // Not measured yet, but reported, with the shard copy's allocation id and the cluster state the evaluation used.
        assertThat(published, hasSize(1));
        assertThat(published.get(0).state(), equalTo(lastState));
        assertThat(published.get(0).report(), equalTo(Map.of(REPOSITORY, Map.of(SHARD, ShardBacklog.notMeasured("copy-0")))));

        answer(0, Map.of(SHARD, RECORDED));
        tracker.evaluate();
        taskQueue.runAllRunnableTasks();
        tracker.evaluate();

        assertThat(published, hasSize(3));
        assertThat(
            published.get(2).report(),
            equalTo(Map.of(REPOSITORY, Map.of(SHARD, new ShardBacklog("copy-0", RECORDED, FILE_LENGTH))))
        );
    }

    public void testEvaluationAfterTurningOffPublishesNothing() {
        tracker.setEnabled(true);
        apply(state(TransportVersion.current(), project(PROJECT, List.of("repo"), List.of("repo"), INDEX)));
        // Measuring is turned off while an evaluation runs: here, as it checks that the shard is started.
        primaries.add(new LocalPrimary(new ShardCopy(SHARD, "copy-0"), () -> {
            tracker.setEnabled(false);
            return true;
        }, () -> false, this::takeCommit));
        tracker.evaluate();

        assertThat(published, empty());
        assertThat(tracker.getResults(), anEmptyMap());
    }

    private LocalPrimary primary(ShardId shardId, boolean hollow) {
        return new LocalPrimary(new ShardCopy(shardId, "copy-" + shardId.id()), () -> true, () -> hollow, this::takeCommit);
    }

    private LatestCommit takeCommit() {
        commitsTaken.incrementAndGet();
        return new LatestCommit(null, List.of(new LuceneFile("_0.cfs", FILE_LENGTH, new BytesRef("writer"))));
    }

    private void apply(ClusterState state) {
        final ClusterState newState = ClusterState.builder(state).version(lastState.version() + 1).build();
        tracker.clusterChanged(new ClusterChangedEvent("test", newState, lastState));
        lastState = newState;
        taskQueue.runAllRunnableTasks();
    }

    private void answer(int request, Map<ShardId, ShardGeneration> generations) {
        final Map<ShardId, RepositoryShardGeneration> answer = new HashMap<>();
        generations.forEach((shardId, generation) -> answer.put(shardId, new RepositoryShardGeneration(INDEX_ID, generation)));
        sent.get(request).listener().onResponse(new GetShardGenerationsResponse(5, answer));
        taskQueue.runAllRunnableTasks();
    }

    private List<Long> gauge(String name) {
        meterRegistry.getRecorder().resetCalls();
        meterRegistry.getRecorder().collect();
        return meterRegistry.getRecorder()
            .getMeasurements(InstrumentType.LONG_ASYNC_GAUGE, name)
            .stream()
            .map(Measurement::getLong)
            .toList();
    }

    private Map<Map<String, Object>, Long> notMeasuredGauge() {
        meterRegistry.getRecorder().resetCalls();
        meterRegistry.getRecorder().collect();
        final Map<Map<String, Object>, Long> byAttributes = new HashMap<>();
        for (Measurement measurement : meterRegistry.getRecorder()
            .getMeasurements(InstrumentType.LONG_ASYNC_GAUGE, SnapshotBacklogTracker.NOT_MEASURED_SHARDS_METRIC)) {
            byAttributes.put(measurement.attributes(), measurement.getLong());
        }
        return byAttributes;
    }

    private record ProjectSpec(ProjectId id, List<String> registered, List<String> policyRepositories, Index index) {}

    private static ProjectSpec project(ProjectId id, List<String> registered, List<String> policyRepositories, Index index) {
        return new ProjectSpec(id, registered, policyRepositories, index);
    }

    private static ClusterState state(TransportVersion minTransportVersion, ProjectSpec... projects) {
        final Metadata.Builder metadata = Metadata.builder();
        for (ProjectSpec spec : projects) {
            final List<RepositoryMetadata> repositories = new ArrayList<>();
            for (String name : spec.registered()) {
                repositories.add(new RepositoryMetadata(new RepositoryMetadata(name, name + "-uuid", "fs", Settings.EMPTY), 5, 5));
            }
            final Map<String, SnapshotLifecyclePolicyMetadata> policies = new HashMap<>();
            for (int i = 0; i < spec.policyRepositories().size(); i++) {
                final String id = "policy-" + i;
                policies.put(
                    id,
                    SnapshotLifecyclePolicyMetadata.builder()
                        .setPolicy(new SnapshotLifecyclePolicy(id, "snap", "0 0/30 * * * ?", spec.policyRepositories().get(i), null, null))
                        .setVersion(1)
                        .setModifiedDate(0)
                        .build()
                );
            }
            final IndexMetadata indexMetadata = IndexMetadata.builder(spec.index().getName())
                .settings(indexSettings(IndexVersion.current(), spec.index().getUUID(), 3, 0))
                .build();
            metadata.put(
                ProjectMetadata.builder(spec.id())
                    .putCustom(RepositoriesMetadata.TYPE, new RepositoriesMetadata(repositories))
                    .putCustom(SnapshotLifecycleMetadata.TYPE, new SnapshotLifecycleMetadata(policies, OperationMode.RUNNING, null))
                    .put(indexMetadata, false)
            );
        }
        return ClusterState.builder(ClusterName.DEFAULT)
            .metadata(metadata)
            .putCompatibilityVersions("node", new CompatibilityVersions(minTransportVersion, Map.of()))
            .build();
    }
}
