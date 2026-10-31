/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots.backlog;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress.ShardSnapshotStatus;
import org.elasticsearch.cluster.SnapshotsInProgress.ShardState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.version.CompatibilityVersions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.ProjectRepo;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.repositories.ShardGenerations;
import org.elasticsearch.repositories.ShardSnapshotResult;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.stateless.snapshots.backlog.NextShardGenerations.Choice;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;

public class NextShardGenerationsTests extends ESTestCase {

    private static final ProjectRepo REPOSITORY = new ProjectRepo(ProjectId.DEFAULT, "repo");
    private static final String INDEX = "index";
    private static final IndexId INDEX_ID = new IndexId(INDEX, "index-id-in-repository");
    private static final ShardId SHARD_0 = new ShardId(INDEX, "index-uuid", 0);
    private static final ShardId SHARD_1 = new ShardId(INDEX, "index-uuid", 1);
    private static final ShardGeneration RECORDED = new ShardGeneration("recorded");
    private static final ShardGeneration IN_FLIGHT = new ShardGeneration("in-flight");
    private static final ShardGeneration LATER = new ShardGeneration("later");

    private record Sent(GetShardGenerationsRequest request, ActionListener<GetShardGenerationsResponse> listener) {}

    private final List<Sent> sent = new ArrayList<>();
    private int requestsNeeded;
    private final NextShardGenerations generations = new NextShardGenerations(
        (request, listener) -> sent.add(new Sent(request, listener)),
        () -> 0L,
        () -> requestsNeeded += 1
    );

    public void testAsksForAllPrimariesInOneRequestAndUsesTheAnswer() {
        onClusterState(state(5), Map.of(REPOSITORY, List.of(SHARD_0, SHARD_1)));
        assertThat(generations.choose(REPOSITORY, SHARD_0), nullValue());

        generations.maybeRequest(REPOSITORY, List.of(SHARD_0, SHARD_1), true);
        assertThat(sent, hasSize(1));
        assertThat(sent.get(0).request().projectRepo(), equalTo(REPOSITORY));
        assertThat(sent.get(0).request().shardIds(), equalTo(List.of(SHARD_0, SHARD_1)));
        answer(0, 5, Map.of(SHARD_0, RECORDED));

        assertThat(generations.choose(REPOSITORY, SHARD_0), equalTo(new Choice(INDEX_ID, RECORDED)));
        // The repository has no backup metadata for the other shard, so nothing of it is backed up.
        assertThat(generations.choose(REPOSITORY, SHARD_1).generation(), equalTo(ShardGenerations.NEW_SHARD_GEN));
    }

    public void testShardNeverBackedUpIsNotAskedAgain() {
        onClusterState(state(5), Map.of(REPOSITORY, List.of(SHARD_0)));
        generations.maybeRequest(REPOSITORY, List.of(SHARD_0), true);
        // The shard isn't in the answer, because the repository has nothing of it.
        answer(0, 5, Map.of());

        generations.maybeRequest(REPOSITORY, List.of(SHARD_0), true);
        generations.maybeRequest(REPOSITORY, List.of(SHARD_0), false);

        assertThat(sent, hasSize(1));
    }

    public void testANewPrimaryIsAskedAbout() {
        onClusterState(state(5), Map.of(REPOSITORY, List.of(SHARD_0)));
        generations.maybeRequest(REPOSITORY, List.of(SHARD_0), true);
        answer(0, 5, Map.of(SHARD_0, RECORDED));

        // Another primary arrived on the node. Until the master answers about it, it has no generation.
        onClusterState(state(5), Map.of(REPOSITORY, List.of(SHARD_0, SHARD_1)));
        assertThat(generations.choose(REPOSITORY, SHARD_1), nullValue());
        generations.maybeRequest(REPOSITORY, List.of(SHARD_0, SHARD_1), false);

        assertThat(sent, hasSize(2));
        assertThat(sent.get(1).request().shardIds(), equalTo(List.of(SHARD_0, SHARD_1)));
    }

    public void testInFlightGenerationBeatsMasterAnswer() {
        onClusterState(state(5), Map.of(REPOSITORY, List.of(SHARD_0)));
        generations.maybeRequest(REPOSITORY, List.of(SHARD_0), true);
        answer(0, 5, Map.of(SHARD_0, RECORDED));

        // A running snapshot has finished the shard. The next snapshot starts from what it wrote.
        onClusterState(state(5, snapshot("snap", ShardState.SUCCESS, IN_FLIGHT)), Map.of(REPOSITORY, List.of(SHARD_0)));

        assertThat(generations.choose(REPOSITORY, SHARD_0), equalTo(new Choice(INDEX_ID, IN_FLIGHT)));
    }

    public void testAShardStillBeingSnapshottedKeepsTheGenerationItStartedFrom() {
        onClusterState(state(5), Map.of(REPOSITORY, List.of(SHARD_0)));
        generations.maybeRequest(REPOSITORY, List.of(SHARD_0), true);
        answer(0, 5, Map.of(SHARD_0, RECORDED));

        onClusterState(state(5, snapshot("snap", ShardState.INIT, RECORDED)), Map.of(REPOSITORY, List.of(SHARD_0)));

        assertThat(generations.choose(REPOSITORY, SHARD_0), equalTo(new Choice(INDEX_ID, RECORDED)));
    }

    public void testKeepsFinishedSnapshotGenerationUntilAnswerCatchesUp() {
        onClusterState(state(5), Map.of(REPOSITORY, List.of(SHARD_0)));
        generations.maybeRequest(REPOSITORY, List.of(SHARD_0), true);
        answer(0, 5, Map.of(SHARD_0, RECORDED));
        onClusterState(state(5, snapshot("snap", ShardState.SUCCESS, IN_FLIGHT)), Map.of(REPOSITORY, List.of(SHARD_0)));

        // The snapshot is recorded in the repository (generation 6) and leaves the cluster state in the same update. The node still has
        // the master's answer at generation 5, which is out of date, but the snapshot's generation is what the next snapshot starts from.
        onClusterState(state(6), Map.of(REPOSITORY, List.of(SHARD_0)));
        assertThat(generations.choose(REPOSITORY, SHARD_0), equalTo(new Choice(INDEX_ID, IN_FLIGHT)));

        // An answer still at generation 5 changes nothing.
        generations.maybeRequest(REPOSITORY, List.of(SHARD_0), false);
        answer(1, 5, Map.of(SHARD_0, RECORDED));
        assertThat(generations.choose(REPOSITORY, SHARD_0), equalTo(new Choice(INDEX_ID, IN_FLIGHT)));

        // Once the answer is at generation 6, it is used.
        generations.maybeRequest(REPOSITORY, List.of(SHARD_0), true);
        answer(2, 6, Map.of(SHARD_0, IN_FLIGHT));
        assertThat(generations.choose(REPOSITORY, SHARD_0), equalTo(new Choice(INDEX_ID, IN_FLIGHT)));
        onClusterState(state(7), Map.of(REPOSITORY, List.of(SHARD_0)));
        assertThat(generations.choose(REPOSITORY, SHARD_0), nullValue());
    }

    public void testFailedFinalizationIsNotRemembered() {
        onClusterState(state(5), Map.of(REPOSITORY, List.of(SHARD_0)));
        onClusterState(state(5, snapshot("snap", ShardState.SUCCESS, IN_FLIGHT)), Map.of(REPOSITORY, List.of(SHARD_0)));

        // The snapshot failed to be recorded and left the cluster state without a write to the repository. Its generation lists files
        // the next snapshot would upload again, so it must not be used, even before the node has any answer from the master.
        onClusterState(state(5), Map.of(REPOSITORY, List.of(SHARD_0)));
        assertThat(generations.choose(REPOSITORY, SHARD_0), nullValue());

        // The master's answer at generation 5 is right.
        generations.maybeRequest(REPOSITORY, List.of(SHARD_0), true);
        answer(0, 5, Map.of(SHARD_0, RECORDED));
        assertThat(generations.choose(REPOSITORY, SHARD_0), equalTo(new Choice(INDEX_ID, RECORDED)));
    }

    public void testFinishedSnapshotIsNotRememberedIfMoreWasWritten() {
        onClusterState(state(5), Map.of(REPOSITORY, List.of(SHARD_0)));
        onClusterState(state(5, snapshot("snap", ShardState.SUCCESS, IN_FLIGHT)), Map.of(REPOSITORY, List.of(SHARD_0)));

        // The node didn't see a state in between: two writes, so the update that removed the snapshot isn't known to have recorded it.
        onClusterState(state(7), Map.of(REPOSITORY, List.of(SHARD_0)));

        assertThat(generations.choose(REPOSITORY, SHARD_0), nullValue());
    }

    public void testFinishedSnapshotIsNotRememberedAfterAMissedState() {
        onClusterState(state(5), Map.of(REPOSITORY, List.of(SHARD_0)));
        onClusterState(state(5, snapshot("snap", ShardState.SUCCESS, IN_FLIGHT)), Map.of(REPOSITORY, List.of(SHARD_0)));

        // One write and the snapshot gone, but the node missed a state in between (version 3): the snapshot may have failed to be
        // recorded, and something else written, so its generation isn't known to be what the next snapshot starts from.
        generations.onClusterState(ClusterState.builder(state(6)).version(4).build(), 2, Map.of(REPOSITORY, List.of(SHARD_0)));

        assertThat(generations.choose(REPOSITORY, SHARD_0), nullValue());
    }

    public void testRememberedGenerationDroppedWhenRepositoryMovesOn() {
        onClusterState(state(5), Map.of(REPOSITORY, List.of(SHARD_0)));
        generations.maybeRequest(REPOSITORY, List.of(SHARD_0), true);
        answer(0, 5, Map.of(SHARD_0, RECORDED));
        onClusterState(state(5, snapshot("snap", ShardState.SUCCESS, IN_FLIGHT)), Map.of(REPOSITORY, List.of(SHARD_0)));
        onClusterState(state(6), Map.of(REPOSITORY, List.of(SHARD_0)));
        assertThat(generations.choose(REPOSITORY, SHARD_0), equalTo(new Choice(INDEX_ID, IN_FLIGHT)));

        // Something else is written, for example a deletion that rewrote the shard's metadata.
        onClusterState(state(7), Map.of(REPOSITORY, List.of(SHARD_0)));

        assertThat(generations.choose(REPOSITORY, SHARD_0), nullValue());
    }

    public void testAnswerOlderThanRepositoryIsNotUsed() {
        onClusterState(state(5), Map.of(REPOSITORY, List.of(SHARD_0)));
        generations.maybeRequest(REPOSITORY, List.of(SHARD_0), true);
        answer(0, 5, Map.of(SHARD_0, RECORDED));

        // For example, a deletion was written, and the old metadata can list files that are gone.
        onClusterState(state(6), Map.of(REPOSITORY, List.of(SHARD_0)));

        assertThat(generations.choose(REPOSITORY, SHARD_0), nullValue());
        generations.maybeRequest(REPOSITORY, List.of(SHARD_0), false);
        assertThat(sent, hasSize(2));
        answer(1, 6, Map.of(SHARD_0, LATER));
        assertThat(generations.choose(REPOSITORY, SHARD_0), equalTo(new Choice(INDEX_ID, LATER)));
    }

    public void testOneRequestAtATimeAndOneMoreIfNeeded() {
        onClusterState(state(5), Map.of(REPOSITORY, List.of(SHARD_0)));
        generations.maybeRequest(REPOSITORY, List.of(SHARD_0), true);
        generations.maybeRequest(REPOSITORY, List.of(SHARD_0), true);
        generations.maybeRequest(REPOSITORY, List.of(SHARD_0), false);
        assertThat(sent, hasSize(1));

        answer(0, 5, Map.of(SHARD_0, RECORDED));
        // Requests asked for while one was running mean another is needed now.
        assertThat(requestsNeeded, equalTo(1));
    }

    public void testAnAnswerThatIsAlreadyOlderIsAskedForOnceMoreAtOnce() {
        onClusterState(state(6), Map.of(REPOSITORY, List.of(SHARD_0)));
        generations.maybeRequest(REPOSITORY, List.of(SHARD_0), true);
        answer(0, 5, Map.of(SHARD_0, RECORDED));
        assertThat(requestsNeeded, equalTo(1));

        generations.maybeRequest(REPOSITORY, List.of(SHARD_0), false);
        answer(1, 5, Map.of(SHARD_0, RECORDED));
        // Twice in a row: wait for the next evaluation rather than ask the master again and again.
        assertThat(requestsNeeded, equalTo(1));
        generations.maybeRequest(REPOSITORY, List.of(SHARD_0), false);
        assertThat(sent, hasSize(2));
        generations.maybeRequest(REPOSITORY, List.of(SHARD_0), true);
        assertThat(sent, hasSize(3));
    }

    public void testAfterAFailureOnlyTheNextEvaluationAsks() {
        onClusterState(state(5), Map.of(REPOSITORY, List.of(SHARD_0)));
        generations.maybeRequest(REPOSITORY, List.of(SHARD_0), true);
        sent.get(0).listener().onFailure(new ElasticsearchException("simulated"));
        assertThat(generations.choose(REPOSITORY, SHARD_0), nullValue());

        generations.maybeRequest(REPOSITORY, List.of(SHARD_0), false);
        assertThat(sent, hasSize(1));
        generations.maybeRequest(REPOSITORY, List.of(SHARD_0), true);
        assertThat(sent, hasSize(2));
    }

    public void testUnknownRepositoryGenerationAsksOnEveryEvaluation() {
        onClusterState(state(RepositoryData.UNKNOWN_REPO_GEN), Map.of(REPOSITORY, List.of(SHARD_0)));
        generations.maybeRequest(REPOSITORY, List.of(SHARD_0), true);
        answer(0, 5, Map.of(SHARD_0, RECORDED));
        assertThat(generations.choose(REPOSITORY, SHARD_0), equalTo(new Choice(INDEX_ID, RECORDED)));

        generations.maybeRequest(REPOSITORY, List.of(SHARD_0), false);
        assertThat(sent, hasSize(1));
        generations.maybeRequest(REPOSITORY, List.of(SHARD_0), true);
        assertThat(sent, hasSize(2));
    }

    public void testARepositoryRegisteredAgainStartsOver() {
        onClusterState(state(5), Map.of(REPOSITORY, List.of(SHARD_0)));
        generations.maybeRequest(REPOSITORY, List.of(SHARD_0), true);
        answer(0, 5, Map.of(SHARD_0, RECORDED));

        onClusterState(state(5, "another-uuid"), Map.of(REPOSITORY, List.of(SHARD_0)));

        assertThat(generations.choose(REPOSITORY, SHARD_0), nullValue());
        generations.maybeRequest(REPOSITORY, List.of(SHARD_0), false);
        assertThat(sent, hasSize(2));
    }

    public void testARepositoryRegisteredAgainWithoutAUuidYetStartsOver() {
        onClusterState(state(5), Map.of(REPOSITORY, List.of(SHARD_0)));
        generations.maybeRequest(REPOSITORY, List.of(SHARD_0), true);
        answer(0, 5, Map.of(SHARD_0, RECORDED));

        // Registered again, for example at another location: no uuid yet, and an unknown generation, so the old answer can't be seen
        // to be out of date. It must not be used.
        onClusterState(state(RepositoryData.UNKNOWN_REPO_GEN, RepositoryData.MISSING_UUID), Map.of(REPOSITORY, List.of(SHARD_0)));

        assertThat(generations.choose(REPOSITORY, SHARD_0), nullValue());
        generations.maybeRequest(REPOSITORY, List.of(SHARD_0), false);
        assertThat(sent, hasSize(2));
    }

    public void testARepositoryWhoseGenerationGoesBackStartsOver() {
        onClusterState(state(5), Map.of(REPOSITORY, List.of(SHARD_0)));
        generations.maybeRequest(REPOSITORY, List.of(SHARD_0), true);
        answer(0, 5, Map.of(SHARD_0, RECORDED));

        // Updated in place, keeping its uuid, but with an unknown generation, for example after its read-only setting changed: the
        // repository may have been written by someone else, so the old answer must not be used.
        onClusterState(state(RepositoryData.UNKNOWN_REPO_GEN), Map.of(REPOSITORY, List.of(SHARD_0)));

        assertThat(generations.choose(REPOSITORY, SHARD_0), nullValue());
        generations.maybeRequest(REPOSITORY, List.of(SHARD_0), false);
        assertThat(sent, hasSize(2));
    }

    public void testAnswerForARepositoryNoLongerTrackedIsIgnored() {
        onClusterState(state(5), Map.of(REPOSITORY, List.of(SHARD_0)));
        generations.maybeRequest(REPOSITORY, List.of(SHARD_0), true);
        onClusterState(state(5), Map.of());
        onClusterState(state(5), Map.of(REPOSITORY, List.of(SHARD_0)));

        answer(0, 5, Map.of(SHARD_0, RECORDED));

        assertThat(generations.choose(REPOSITORY, SHARD_0), nullValue());
    }

    public void testIsSupportedOnlyOnceEveryNodeIs() {
        assertTrue(NextShardGenerations.isSupported(state(5)));
        final TransportVersion older = TransportVersionUtils.getPreviousVersion(NextShardGenerations.SHARD_GENERATIONS_ACTION);
        final ClusterState mixed = ClusterState.builder(state(5))
            .putCompatibilityVersions("old-node", new CompatibilityVersions(older, Map.of()))
            .build();
        assertFalse(NextShardGenerations.isSupported(mixed));
        assertThat(sent, empty());
    }

    private long version;

    // Each cluster state directly follows the one before it.
    private void onClusterState(ClusterState state, Map<ProjectRepo, List<ShardId>> primaries) {
        version += 1;
        generations.onClusterState(ClusterState.builder(state).version(version).build(), version - 1, primaries);
    }

    private void answer(int request, long repositoryGeneration, Map<ShardId, ShardGeneration> shardGenerations) {
        final Map<ShardId, RepositoryShardGeneration> answer = new HashMap<>();
        shardGenerations.forEach((shardId, generation) -> answer.put(shardId, new RepositoryShardGeneration(INDEX_ID, generation)));
        sent.get(request).listener().onResponse(new GetShardGenerationsResponse(repositoryGeneration, answer));
    }

    private static ClusterState state(long repositoryGeneration, SnapshotsInProgress.Entry... snapshots) {
        return state(repositoryGeneration, "repo-uuid", snapshots);
    }

    private static ClusterState state(long repositoryGeneration, String repositoryUuid, SnapshotsInProgress.Entry... snapshots) {
        final RepositoryMetadata repository = new RepositoryMetadata(
            new RepositoryMetadata(REPOSITORY.name(), repositoryUuid, "fs", Settings.EMPTY),
            repositoryGeneration,
            repositoryGeneration
        );
        final ProjectMetadata project = ProjectMetadata.builder(ProjectId.DEFAULT)
            .putCustom(RepositoriesMetadata.TYPE, new RepositoriesMetadata(List.of(repository)))
            .build();
        SnapshotsInProgress snapshotsInProgress = SnapshotsInProgress.EMPTY;
        for (SnapshotsInProgress.Entry snapshot : snapshots) {
            snapshotsInProgress = snapshotsInProgress.withAddedEntry(snapshot);
        }
        return ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(project))
            .putCustom(SnapshotsInProgress.TYPE, snapshotsInProgress)
            .putCompatibilityVersions("node", new CompatibilityVersions(TransportVersion.current(), Map.of()))
            .build();
    }

    // A running snapshot of shard 0, finished with the given generation or still running from it.
    private static SnapshotsInProgress.Entry snapshot(String name, ShardState state, ShardGeneration generation) {
        final ShardSnapshotStatus status = state == ShardState.SUCCESS
            ? ShardSnapshotStatus.success("node", new ShardSnapshotResult(generation, ByteSizeValue.ZERO, 1))
            : new ShardSnapshotStatus("node", state, generation);
        return SnapshotsInProgress.startedEntry(
            new Snapshot(ProjectId.DEFAULT, REPOSITORY.name(), new SnapshotId(name, name + "-uuid")),
            false,
            false,
            Map.of(INDEX, INDEX_ID),
            List.of(),
            0,
            5,
            Map.of(SHARD_0, status, SHARD_1, new ShardSnapshotStatus("node", ShardState.INIT, RECORDED)),
            Map.of(),
            IndexVersion.current(),
            List.of()
        );
    }
}
