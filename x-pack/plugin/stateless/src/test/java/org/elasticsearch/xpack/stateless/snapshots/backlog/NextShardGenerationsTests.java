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
        generations.onClusterState(state(5), Map.of(REPOSITORY, List.of(SHARD_0, SHARD_1)));
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
        generations.onClusterState(state(5), Map.of(REPOSITORY, List.of(SHARD_0)));
        generations.maybeRequest(REPOSITORY, List.of(SHARD_0), true);
        // The shard isn't in the answer, because the repository has nothing of it.
        answer(0, 5, Map.of());

        generations.maybeRequest(REPOSITORY, List.of(SHARD_0), true);
        generations.maybeRequest(REPOSITORY, List.of(SHARD_0), false);

        assertThat(sent, hasSize(1));
    }

    public void testANewPrimaryIsAskedAbout() {
        generations.onClusterState(state(5), Map.of(REPOSITORY, List.of(SHARD_0)));
        generations.maybeRequest(REPOSITORY, List.of(SHARD_0), true);
        answer(0, 5, Map.of(SHARD_0, RECORDED));

        // Another primary arrived on the node. Until the master answers about it, it has no generation.
        generations.onClusterState(state(5), Map.of(REPOSITORY, List.of(SHARD_0, SHARD_1)));
        assertThat(generations.choose(REPOSITORY, SHARD_1), nullValue());
        generations.maybeRequest(REPOSITORY, List.of(SHARD_0, SHARD_1), false);

        assertThat(sent, hasSize(2));
        assertThat(sent.get(1).request().shardIds(), equalTo(List.of(SHARD_0, SHARD_1)));
    }

    public void testInFlightGenerationBeatsMasterAnswer() {
        generations.onClusterState(state(5), Map.of(REPOSITORY, List.of(SHARD_0)));
        generations.maybeRequest(REPOSITORY, List.of(SHARD_0), true);
        answer(0, 5, Map.of(SHARD_0, RECORDED));

        // A running snapshot has finished the shard. The next snapshot starts from what it wrote.
        generations.onClusterState(state(5, snapshot("snap", ShardState.SUCCESS, IN_FLIGHT)), Map.of(REPOSITORY, List.of(SHARD_0)));

        assertThat(generations.choose(REPOSITORY, SHARD_0), equalTo(new Choice(INDEX_ID, IN_FLIGHT)));
    }

    public void testAShardStillBeingSnapshottedKeepsTheGenerationItStartedFrom() {
        generations.onClusterState(state(5), Map.of(REPOSITORY, List.of(SHARD_0)));
        generations.maybeRequest(REPOSITORY, List.of(SHARD_0), true);
        answer(0, 5, Map.of(SHARD_0, RECORDED));

        generations.onClusterState(state(5, snapshot("snap", ShardState.INIT, RECORDED)), Map.of(REPOSITORY, List.of(SHARD_0)));

        assertThat(generations.choose(REPOSITORY, SHARD_0), equalTo(new Choice(INDEX_ID, RECORDED)));
    }

    public void testKeepsFinishedSnapshotGenerationUntilAnswerCatchesUp() {
        generations.onClusterState(state(5), Map.of(REPOSITORY, List.of(SHARD_0)));
        generations.maybeRequest(REPOSITORY, List.of(SHARD_0), true);
        answer(0, 5, Map.of(SHARD_0, RECORDED));
        generations.onClusterState(state(5, snapshot("snap", ShardState.SUCCESS, IN_FLIGHT)), Map.of(REPOSITORY, List.of(SHARD_0)));

        // The snapshot is recorded in the repository (generation 6) and leaves the cluster state in the same update. The node still has
        // the master's answer at generation 5, which is out of date, but the snapshot's generation is what the next snapshot starts from.
        generations.onClusterState(state(6), Map.of(REPOSITORY, List.of(SHARD_0)));
        assertThat(generations.choose(REPOSITORY, SHARD_0), equalTo(new Choice(INDEX_ID, IN_FLIGHT)));

        // An answer still at generation 5 changes nothing.
        generations.maybeRequest(REPOSITORY, List.of(SHARD_0), false);
        answer(1, 5, Map.of(SHARD_0, RECORDED));
        assertThat(generations.choose(REPOSITORY, SHARD_0), equalTo(new Choice(INDEX_ID, IN_FLIGHT)));

        // Once the answer is at generation 6, it is used.
        generations.maybeRequest(REPOSITORY, List.of(SHARD_0), true);
        answer(2, 6, Map.of(SHARD_0, IN_FLIGHT));
        assertThat(generations.choose(REPOSITORY, SHARD_0), equalTo(new Choice(INDEX_ID, IN_FLIGHT)));
        generations.onClusterState(state(7), Map.of(REPOSITORY, List.of(SHARD_0)));
        assertThat(generations.choose(REPOSITORY, SHARD_0), nullValue());
    }

    public void testFailedFinalizationIsNotRemembered() {
        generations.onClusterState(state(5), Map.of(REPOSITORY, List.of(SHARD_0)));
        generations.maybeRequest(REPOSITORY, List.of(SHARD_0), true);
        answer(0, 5, Map.of(SHARD_0, RECORDED));
        generations.onClusterState(state(5, snapshot("snap", ShardState.SUCCESS, IN_FLIGHT)), Map.of(REPOSITORY, List.of(SHARD_0)));

        // The snapshot failed to be recorded and left the cluster state without a write to the repository. Its generation lists files
        // the next snapshot would upload again, so it must not be used; the master's answer at generation 5 is still right.
        generations.onClusterState(state(5), Map.of(REPOSITORY, List.of(SHARD_0)));

        assertThat(generations.choose(REPOSITORY, SHARD_0), equalTo(new Choice(INDEX_ID, RECORDED)));
    }

    public void testFinishedSnapshotIsNotRememberedIfMoreWasWritten() {
        generations.onClusterState(state(5), Map.of(REPOSITORY, List.of(SHARD_0)));
        generations.onClusterState(state(5, snapshot("snap", ShardState.SUCCESS, IN_FLIGHT)), Map.of(REPOSITORY, List.of(SHARD_0)));

        // The node didn't see a state in between: two writes, so the update that removed the snapshot isn't known to have recorded it.
        generations.onClusterState(state(7), Map.of(REPOSITORY, List.of(SHARD_0)));

        assertThat(generations.choose(REPOSITORY, SHARD_0), nullValue());
    }

    public void testRememberedGenerationDroppedWhenRepositoryMovesOn() {
        generations.onClusterState(state(5), Map.of(REPOSITORY, List.of(SHARD_0)));
        generations.maybeRequest(REPOSITORY, List.of(SHARD_0), true);
        answer(0, 5, Map.of(SHARD_0, RECORDED));
        generations.onClusterState(state(5, snapshot("snap", ShardState.SUCCESS, IN_FLIGHT)), Map.of(REPOSITORY, List.of(SHARD_0)));
        generations.onClusterState(state(6), Map.of(REPOSITORY, List.of(SHARD_0)));
        assertThat(generations.choose(REPOSITORY, SHARD_0), equalTo(new Choice(INDEX_ID, IN_FLIGHT)));

        // Something else is written, for example a deletion that rewrote the shard's metadata.
        generations.onClusterState(state(7), Map.of(REPOSITORY, List.of(SHARD_0)));

        assertThat(generations.choose(REPOSITORY, SHARD_0), nullValue());
    }

    public void testAnswerOlderThanRepositoryIsNotUsed() {
        generations.onClusterState(state(5), Map.of(REPOSITORY, List.of(SHARD_0)));
        generations.maybeRequest(REPOSITORY, List.of(SHARD_0), true);
        answer(0, 5, Map.of(SHARD_0, RECORDED));

        // For example, a deletion was written, and the old metadata can list files that are gone.
        generations.onClusterState(state(6), Map.of(REPOSITORY, List.of(SHARD_0)));

        assertThat(generations.choose(REPOSITORY, SHARD_0), nullValue());
        generations.maybeRequest(REPOSITORY, List.of(SHARD_0), false);
        assertThat(sent, hasSize(2));
        answer(1, 6, Map.of(SHARD_0, LATER));
        assertThat(generations.choose(REPOSITORY, SHARD_0), equalTo(new Choice(INDEX_ID, LATER)));
    }

    public void testOneRequestAtATimeAndOneMoreIfNeeded() {
        generations.onClusterState(state(5), Map.of(REPOSITORY, List.of(SHARD_0)));
        generations.maybeRequest(REPOSITORY, List.of(SHARD_0), true);
        generations.maybeRequest(REPOSITORY, List.of(SHARD_0), true);
        generations.maybeRequest(REPOSITORY, List.of(SHARD_0), false);
        assertThat(sent, hasSize(1));

        answer(0, 5, Map.of(SHARD_0, RECORDED));
        // Requests asked for while one was running mean another is needed now.
        assertThat(requestsNeeded, equalTo(1));
    }

    public void testAnAnswerThatIsAlreadyOlderIsAskedForOnceMoreAtOnce() {
        generations.onClusterState(state(6), Map.of(REPOSITORY, List.of(SHARD_0)));
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
        generations.onClusterState(state(5), Map.of(REPOSITORY, List.of(SHARD_0)));
        generations.maybeRequest(REPOSITORY, List.of(SHARD_0), true);
        sent.get(0).listener().onFailure(new ElasticsearchException("simulated"));
        assertThat(generations.choose(REPOSITORY, SHARD_0), nullValue());

        generations.maybeRequest(REPOSITORY, List.of(SHARD_0), false);
        assertThat(sent, hasSize(1));
        generations.maybeRequest(REPOSITORY, List.of(SHARD_0), true);
        assertThat(sent, hasSize(2));
    }

    public void testUnknownRepositoryGenerationAsksOnEveryEvaluation() {
        generations.onClusterState(state(RepositoryData.UNKNOWN_REPO_GEN), Map.of(REPOSITORY, List.of(SHARD_0)));
        generations.maybeRequest(REPOSITORY, List.of(SHARD_0), true);
        answer(0, 5, Map.of(SHARD_0, RECORDED));
        assertThat(generations.choose(REPOSITORY, SHARD_0), equalTo(new Choice(INDEX_ID, RECORDED)));

        generations.maybeRequest(REPOSITORY, List.of(SHARD_0), false);
        assertThat(sent, hasSize(1));
        generations.maybeRequest(REPOSITORY, List.of(SHARD_0), true);
        assertThat(sent, hasSize(2));
    }

    public void testARepositoryRegisteredAgainStartsOver() {
        generations.onClusterState(state(5), Map.of(REPOSITORY, List.of(SHARD_0)));
        generations.maybeRequest(REPOSITORY, List.of(SHARD_0), true);
        answer(0, 5, Map.of(SHARD_0, RECORDED));

        generations.onClusterState(state(5, "another-uuid"), Map.of(REPOSITORY, List.of(SHARD_0)));

        assertThat(generations.choose(REPOSITORY, SHARD_0), nullValue());
        generations.maybeRequest(REPOSITORY, List.of(SHARD_0), false);
        assertThat(sent, hasSize(2));
    }

    public void testAnswerForARepositoryNoLongerTrackedIsIgnored() {
        generations.onClusterState(state(5), Map.of(REPOSITORY, List.of(SHARD_0)));
        generations.maybeRequest(REPOSITORY, List.of(SHARD_0), true);
        generations.onClusterState(state(5), Map.of());
        generations.onClusterState(state(5), Map.of(REPOSITORY, List.of(SHARD_0)));

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
