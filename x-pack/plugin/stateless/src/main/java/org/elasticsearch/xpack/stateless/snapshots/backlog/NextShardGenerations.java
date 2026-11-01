/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots.backlog;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.ProjectRepo;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.repositories.ShardGenerations;
import org.elasticsearch.snapshots.InFlightShardSnapshotStates;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.LongSupplier;

/**
 * Works out, for each primary shard on this node and each repository it is measured for, which generation of the shard's backup
 * metadata ({@code index-{generation}}) the next snapshot of the shard will start from. That is the generation its backlog must be
 * measured against ({@link ShardBaselineCache#measure}).
 * <p>
 * The master picks it the same way when it starts a shard snapshot ({@link InFlightShardSnapshotStates#generationForShard}): the
 * shard's generation from a snapshot that is still running and has already finished the shard, else the one in the repository's
 * top-level data ({@link RepositoryData}). Only the master loads that data, so this class asks the master for it
 * ({@link TransportGetShardGenerationsAction}), one request per repository for all the node's primaries of the repository's project.
 * <p>
 * Every generation this class gives out was published after its blob was written in full, which {@link ShardBaselineCache} relies on:
 * it comes from a running snapshot in the cluster state, from a snapshot that just finished (below), or from the master's answer. Or it
 * is {@link ShardGenerations#NEW_SHARD_GEN}, which means nothing is backed up and nothing is read.
 * <p>
 * How each case is handled:
 * <ul>
 *     <li><b>A running snapshot has finished the shard.</b> Its new generation is used, from the cluster state.</li>
 *     <li><b>That snapshot then ends.</b> It leaves the cluster state in the same update that records its generations in the
 *     repository's top-level data, one repository generation up. The node may not have the master's answer for that repository
 *     generation yet, so it keeps using the shard's generation from the snapshot until it does, and only while the repository stays at
 *     that generation. It does that only if the snapshot left in the very next cluster state after the one the node saw it in, and the
 *     repository generation went up by exactly one in it. Every write of the repository's top-level data is published in a cluster
 *     state of its own, and the only such write that removes a running snapshot is that snapshot being recorded. So these two checks
 *     mean the update was the snapshot being recorded. Otherwise, for example if the snapshot failed to be recorded, or the node
 *     missed a cluster state in which something else was written, the snapshot's generation may not be what the next snapshot starts
 *     from, and its metadata may list files as backed up that the next snapshot would still upload. The shard then waits for the
 *     master's answer.</li>
 *     <li><b>The master's answer is older than the repository</b>, because something was written since. The answer isn't used, as
 *     after a deletion its metadata could list files as backed up that are gone. A new answer is asked for.</li>
 *     <li><b>A shard isn't in the master's answer.</b> If the request asked about it, the repository has no backup metadata for it,
 *     so it is measured against {@link ShardGenerations#NEW_SHARD_GEN}: nothing is backed up. If the request didn't ask about it, for
 *     example because it has just started, a new answer is asked for.</li>
 *     <li><b>The cluster state doesn't know the repository's generation</b> (it is negative, below
 *     {@link RepositoryData#EMPTY_REPO_GEN}). Answers can't be seen to be out of date, so they are used, and asked for again on every
 *     evaluation.</li>
 *     <li><b>A repository is registered again under the same name</b>, which the cluster state shows as a known uuid changing, or as
 *     the generation going down (a new registration starts with no uuid and an unknown generation). Everything known about it is
 *     dropped.</li>
 * </ul>
 * Requests: at most one per repository at a time. If the reason for another arises while one is running, another is sent when it
 * returns. If an answer comes back already older than the repository, one more is sent at once. After a failure, or a second answer
 * in a row that is already older, nothing more is sent for the repository until the next evaluation ({@code fromEvaluation}), so a
 * failing master gets at most one request per repository per evaluation.
 */
final class NextShardGenerations {

    private static final Logger logger = LogManager.getLogger(NextShardGenerations.class);

    /**
     * Nodes from this version on can answer {@link TransportGetShardGenerationsAction}. Nothing is asked until every node in the
     * cluster is on it, since any node can become the master.
     */
    static final TransportVersion SHARD_GENERATIONS_ACTION = TransportVersion.fromName("snapshot_backlog_shard_generations");

    static final TimeValue MASTER_NODE_TIMEOUT = TimeValue.timeValueSeconds(30);
    static final TimeValue WARNING_INTERVAL = TimeValue.timeValueMinutes(10);

    /**
     * Sends a request to the master.
     */
    interface Sender {
        void send(GetShardGenerationsRequest request, ActionListener<GetShardGenerationsResponse> listener);
    }

    /**
     * The generation of a shard's backup metadata to measure against, and the id of the shard's index in the repository, which the
     * path of the metadata blob needs.
     */
    record Choice(IndexId indexId, ShardGeneration generation) {}

    // The master's answer, and the shards the request asked about.
    private record Answer(long repositoryGeneration, Set<ShardId> askedAbout, Map<ShardId, RepositoryShardGeneration> generations) {}

    // A shard's generation from a snapshot that has ended, used until there is an answer at the repository generation that recorded it.
    private record Remembered(Choice choice, long repositoryGeneration) {}

    // What is known about one repository. Guarded by the lock of the enclosing object.
    private static final class Repository {
        // From the last cluster state seen.
        String uuid;
        long generation;
        InFlightShardSnapshotStates inFlight;
        Map<String, IndexId> inFlightIndexIds;
        final Map<ShardId, Remembered> remembered = new HashMap<>();
        @Nullable
        Answer answer;
        boolean requesting;
        // Whether something changed while a request was running, so another is needed when it returns.
        boolean requestAgain;
        // How many answers in a row were already older than the repository when they came back.
        int olderAnswersInARow;
        // Whether only the next evaluation may send a request, after a failure or answers that kept being older.
        boolean waitForEvaluation;
        long nextWarningMillis = Long.MIN_VALUE;
        int failuresSinceWarning;

        Repository(String uuid, long generation, InFlightShardSnapshotStates inFlight, Map<String, IndexId> inFlightIndexIds) {
            this.uuid = uuid;
            this.generation = generation;
            this.inFlight = inFlight;
            this.inFlightIndexIds = inFlightIndexIds;
        }
    }

    private final Sender sender;
    private final LongSupplier relativeTimeMillis;
    private final Runnable onRequestNeeded;

    private final Map<ProjectRepo, Repository> repositories = new HashMap<>();

    /**
     * @param onRequestNeeded called when an answer comes back and another request should be sent now, by calling {@link #maybeRequest}
     *                        for each repository. It is called on the thread that handles the answer, so it should fork.
     */
    NextShardGenerations(Sender sender, LongSupplier relativeTimeMillis, Runnable onRequestNeeded) {
        this.sender = sender;
        this.relativeTimeMillis = relativeTimeMillis;
        this.onRequestNeeded = onRequestNeeded;
    }

    /**
     * Whether every node in the cluster can answer where shards' backup metadata is.
     */
    static boolean isSupported(ClusterState state) {
        return state.getMinTransportVersion().supports(SHARD_GENERATIONS_ACTION);
    }

    /**
     * Takes in a new cluster state, in the order the node applies them. States in which no running snapshot, metadata or routing
     * changed may be left out.
     *
     * @param previousVersion the version of the cluster state the node applied just before this one. If it is one less than this
     *                        state's, the node missed no state in between, and can tell what a single update did.
     * @param primaries       for each repository to track, the shards on this node that are primaries of its project. The repository
     *                        must be registered in the given state. Repositories not given are forgotten.
     */
    synchronized void onClusterState(ClusterState state, long previousVersion, Map<ProjectRepo, ? extends Collection<ShardId>> primaries) {
        // Each published cluster state's version is one more than the one before, so a bigger step means the node missed a state.
        final boolean followsPrevious = state.version() == previousVersion + 1;
        repositories.keySet().retainAll(primaries.keySet());
        final SnapshotsInProgress snapshotsInProgress = SnapshotsInProgress.get(state);
        for (Map.Entry<ProjectRepo, ? extends Collection<ShardId>> repositoryAndShards : primaries.entrySet()) {
            final ProjectRepo projectRepo = repositoryAndShards.getKey();
            final Collection<ShardId> shardIds = repositoryAndShards.getValue();
            final RepositoryMetadata metadata = RepositoriesMetadata.get(state.metadata().getProject(projectRepo.projectId()))
                .repository(projectRepo.name());
            assert metadata != null : projectRepo;
            final long generation = metadata.generation();
            final List<SnapshotsInProgress.Entry> entries = snapshotsInProgress.forRepo(projectRepo);
            final InFlightShardSnapshotStates inFlight = InFlightShardSnapshotStates.forEntries(entries);
            final Map<String, IndexId> inFlightIndexIds = indexIds(entries);

            final Repository repository = repositories.get(projectRepo);
            if (repository == null || isRegisteredAgain(repository, metadata)) {
                repositories.put(projectRepo, new Repository(metadata.uuid(), generation, inFlight, inFlightIndexIds));
                continue;
            }
            // A successful snapshot leaves the cluster state in the same update that writes it to the repository, one generation up.
            final boolean recorded = followsPrevious
                && repository.generation >= RepositoryData.EMPTY_REPO_GEN
                && generation == repository.generation + 1;
            if (recorded) {
                for (ShardId shardId : shardIds) {
                    final Choice before = inFlightGeneration(repository.inFlight, repository.inFlightIndexIds, shardId);
                    if (before != null && inFlightGeneration(inFlight, inFlightIndexIds, shardId) == null) {
                        repository.remembered.put(shardId, new Remembered(before, generation));
                    }
                }
            }
            repository.remembered.values().removeIf(remembered -> remembered.repositoryGeneration() != generation);
            repository.remembered.keySet().retainAll(new HashSet<>(shardIds));
            // If the repository's uuid has just become known, it is still the same repository.
            repository.uuid = metadata.uuid();
            repository.generation = generation;
            repository.inFlight = inFlight;
            repository.inFlightIndexIds = inFlightIndexIds;
        }
    }

    /**
     * The generation the next snapshot of the shard will start from, or {@code null} if it isn't known yet.
     */
    @Nullable
    synchronized Choice choose(ProjectRepo projectRepo, ShardId shardId) {
        final Repository repository = repositories.get(projectRepo);
        if (repository == null) {
            return null;
        }
        final Choice inFlight = inFlightGeneration(repository.inFlight, repository.inFlightIndexIds, shardId);
        if (inFlight != null) {
            return inFlight;
        }
        final Answer answer = repository.answer;
        final Remembered remembered = repository.remembered.get(shardId);
        if (remembered != null && (answer == null || answer.repositoryGeneration() < remembered.repositoryGeneration())) {
            return remembered.choice();
        }
        if (answer == null || answer.askedAbout().contains(shardId) == false || isOlderThanRepository(answer, repository)) {
            return null;
        }
        final RepositoryShardGeneration shardGeneration = answer.generations().get(shardId);
        if (shardGeneration == null) {
            // The repository has no backup metadata for the shard. Nothing is read for this generation, so the index id is never used.
            return new Choice(new IndexId(shardId.getIndexName(), shardId.getIndex().getUUID()), ShardGenerations.NEW_SHARD_GEN);
        }
        return new Choice(shardGeneration.indexId(), shardGeneration.generation());
    }

    /**
     * Asks the master for the generations of the given shards in the repository, if the answer this node has is missing, older than
     * the repository, or didn't ask about one of them. Call it only if {@link #isSupported}.
     *
     * @param shardIds       the shards on this node that are primaries of the repository's project
     * @param fromEvaluation whether this is the regular evaluation, which may send a request after a failure
     */
    void maybeRequest(ProjectRepo projectRepo, Collection<ShardId> shardIds, boolean fromEvaluation) {
        final Repository repository;
        final GetShardGenerationsRequest request;
        synchronized (this) {
            repository = repositories.get(projectRepo);
            if (repository == null || shardIds.isEmpty() || needsRequest(repository, shardIds, fromEvaluation) == false) {
                return;
            }
            if (repository.requesting) {
                repository.requestAgain = true;
                return;
            }
            if (repository.waitForEvaluation && fromEvaluation == false) {
                return;
            }
            repository.waitForEvaluation = false;
            repository.requesting = true;
            request = new GetShardGenerationsRequest(MASTER_NODE_TIMEOUT, projectRepo, List.copyOf(shardIds));
        }
        sender.send(
            request,
            ActionListener.wrap(
                response -> onResponse(projectRepo, repository, request, response),
                e -> onFailure(projectRepo, repository, e)
            )
        );
    }

    private void onResponse(
        ProjectRepo projectRepo,
        Repository repository,
        GetShardGenerationsRequest request,
        GetShardGenerationsResponse response
    ) {
        final boolean requestNow;
        synchronized (this) {
            if (repositories.get(projectRepo) != repository) {
                // The repository is no longer tracked, or was registered again.
                return;
            }
            repository.requesting = false;
            repository.answer = new Answer(response.repositoryGeneration(), Set.copyOf(request.shardIds()), response.shardGenerations());
            // The answer covers what was remembered until it reached this repository generation.
            repository.remembered.values().removeIf(remembered -> remembered.repositoryGeneration() <= response.repositoryGeneration());
            if (isOlderThanRepository(repository.answer, repository)) {
                repository.olderAnswersInARow += 1;
                repository.waitForEvaluation = repository.olderAnswersInARow > 1;
                requestNow = repository.waitForEvaluation == false;
            } else {
                repository.olderAnswersInARow = 0;
                requestNow = repository.requestAgain;
            }
            repository.requestAgain = false;
        }
        if (requestNow) {
            onRequestNeeded.run();
        }
    }

    private void onFailure(ProjectRepo projectRepo, Repository repository, Exception e) {
        synchronized (this) {
            if (repositories.get(projectRepo) != repository) {
                return;
            }
            repository.requesting = false;
            repository.requestAgain = false;
            repository.waitForEvaluation = true;
            logger.debug(() -> Strings.format("[%s] failed to get the shard generations from the master", projectRepo), e);
            final long now = relativeTimeMillis.getAsLong();
            repository.failuresSinceWarning += 1;
            if (now >= repository.nextWarningMillis) {
                logger.warn(
                    "[{}] failed to get shard generations from the master {} time(s) since the last warning; the latest failure was [{}]",
                    projectRepo,
                    repository.failuresSinceWarning,
                    e.getMessage()
                );
                repository.failuresSinceWarning = 0;
                repository.nextWarningMillis = now + WARNING_INTERVAL.millis();
            }
        }
    }

    private static boolean needsRequest(Repository repository, Collection<ShardId> shardIds, boolean fromEvaluation) {
        return repository.answer == null
            || isOlderThanRepository(repository.answer, repository)
            || repository.answer.askedAbout().containsAll(shardIds) == false
            // Without a known generation in the cluster state, an answer can't be seen to be out of date, so ask on every evaluation.
            || (fromEvaluation && repository.generation < RepositoryData.EMPTY_REPO_GEN);
    }

    // An unknown repository generation in the cluster state (negative, below EMPTY_REPO_GEN) puts no limit on the answer.
    private static boolean isOlderThanRepository(Answer answer, Repository repository) {
        return repository.generation >= RepositoryData.EMPTY_REPO_GEN && answer.repositoryGeneration() < repository.generation;
    }

    // A new registration starts with no uuid and an unknown generation; a uuid only ever goes from missing to known otherwise, and the
    // generation only goes up.
    private static boolean isRegisteredAgain(Repository repository, RepositoryMetadata metadata) {
        return (repository.uuid.equals(RepositoryData.MISSING_UUID) == false && repository.uuid.equals(metadata.uuid()) == false)
            || metadata.generation() < repository.generation;
    }

    @Nullable
    private static Choice inFlightGeneration(InFlightShardSnapshotStates inFlight, Map<String, IndexId> indexIds, ShardId shardId) {
        final IndexId indexId = indexIds.get(shardId.getIndexName());
        if (indexId == null) {
            return null;
        }
        // With no shard generations of its own, this gives only a generation from a running snapshot.
        final ShardGeneration generation = inFlight.generationForShard(indexId, shardId.id(), ShardGenerations.EMPTY);
        return generation == null ? null : new Choice(indexId, generation);
    }

    // The ids of the indices in the running snapshots, by name. A later snapshot wins, as its generations win in
    // InFlightShardSnapshotStates.
    private static Map<String, IndexId> indexIds(List<SnapshotsInProgress.Entry> entries) {
        if (entries.isEmpty()) {
            return Map.of();
        }
        final Map<String, IndexId> indexIds = new HashMap<>();
        for (SnapshotsInProgress.Entry entry : entries) {
            indexIds.putAll(entry.indices());
        }
        return indexIds;
    }
}
