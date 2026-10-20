/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots.backlog;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.repositories.ShardGenerations;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Tells an index node where a repository keeps the shard-level metadata ({@code index-{generation}}) of its shards, so that the node can
 * read which files the repository already has of them. The generations are in the {@link RepositoryData}, which only the master loads,
 * so index nodes ask the master, as peer recovery does for the latest snapshot of a shard (see
 * {@link org.elasticsearch.action.admin.cluster.snapshots.get.shard.TransportGetShardSnapshotAction}).
 * <p>
 * The answer only covers snapshots that have been finalized into the {@link RepositoryData}. A running snapshot that has already
 * finished a shard has written newer shard-level metadata for it, which the next snapshot of the shard will start from (see
 * {@link org.elasticsearch.snapshots.InFlightShardSnapshotStates}). That generation is in the {@link
 * org.elasticsearch.cluster.SnapshotsInProgress} of the cluster state, so a node that wants the generation the next snapshot of a shard
 * starts from takes it from there and uses this answer only for shards without one.
 */
public class TransportGetShardGenerationsAction extends TransportMasterNodeAction<GetShardGenerationsRequest, GetShardGenerationsResponse> {

    public static final String NAME = "internal:admin/stateless/snapshot/get_shard_generations";
    public static final ActionType<GetShardGenerationsResponse> TYPE = new ActionType<>(NAME);

    private final RepositoriesService repositoriesService;

    @Inject
    public TransportGetShardGenerationsAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        RepositoriesService repositoriesService
    ) {
        super(
            NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            GetShardGenerationsRequest::new,
            GetShardGenerationsResponse::new,
            // the repository data may have to be loaded from the repository, and the response can cover thousands of shards
            threadPool.executor(ThreadPool.Names.SNAPSHOT_META)
        );
        this.repositoriesService = repositoriesService;
    }

    @Override
    protected void masterOperation(
        Task task,
        GetShardGenerationsRequest request,
        ClusterState state,
        ActionListener<GetShardGenerationsResponse> listener
    ) {
        assert ThreadPool.assertCurrentThreadPool(ThreadPool.Names.SNAPSHOT_META);
        final var projectRepo = request.projectRepo();
        // throws if the repository does not exist, which is passed on to the node that asked
        final var repository = repositoriesService.repository(projectRepo.projectId(), projectRepo.name());
        repository.getRepositoryData(
            threadPool.executor(ThreadPool.Names.SNAPSHOT_META),
            listener.map(repositoryData -> getResponse(repositoryData, request.shardIds()))
        );
    }

    static GetShardGenerationsResponse getResponse(RepositoryData repositoryData, Collection<ShardId> shardIds) {
        final Map<ShardId, RepositoryShardGeneration> shardGenerations = new HashMap<>();
        for (ShardId shardId : shardIds) {
            final var shardGeneration = getShardGeneration(repositoryData, shardId);
            if (shardGeneration != null) {
                shardGenerations.put(shardId, shardGeneration);
            }
        }
        return new GetShardGenerationsResponse(repositoryData.getGenId(), shardGenerations);
    }

    /**
     * The index is looked up by name, as a new snapshot of the index does ({@link RepositoryData#resolveNewIndices}): if an index was
     * deleted and created again with the same name, its next snapshot is compared against what the repository holds of the old one.
     *
     * @return where the repository keeps the shard-level metadata of the shard, or {@code null} if it has none: the repository does not
     *         know the index or the shard, or it knows that it holds nothing of the shard yet ({@link ShardGenerations#NEW_SHARD_GEN}) or
     *         any more ({@link ShardGenerations#DELETED_SHARD_GEN})
     */
    private static RepositoryShardGeneration getShardGeneration(RepositoryData repositoryData, ShardId shardId) {
        final IndexId indexId = repositoryData.getIndices().get(shardId.getIndexName());
        if (indexId == null) {
            return null;
        }
        final ShardGeneration generation = repositoryData.shardGenerations().getShardGen(indexId, shardId.id());
        if (generation == null
            || generation.equals(ShardGenerations.NEW_SHARD_GEN)
            || generation.equals(ShardGenerations.DELETED_SHARD_GEN)) {
            return null;
        }
        return new RepositoryShardGeneration(indexId, generation);
    }

    @Override
    protected ClusterBlockException checkBlock(GetShardGenerationsRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
