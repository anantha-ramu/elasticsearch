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
 * Tells an index node where to find the backup metadata ({@code index-{generation}}) of its shards in a repository, so the node can read
 * which of their files are already backed up.
 * <p>
 * Each shard's current metadata generation is recorded in the repository's top-level data ({@link RepositoryData}). Only the master
 * loads that, so index nodes ask the master. Peer recovery does the same to find a shard's latest snapshot (see
 * {@link org.elasticsearch.action.admin.cluster.snapshots.get.shard.TransportGetShardSnapshotAction}).
 * <p>
 * The answer only covers finished snapshots, the ones already recorded in the {@link RepositoryData}. A snapshot that is still running
 * may already have written newer metadata for a shard it has finished, and the shard's next snapshot will start from that (see
 * {@link org.elasticsearch.snapshots.InFlightShardSnapshotStates}). That generation is in the cluster state
 * ({@link org.elasticsearch.cluster.SnapshotsInProgress}). So a node that wants the generation the next snapshot will start from takes it
 * from there when it's present, and uses this answer for the other shards.
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
            // runs on the snapshot-meta pool: loading the repository data can mean reading from the repository, and one answer can
            // cover thousands of shards
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
        // throws if the repository doesn't exist, and the node that asked gets that error
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
     * Looks the index up by name, as a new snapshot does ({@link RepositoryData#resolveNewIndices}). So if an index was deleted and
     * created again under the same name, the answer points at the old index's metadata, which is what its next snapshot compares
     * against.
     *
     * @return where the shard's backup metadata is, or {@code null} if there is none: the repository doesn't know the index or the
     *         shard, or it knows it holds nothing of the shard yet ({@link ShardGenerations#NEW_SHARD_GEN}) or any more
     *         ({@link ShardGenerations#DELETED_SHARD_GEN})
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
