/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots.backlog;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * The answer to a {@link GetShardGenerationsRequest}.
 */
public class GetShardGenerationsResponse extends ActionResponse {

    private final long repositoryGeneration;
    private final Map<ShardId, RepositoryShardGeneration> shardGenerations;

    /**
     * @param repositoryGeneration the generation of the repository data the answer is from, so that the node that asked can tell whether
     *                             the answer is older than a repository generation it has seen since
     * @param shardGenerations     where the repository keeps the shard-level metadata of each shard asked for. A shard that is not in
     *                             the map has no shard-level metadata in the repository, so the repository holds none of its files.
     */
    public GetShardGenerationsResponse(long repositoryGeneration, Map<ShardId, RepositoryShardGeneration> shardGenerations) {
        this.repositoryGeneration = repositoryGeneration;
        this.shardGenerations = Map.copyOf(shardGenerations);
    }

    public GetShardGenerationsResponse(StreamInput in) throws IOException {
        this.repositoryGeneration = in.readLong();
        this.shardGenerations = in.readImmutableMap(ShardId::new, RepositoryShardGeneration::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(repositoryGeneration);
        out.writeMap(shardGenerations, StreamOutput::writeWriteable, StreamOutput::writeWriteable);
    }

    public long repositoryGeneration() {
        return repositoryGeneration;
    }

    public Map<ShardId, RepositoryShardGeneration> shardGenerations() {
        return shardGenerations;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GetShardGenerationsResponse that = (GetShardGenerationsResponse) o;
        return repositoryGeneration == that.repositoryGeneration && shardGenerations.equals(that.shardGenerations);
    }

    @Override
    public int hashCode() {
        return Objects.hash(repositoryGeneration, shardGenerations);
    }

    @Override
    public String toString() {
        return "GetShardGenerationsResponse{repositoryGeneration=" + repositoryGeneration + ", shards=" + shardGenerations.size() + '}';
    }
}
