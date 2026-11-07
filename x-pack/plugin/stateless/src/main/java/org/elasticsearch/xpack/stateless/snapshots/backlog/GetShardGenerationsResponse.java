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
 * The master's answer to a {@link GetShardGenerationsRequest}.
 */
public class GetShardGenerationsResponse extends ActionResponse {

    private final long repositoryGeneration;
    private final Map<ShardId, RepositoryShardGeneration> shardGenerations;

    /**
     * @param repositoryGeneration the generation of the repository data the answer comes from. The node that asked can use it to tell
     *                             whether the answer is older than a repository change it has already seen.
     * @param shardGenerations     for each shard asked about, where its backup metadata is. A shard missing from the map has no backup
     *                             metadata in the repository, so none of its files are backed up.
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
