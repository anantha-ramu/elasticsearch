/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots.backlog;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.ShardGeneration;

import java.io.IOException;

/**
 * Where to find a shard's backup metadata in a repository: the id the repository gives the shard's index, and the generation of the
 * shard's metadata file ({@code index-{generation}}) to read.
 */
public record RepositoryShardGeneration(IndexId indexId, ShardGeneration generation) implements Writeable {

    public RepositoryShardGeneration(StreamInput in) throws IOException {
        this(new IndexId(in), new ShardGeneration(in));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        indexId.writeTo(out);
        generation.writeTo(out);
    }
}
