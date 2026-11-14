/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots.backlog;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.stateless.snapshots.backlog.GetShardGenerationsRequestSerializationTests.randomShardId;

public class GetShardGenerationsResponseSerializationTests extends AbstractWireSerializingTestCase<GetShardGenerationsResponse> {

    private static RepositoryShardGeneration randomShardGeneration() {
        return new RepositoryShardGeneration(new IndexId(randomIdentifier(), randomUUID()), new ShardGeneration(randomUUID()));
    }

    @Override
    protected Writeable.Reader<GetShardGenerationsResponse> instanceReader() {
        return GetShardGenerationsResponse::new;
    }

    @Override
    protected GetShardGenerationsResponse createTestInstance() {
        final Map<ShardId, RepositoryShardGeneration> shardGenerations = new HashMap<>();
        for (int i = between(0, 10); i > 0; i--) {
            shardGenerations.put(randomShardId(), randomShardGeneration());
        }
        return new GetShardGenerationsResponse(randomLongBetween(-1, Long.MAX_VALUE), shardGenerations);
    }

    @Override
    protected GetShardGenerationsResponse mutateInstance(GetShardGenerationsResponse instance) {
        final var shardGenerations = new HashMap<>(instance.shardGenerations());
        return switch (between(0, 2)) {
            case 0 -> new GetShardGenerationsResponse(
                randomValueOtherThan(instance.repositoryGeneration(), () -> randomLongBetween(-1, Long.MAX_VALUE)),
                shardGenerations
            );
            case 1 -> {
                if (shardGenerations.isEmpty() == false && randomBoolean()) {
                    shardGenerations.remove(randomFrom(shardGenerations.keySet()));
                } else {
                    shardGenerations.put(
                        randomValueOtherThanMany(shardGenerations::containsKey, () -> randomShardId()),
                        randomShardGeneration()
                    );
                }
                yield new GetShardGenerationsResponse(instance.repositoryGeneration(), shardGenerations);
            }
            case 2 -> {
                if (shardGenerations.isEmpty()) {
                    shardGenerations.put(randomShardId(), randomShardGeneration());
                } else {
                    final var shardId = randomFrom(shardGenerations.keySet());
                    shardGenerations.put(shardId, randomValueOtherThan(shardGenerations.get(shardId), () -> randomShardGeneration()));
                }
                yield new GetShardGenerationsResponse(instance.repositoryGeneration(), shardGenerations);
            }
            default -> throw new AssertionError("unexpected mutation");
        };
    }
}
