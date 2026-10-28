/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots.backlog;

import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.IndexMetaDataGenerations;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.repositories.ShardGenerations;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.equalTo;

public class TransportGetShardGenerationsActionTests extends ESTestCase {

    private static final IndexId INDEX = new IndexId("index", "index-id");
    private static final IndexId OTHER_INDEX = new IndexId("other", "other-id");

    private static ShardId shardId(String indexName, int id) {
        return new ShardId(new Index(indexName, indexName + "-uuid"), id);
    }

    private static RepositoryData repositoryData(long generation, ShardGenerations shardGenerations) {
        final var snapshot = new SnapshotId("snap", "snap-uuid");
        return new RepositoryData(
            "repo-uuid",
            generation,
            Map.of(snapshot.getUUID(), snapshot),
            Map.of(),
            Map.of(INDEX, List.of(snapshot), OTHER_INDEX, List.of(snapshot)),
            shardGenerations,
            IndexMetaDataGenerations.EMPTY,
            "cluster-uuid"
        );
    }

    public void testShardsOfSeveralIndicesAreAnsweredTogether() {
        final var gen0 = new ShardGeneration(randomUUID());
        final var gen1 = new ShardGeneration(randomUUID());
        final var otherGen = new ShardGeneration(randomUUID());
        final long repositoryGeneration = randomNonNegativeLong();
        final var data = repositoryData(
            repositoryGeneration,
            ShardGenerations.builder().put(INDEX, 0, gen0).put(INDEX, 1, gen1).put(OTHER_INDEX, 0, otherGen).build()
        );

        final var response = TransportGetShardGenerationsAction.getResponse(
            data,
            List.of(shardId("index", 0), shardId("index", 1), shardId("other", 0))
        );

        assertThat(response.repositoryGeneration(), equalTo(repositoryGeneration));
        assertThat(
            response.shardGenerations(),
            equalTo(
                Map.of(
                    shardId("index", 0),
                    new RepositoryShardGeneration(INDEX, gen0),
                    shardId("index", 1),
                    new RepositoryShardGeneration(INDEX, gen1),
                    shardId("other", 0),
                    new RepositoryShardGeneration(OTHER_INDEX, otherGen)
                )
            )
        );
    }

    public void testShardsWithoutShardLevelMetadataAreLeftOut() {
        final var gen0 = new ShardGeneration(randomUUID());
        final var data = repositoryData(
            randomNonNegativeLong(),
            ShardGenerations.builder()
                .put(INDEX, 0, gen0)
                .put(INDEX, 1, ShardGenerations.NEW_SHARD_GEN)
                .put(INDEX, 2, ShardGenerations.DELETED_SHARD_GEN)
                .build()
        );

        final var response = TransportGetShardGenerationsAction.getResponse(
            data,
            List.of(
                shardId("index", 0),
                shardId("index", 1), // its first snapshot has not completed yet
                shardId("index", 2), // no snapshot refers to it any more
                shardId("index", 3), // past the shards the repository knows of the index, e.g. after a split
                shardId("absent", 0) // an index the repository does not know
            )
        );

        assertThat(response.shardGenerations(), equalTo(Map.of(shardId("index", 0), new RepositoryShardGeneration(INDEX, gen0))));
    }

    public void testIndexIsFoundByName() {
        final var gen0 = new ShardGeneration(randomUUID());
        final var data = repositoryData(randomNonNegativeLong(), ShardGenerations.builder().put(INDEX, 0, gen0).build());
        // an index created again under the same name gets a new uuid, but its next snapshot uses the index id the repository has for
        // that name
        final var recreatedShard = new ShardId(new Index("index", randomUUID()), 0);

        final var response = TransportGetShardGenerationsAction.getResponse(data, List.of(recreatedShard));

        assertThat(response.shardGenerations(), equalTo(Map.of(recreatedShard, new RepositoryShardGeneration(INDEX, gen0))));
    }

    public void testEmptyRepositoryHasNoShardLevelMetadata() {
        final var response = TransportGetShardGenerationsAction.getResponse(RepositoryData.EMPTY, List.of(shardId("index", 0)));

        assertThat(response.repositoryGeneration(), equalTo(RepositoryData.EMPTY_REPO_GEN));
        assertThat(response.shardGenerations(), anEmptyMap());
    }
}
