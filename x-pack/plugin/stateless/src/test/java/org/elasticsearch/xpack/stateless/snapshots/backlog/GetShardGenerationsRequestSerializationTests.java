/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots.backlog;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.ProjectRepo;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.nullValue;

public class GetShardGenerationsRequestSerializationTests extends AbstractWireSerializingTestCase<GetShardGenerationsRequest> {

    static ShardId randomShardId() {
        return new ShardId(new Index(randomIdentifier(), randomUUID()), between(0, 10));
    }

    private static List<ShardId> randomShardIds() {
        return randomList(1, 10, GetShardGenerationsRequestSerializationTests::randomShardId);
    }

    private static ProjectRepo randomProjectRepo() {
        return new ProjectRepo(randomProjectIdOrDefault(), randomIdentifier());
    }

    @Override
    protected Writeable.Reader<GetShardGenerationsRequest> instanceReader() {
        return GetShardGenerationsRequest::new;
    }

    @Override
    protected GetShardGenerationsRequest createTestInstance() {
        return new GetShardGenerationsRequest(randomTimeValue(), randomProjectRepo(), randomShardIds());
    }

    @Override
    protected GetShardGenerationsRequest mutateInstance(GetShardGenerationsRequest instance) {
        return switch (between(0, 2)) {
            case 0 -> new GetShardGenerationsRequest(
                randomValueOtherThan(instance.masterNodeTimeout(), ESTestCase::randomTimeValue),
                instance.projectRepo(),
                instance.shardIds()
            );
            case 1 -> new GetShardGenerationsRequest(
                instance.masterNodeTimeout(),
                randomValueOtherThan(instance.projectRepo(), GetShardGenerationsRequestSerializationTests::randomProjectRepo),
                instance.shardIds()
            );
            case 2 -> {
                final var shardIds = new ArrayList<>(instance.shardIds());
                if (shardIds.size() > 1 && randomBoolean()) {
                    shardIds.remove(between(0, shardIds.size() - 1));
                } else {
                    shardIds.add(randomValueOtherThanMany(shardIds::contains, GetShardGenerationsRequestSerializationTests::randomShardId));
                }
                yield new GetShardGenerationsRequest(instance.masterNodeTimeout(), instance.projectRepo(), shardIds);
            }
            default -> throw new AssertionError("unexpected mutation");
        };
    }

    public void testShardsAreRequired() {
        final var request = new GetShardGenerationsRequest(randomTimeValue(), randomProjectRepo(), List.of());
        assertThat(request.validate().validationErrors(), contains("no shards given"));
    }

    public void testRequestWithShardsIsValid() {
        assertThat(createTestInstance().validate(), nullValue());
    }
}
