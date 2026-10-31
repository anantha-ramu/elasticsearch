/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots.backlog;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.ProjectRepo;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.stateless.snapshots.backlog.PublishSnapshotBacklogRequest.ShardBacklog;

import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;

public class PublishSnapshotBacklogRequestSerializationTests extends AbstractWireSerializingTestCase<PublishSnapshotBacklogRequest> {

    private static ShardBacklog randomShardBacklog() {
        return randomBoolean()
            ? ShardBacklog.notMeasured(randomUUID())
            : new ShardBacklog(randomUUID(), new ShardGeneration(randomUUID()), randomBoolean() ? 0 : randomNonNegativeLong());
    }

    private static Map<ProjectRepo, Map<ShardId, ShardBacklog>> randomShards() {
        final Map<ProjectRepo, Map<ShardId, ShardBacklog>> shards = new HashMap<>();
        for (int i = between(0, 3); i > 0; i--) {
            final Map<ShardId, ShardBacklog> byShard = new HashMap<>();
            for (int j = between(0, 10); j > 0; j--) {
                byShard.put(GetShardGenerationsRequestSerializationTests.randomShardId(), randomShardBacklog());
            }
            shards.put(new ProjectRepo(randomProjectIdOrDefault(), randomIdentifier()), byShard);
        }
        return shards;
    }

    @Override
    protected Writeable.Reader<PublishSnapshotBacklogRequest> instanceReader() {
        return PublishSnapshotBacklogRequest::new;
    }

    @Override
    protected PublishSnapshotBacklogRequest createTestInstance() {
        return new PublishSnapshotBacklogRequest(
            randomTimeValue(),
            randomIdentifier(),
            randomUUID(),
            randomNonNegativeLong(),
            randomShards()
        );
    }

    @Override
    protected PublishSnapshotBacklogRequest mutateInstance(PublishSnapshotBacklogRequest instance) {
        final TimeValue timeout = instance.masterNodeTimeout();
        final String nodeId = instance.nodeId();
        final String ephemeralId = instance.nodeEphemeralId();
        final long seqNo = instance.seqNo();
        final var shards = instance.shards();
        return switch (between(0, 4)) {
            case 0 -> new PublishSnapshotBacklogRequest(
                randomValueOtherThan(timeout, ESTestCase::randomTimeValue),
                nodeId,
                ephemeralId,
                seqNo,
                shards
            );
            case 1 -> new PublishSnapshotBacklogRequest(
                timeout,
                randomValueOtherThan(nodeId, ESTestCase::randomIdentifier),
                ephemeralId,
                seqNo,
                shards
            );
            case 2 -> new PublishSnapshotBacklogRequest(
                timeout,
                nodeId,
                randomValueOtherThan(ephemeralId, ESTestCase::randomUUID),
                seqNo,
                shards
            );
            case 3 -> new PublishSnapshotBacklogRequest(
                timeout,
                nodeId,
                ephemeralId,
                randomValueOtherThan(seqNo, ESTestCase::randomNonNegativeLong),
                shards
            );
            case 4 -> new PublishSnapshotBacklogRequest(
                timeout,
                nodeId,
                ephemeralId,
                seqNo,
                randomValueOtherThan(shards, PublishSnapshotBacklogRequestSerializationTests::randomShards)
            );
            default -> throw new AssertionError("unexpected branch");
        };
    }

    /**
     * A measured 0 and a shard that was not measured stay different on the wire, so the master can tell them apart.
     */
    public void testMeasuredZeroAndNotMeasuredStayApart() throws IOException {
        final ProjectRepo repository = new ProjectRepo(randomProjectIdOrDefault(), "repo");
        final ShardId measuredZero = new ShardId(new Index("index", "index-uuid"), 0);
        final ShardId notMeasured = new ShardId(new Index("index", "index-uuid"), 1);
        final var request = new PublishSnapshotBacklogRequest(
            TimeValue.THIRTY_SECONDS,
            "node",
            "ephemeral",
            1,
            Map.of(
                repository,
                Map.of(
                    measuredZero,
                    new ShardBacklog("copy-0", new ShardGeneration("generation"), 0),
                    notMeasured,
                    ShardBacklog.notMeasured("copy-1")
                )
            )
        );
        final var copy = copyInstance(request);
        assertTrue(copy.shards().get(repository).get(measuredZero).isMeasured());
        assertThat(copy.shards().get(repository).get(measuredZero).bytes(), equalTo(0L));
        assertFalse(copy.shards().get(repository).get(notMeasured).isMeasured());
    }

    /**
     * The size of one node's report with thousands of primaries, with names and ids as they look in serverless: a data stream backing
     * index name, and random base64 uuids for the index, the allocation id and the generation.
     */
    public void testReportSizeForThousandsOfShards() throws IOException {
        final int shards = 2_000;
        final Map<ShardId, ShardBacklog> byShard = new HashMap<>();
        for (int i = 0; i < shards; i++) {
            final Index index = new Index(
                ".ds-logs-generic.otel-default-2026.10.31-" + backingIndexGeneration(i),
                UUIDs.randomBase64UUID(random())
            );
            byShard.put(
                new ShardId(index, 0),
                new ShardBacklog(
                    UUIDs.randomBase64UUID(random()),
                    new ShardGeneration(UUIDs.randomBase64UUID(random())),
                    randomLongBetween(0, 1L << 34)
                )
            );
        }
        final var request = new PublishSnapshotBacklogRequest(
            TimeValue.THIRTY_SECONDS,
            UUIDs.randomBase64UUID(random()),
            UUIDs.randomBase64UUID(random()),
            1,
            Map.of(new ProjectRepo(randomProjectIdOrDefault(), "found-snapshots"), byShard)
        );
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            request.writeTo(out);
            final long bytesPerShard = out.size() / shards;
            logger.info("a report of {} shards in one repository is {} bytes, {} bytes per shard", shards, out.size(), bytesPerShard);
            // the design note estimates about 120 bytes per shard and repository
            assertThat(bytesPerShard, lessThan(150L));
        }
    }

    private static String backingIndexGeneration(int i) {
        return String.format(Locale.ROOT, "%06d", i);
    }
}
