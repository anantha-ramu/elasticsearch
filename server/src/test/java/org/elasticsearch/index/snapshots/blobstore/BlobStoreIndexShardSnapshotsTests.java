/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.snapshots.blobstore;

import org.apache.lucene.util.Version;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 * Tests how a shard generation tracks files stored as byte ranges inside copied blobs.
 */
public class BlobStoreIndexShardSnapshotsTests extends ESTestCase {

    private static final String FIRST_SOURCE = "indices/aBcDeFgH/0/1/stateless_commit_5";
    private static final String SECOND_SOURCE = "indices/aBcDeFgH/0/2/stateless_commit_12";

    public void testCopiedBlobNamesAndLookupBySource() {
        final FileInfo inFirstCopy = copiedFile("_0.cfs", "c__first", 0L, FIRST_SOURCE);
        final FileInfo alsoInFirstCopy = copiedFile("_1.cfs", "c__first", between(16, 4096), FIRST_SOURCE);
        final FileInfo inSecondCopy = copiedFile("_2.cfs", "c__second", 0L, SECOND_SOURCE);
        final FileInfo uploaded = new FileInfo("__" + randomAlphaOfLength(10), metadata("_3.cfs"), null);

        final BlobStoreIndexShardSnapshots generation = BlobStoreIndexShardSnapshots.EMPTY.withAddedSnapshot(
            new SnapshotFiles("snap-1", List.of(inFirstCopy, uploaded), null)
        ).withAddedSnapshot(new SnapshotFiles("snap-2", List.of(inFirstCopy, alsoInFirstCopy, inSecondCopy, uploaded), null));
        assertThat(generation.copiedBlobNames(), equalTo(Set.of("c__first", "c__second")));
        assertThat(generation.findCopiedBlob(FIRST_SOURCE), equalTo("c__first"));
        assertThat(generation.findCopiedBlob(SECOND_SOURCE), equalTo("c__second"));
        assertThat(generation.findCopiedBlob("indices/aBcDeFgH/0/2/stateless_commit_13"), nullValue());

        // deleting snap-2 removes the only reference to the second copy, so delete may remove that blob
        final BlobStoreIndexShardSnapshots retained = generation.withRetainedSnapshots(Set.of(new SnapshotId("snap-1", "uuid-1")));
        assertThat(retained.copiedBlobNames(), equalTo(Set.of("c__first")));
        assertThat(retained.findCopiedBlob(FIRST_SOURCE), equalTo("c__first"));
        assertThat(retained.findCopiedBlob(SECOND_SOURCE), nullValue());

        // a clone refers to the same copies as its source snapshot
        final BlobStoreIndexShardSnapshots cloned = retained.withClone("snap-1", "clone-1");
        assertThat(cloned.copiedBlobNames(), equalTo(Set.of("c__first")));

        assertThat(BlobStoreIndexShardSnapshots.EMPTY.copiedBlobNames(), equalTo(Set.of()));
        assertThat(BlobStoreIndexShardSnapshots.EMPTY.findCopiedBlob(FIRST_SOURCE), nullValue());
    }

    public void testXContentRoundTripKeepsCopiedFiles() throws IOException {
        final BlobStoreIndexShardSnapshots generation = BlobStoreIndexShardSnapshots.EMPTY.withAddedSnapshot(
            new SnapshotFiles(
                "snap-1",
                List.of(
                    copiedFile("_0.cfs", "c__first", 0L, FIRST_SOURCE),
                    copiedFile("_1.cfs", "c__first", between(16, 4096), FIRST_SOURCE),
                    new FileInfo("__" + randomAlphaOfLength(10), metadata("_2.cfs"), null)
                ),
                randomBoolean() ? null : randomAlphaOfLength(10)
            )
        );

        final XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        generation.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        final BlobStoreIndexShardSnapshots parsed;
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
            parsed = BlobStoreIndexShardSnapshots.fromXContent(parser);
        }

        assertThat(parsed.copiedBlobNames(), equalTo(Set.of("c__first")));
        assertThat(parsed.findCopiedBlob(FIRST_SOURCE), equalTo("c__first"));
        for (SnapshotFiles snapshotFiles : generation) {
            for (FileInfo fileInfo : snapshotFiles.indexFiles()) {
                assertTrue(fileInfo + " changed in the round trip", parsed.findNameFile(fileInfo.name()).isSame(fileInfo));
            }
        }
    }

    public void testAddedSnapshotMustAgreeOnWhereBytesAreStored() {
        final FileInfo original = copiedFile("_0.cfs", "c__first", between(0, 4096), FIRST_SOURCE);
        final BlobStoreIndexShardSnapshots generation = BlobStoreIndexShardSnapshots.EMPTY.withAddedSnapshot(
            new SnapshotFiles("snap-1", List.of(original), null)
        );
        final String name = original.name();
        final StoreFileMetadata metadata = original.metadata();
        final List<FileInfo> disagreeing = List.of(
            new FileInfo(name, metadata, "c__other", original.offset(), original.source()),
            new FileInfo(name, metadata, original.blob(), original.offset() + 1, original.source()),
            new FileInfo(name, metadata, original.blob(), original.offset(), SECOND_SOURCE),
            new FileInfo(name, metadata, null)
        );
        for (FileInfo fileInfo : disagreeing) {
            expectThrows(AssertionError.class, () -> generation.withAddedSnapshot(new SnapshotFiles("snap-2", List.of(fileInfo), null)));
        }

        // an equal FileInfo in another instance is the same file, so adding and retaining snapshots with it is fine
        final FileInfo equal = new FileInfo(name, metadata, original.blob(), original.offset(), original.source());
        final BlobStoreIndexShardSnapshots withBoth = generation.withAddedSnapshot(new SnapshotFiles("snap-2", List.of(equal), null));
        final BlobStoreIndexShardSnapshots retained = withBoth.withRetainedSnapshots(
            Set.of(new SnapshotId("snap-1", "uuid-1"), new SnapshotId("snap-2", "uuid-2"))
        );
        assertThat(retained.copiedBlobNames(), equalTo(Set.of("c__first")));
    }

    public void testOneCopyPerCommitBlob() {
        final BlobStoreIndexShardSnapshots generation = BlobStoreIndexShardSnapshots.EMPTY.withAddedSnapshot(
            new SnapshotFiles(
                "snap-1",
                List.of(copiedFile("_0.cfs", "c__first", 0L, FIRST_SOURCE), copiedFile("_1.cfs", "c__second", 0L, FIRST_SOURCE)),
                null
            )
        );
        // delete still keeps both copies, because each one is referenced
        assertThat(generation.copiedBlobNames(), equalTo(Set.of("c__first", "c__second")));
        // but two copies of one commit blob in a generation means a snapshot failed to reuse the first copy
        expectThrows(AssertionError.class, () -> generation.findCopiedBlob(FIRST_SOURCE));
    }

    private static FileInfo copiedFile(String physicalName, String blob, long offset, String source) {
        return new FileInfo("__" + randomAlphaOfLength(10), metadata(physicalName), blob, offset, source);
    }

    private static StoreFileMetadata metadata(String physicalName) {
        return new StoreFileMetadata(physicalName, between(16, 1 << 20), randomAlphaOfLength(8), Version.LATEST.toString());
    }
}
