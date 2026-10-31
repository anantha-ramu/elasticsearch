/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots.backlog;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Version;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshots;
import org.elasticsearch.index.snapshots.blobstore.SnapshotFiles;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.repositories.ProjectRepo;
import org.elasticsearch.repositories.blobstore.ChecksumBlobStoreFormat;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xpack.stateless.snapshots.backlog.ShardBaseline.LuceneFile;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.elasticsearch.repositories.blobstore.BlobStoreRepository.INDEX_SHARD_SNAPSHOTS_FORMAT;
import static org.elasticsearch.repositories.blobstore.BlobStoreRepository.SNAPSHOT_INDEX_NAME_FORMAT;
import static org.hamcrest.Matchers.equalTo;

public class ShardBaselineFormatTests extends ESTestCase {

    private static final ProjectRepo REPOSITORY = new ProjectRepo(ProjectId.DEFAULT, "repo");

    /**
     * Reading a blob straight into a baseline gives the same baseline as reading the whole metadata and then building one, for
     * random metadata: clones, snapshots without a state id, snapshots removed by retention, current files that the repository has
     * under the same writer uuid, under another one, or not at all, and metadata written with or without writer uuids.
     */
    public void testGivesTheSameBaselineAsReadingTheWholeMetadata() throws IOException {
        final List<FileInfo> backedUp = new ArrayList<>();
        final List<LuceneFile> currentFiles = new ArrayList<>();
        for (int i = between(0, 30); i > 0; i--) {
            final var file = new LuceneFile("_" + i + ".cfs", randomLongBetween(16, 1L << 20), new BytesRef(randomByteArrayOfLength(16)));
            if (randomBoolean()) {
                backedUp.add(backedUpFile(file.name(), file.length(), file.writerUuid()));
            }
            if (randomBoolean()) {
                // The same name and length from an earlier copy of the index, with another writer uuid.
                backedUp.add(backedUpFile(file.name(), file.length(), new BytesRef(randomByteArrayOfLength(16))));
            }
            if (randomBoolean()) {
                currentFiles.add(file);
            }
        }
        var snapshots = BlobStoreIndexShardSnapshots.EMPTY;
        final List<SnapshotId> snapshotIds = new ArrayList<>();
        for (int i = between(0, 10); i > 0; i--) {
            final String name = "snap-" + i;
            if (snapshotIds.isEmpty() == false && rarely()) {
                snapshots = snapshots.withClone(randomFrom(snapshots.snapshots()).snapshot(), name);
            } else {
                snapshots = snapshots.withAddedSnapshot(
                    new SnapshotFiles(name, randomSubsetOf(backedUp), randomBoolean() ? null : "state-" + i)
                );
            }
            snapshotIds.add(new SnapshotId(name, randomUUID()));
        }
        if (snapshotIds.isEmpty() == false && randomBoolean()) {
            snapshots = snapshots.withRetainedSnapshots(Set.copyOf(randomSubsetOf(snapshotIds)));
        }
        final byte[] blob = serialize(snapshots, randomBoolean());

        final ShardBaseline streamed = ShardBaselineFormat.forFiles(currentFiles)
            .deserialize(REPOSITORY, NamedXContentRegistry.EMPTY, new ByteArrayInputStream(blob));

        final var wholeMetadata = INDEX_SHARD_SNAPSHOTS_FORMAT.deserialize(
            REPOSITORY,
            NamedXContentRegistry.EMPTY,
            new ByteArrayInputStream(blob)
        );
        assertThat(streamed, equalTo(ShardBaseline.of(wholeMetadata, currentFiles)));
    }

    /**
     * Elasticsearch never writes a snapshot entry without its list of files, but {@code BlobStoreIndexShardSnapshots#fromXContent}
     * leaves such an entry out, state id and all, and reading straight into a baseline must do the same.
     */
    public void testASnapshotThatListsNoFilesIsLeftOut() throws IOException {
        final var cfs = new LuceneFile("_0.cfs", 1000, new BytesRef(randomByteArrayOfLength(16)));
        final var backedUp = backedUpFile(cfs.name(), cfs.length(), cfs.writerUuid());
        // Written by hand: snap-1 holds _0.cfs with state-1, and snap-2 lists no files but has state-2.
        final ToXContent metadata = (builder, params) -> {
            builder.startArray("files");
            FileInfo.toXContent(backedUp, builder, params);
            builder.endArray();
            builder.startObject("snapshots");
            builder.startObject("snap-1").array("files", backedUp.name()).field("shard_state_id", "state-1").endObject();
            builder.startObject("snap-2").field("shard_state_id", "state-2").endObject();
            builder.endObject();
            return builder;
        };
        final var writer = new ChecksumBlobStoreFormat<ToXContent>("snapshots", SNAPSHOT_INDEX_NAME_FORMAT, (repository, parser) -> {
            throw new AssertionError("only written");
        }, Function.identity());
        final byte[] blob;
        try (var out = new BytesStreamOutput()) {
            writer.serialize(metadata, "index-" + randomUUID(), randomBoolean(), out);
            blob = BytesReference.toBytes(out.bytes());
        }

        final var wholeMetadata = INDEX_SHARD_SNAPSHOTS_FORMAT.deserialize(
            REPOSITORY,
            NamedXContentRegistry.EMPTY,
            new ByteArrayInputStream(blob)
        );
        assertThat(wholeMetadata.snapshots().size(), equalTo(1));
        final ShardBaseline streamed = ShardBaselineFormat.forFiles(List.of(cfs))
            .deserialize(REPOSITORY, NamedXContentRegistry.EMPTY, new ByteArrayInputStream(blob));
        assertThat(streamed, equalTo(ShardBaseline.of(wholeMetadata, List.of(cfs))));
        // The latest state id is state-1, so a commit at state-2 compares files.
        assertThat(streamed.bytesToUpload("state-2", List.of(cfs)), equalTo(0L));
        assertThat(
            streamed.bytesToUpload("state-1", List.of(new LuceneFile("_1.cfs", 500, new BytesRef(randomByteArrayOfLength(16))))),
            equalTo(0L)
        );
    }

    private static FileInfo backedUpFile(String name, long length, BytesRef writerUuid) {
        final var metadata = new StoreFileMetadata(
            name,
            length,
            Store.digestToString(randomLongBetween(0, 0xFFFFFFFFL)),
            Version.LATEST.toString(),
            null,
            writerUuid
        );
        return new FileInfo("__" + randomUUID(), metadata, null);
    }

    private static byte[] serialize(BlobStoreIndexShardSnapshots snapshots, boolean withWriterUuids) throws IOException {
        try (var out = new BytesStreamOutput()) {
            INDEX_SHARD_SNAPSHOTS_FORMAT.serialize(
                snapshots,
                "index-" + randomUUID(),
                randomBoolean(),
                Map.of(FileInfo.SERIALIZE_WRITER_UUID, Boolean.toString(withWriterUuids)),
                out
            );
            return BytesReference.toBytes(out.bytes());
        }
    }
}
