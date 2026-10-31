/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.snapshots.blobstore;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Version;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.repositories.ProjectRepo;
import org.elasticsearch.repositories.blobstore.ChecksumBlobStoreFormat;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.repositories.blobstore.BlobStoreRepository.INDEX_SHARD_SNAPSHOTS_FORMAT;
import static org.elasticsearch.repositories.blobstore.BlobStoreRepository.SNAPSHOT_INDEX_NAME_FORMAT;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

public class BlobStoreIndexShardSnapshotsTests extends ESTestCase {

    private static final ProjectRepo REPOSITORY = new ProjectRepo(ProjectId.DEFAULT, "repo");

    /**
     * {@link BlobStoreIndexShardSnapshots#parse} hands over every blob once and every snapshot in order, as {@code fromXContent},
     * which is built on it, reads them back.
     */
    public void testParseHandsOverEveryEntryInOrder() throws IOException {
        final var snapshots = randomShardSnapshots(between(0, 50), between(0, 20));
        final byte[] blob = serialize(snapshots);

        final Map<String, FileInfo> files = new HashMap<>();
        final List<SnapshotFiles> parsed = new ArrayList<>();
        final var format = new ChecksumBlobStoreFormat<>("snapshots", SNAPSHOT_INDEX_NAME_FORMAT, (repository, parser) -> {
            BlobStoreIndexShardSnapshots.parse(parser, new BlobStoreIndexShardSnapshots.Visitor() {
                @Override
                public void file(FileInfo fileInfo) {
                    assertNull("each blob is listed once", files.put(fileInfo.name(), fileInfo));
                }

                @Override
                public void snapshot(String name, List<String> fileNames, String shardStateId) {
                    parsed.add(new SnapshotFiles(name, fileNames.stream().map(files::get).toList(), shardStateId));
                }
            });
            return Boolean.TRUE;
        }, ignored -> { throw new AssertionError("never written"); });
        format.deserialize(REPOSITORY, NamedXContentRegistry.EMPTY, stream(blob));

        assertThat(parsed.size(), equalTo(snapshots.snapshots().size()));
        final Set<String> expectedFiles = new HashSet<>();
        for (int i = 0; i < parsed.size(); i++) {
            assertSameSnapshot(parsed.get(i), snapshots.snapshots().get(i));
            snapshots.snapshots().get(i).indexFiles().forEach(file -> expectedFiles.add(file.name()));
        }
        assertThat(files.keySet(), equalTo(expectedFiles));

        final var readBack = INDEX_SHARD_SNAPSHOTS_FORMAT.deserialize(REPOSITORY, NamedXContentRegistry.EMPTY, stream(blob));
        assertThat(readBack.snapshots().size(), equalTo(snapshots.snapshots().size()));
        for (int i = 0; i < snapshots.snapshots().size(); i++) {
            assertSameSnapshot(readBack.snapshots().get(i), snapshots.snapshots().get(i));
        }
    }

    /**
     * Reading a blob with {@link BlobStoreIndexShardSnapshots#parse} hands each entry over as soon as it is read, rather than after
     * reading the whole blob, so a reader that keeps only some entries needs only as much memory as those.
     */
    public void testParseReadsTheBlobOneEntryAtATime() throws IOException {
        // One snapshot of 2,000 files, so most of the blob is the list of files, which comes first. The blob is far larger than the
        // 8 KB that the blob format reads ahead.
        final var snapshots = BlobStoreIndexShardSnapshots.EMPTY.withAddedSnapshot(new SnapshotFiles("snap", randomFiles(2_000), null));
        final byte[] blob = serialize(snapshots);
        final int[] filesSeen = new int[1];
        final var format = new ChecksumBlobStoreFormat<>("snapshots", SNAPSHOT_INDEX_NAME_FORMAT, (repository, parser) -> {
            BlobStoreIndexShardSnapshots.parse(parser, new BlobStoreIndexShardSnapshots.Visitor() {
                @Override
                public void file(FileInfo fileInfo) {
                    filesSeen[0] += 1;
                }

                @Override
                public void snapshot(String name, List<String> fileNames, String shardStateId) {}
            });
            return Boolean.TRUE;
        }, ignored -> { throw new AssertionError("never written"); });

        // The blob can't be read past its middle. Files listed before that point are handed over before the read fails.
        final int readable = blob.length / 2;
        expectThrows(
            IOException.class,
            () -> format.deserialize(REPOSITORY, NamedXContentRegistry.EMPTY, new FilterInputStream(stream(blob)) {
                private int read;

                @Override
                public int read() throws IOException {
                    final byte[] one = new byte[1];
                    return read(one, 0, 1) == -1 ? -1 : one[0] & 0xFF;
                }

                // Gives the bytes up to the middle of the blob, as many as are asked for, and then fails.
                @Override
                public int read(byte[] b, int off, int len) throws IOException {
                    if (read >= readable) {
                        throw new IOException("simulated");
                    }
                    final int n = super.read(b, off, Math.min(len, readable - read));
                    read += Math.max(n, 0);
                    return n;
                }
            })
        );
        assertThat(filesSeen[0], greaterThan(0));
        assertThat(filesSeen[0], lessThan(2_000));
    }

    private static void assertSameSnapshot(SnapshotFiles actual, SnapshotFiles expected) {
        assertThat(actual.snapshot(), equalTo(expected.snapshot()));
        assertThat(actual.shardStateIdentifier(), equalTo(expected.shardStateIdentifier()));
        assertThat(
            actual.indexFiles().stream().map(FileInfo::name).toList(),
            equalTo(expected.indexFiles().stream().map(FileInfo::name).toList())
        );
    }

    /**
     * Shard-level metadata with {@code snapshotCount} snapshots of random subsets of {@code fileCount} files, some of them clones of
     * earlier ones, with or without a state id, sometimes after retention removed some of them.
     */
    private static BlobStoreIndexShardSnapshots randomShardSnapshots(int fileCount, int snapshotCount) {
        final List<FileInfo> files = randomFiles(fileCount);
        var snapshots = BlobStoreIndexShardSnapshots.EMPTY;
        final List<SnapshotId> snapshotIds = new ArrayList<>();
        for (int i = 0; i < snapshotCount; i++) {
            final String name = "snap-" + i;
            if (i > 0 && rarely()) {
                snapshots = snapshots.withClone(randomFrom(snapshots.snapshots()).snapshot(), name);
            } else {
                snapshots = snapshots.withAddedSnapshot(
                    new SnapshotFiles(name, randomSubsetOf(files), randomBoolean() ? null : randomAlphaOfLength(30))
                );
            }
            snapshotIds.add(new SnapshotId(name, randomUUID()));
        }
        if (snapshotIds.isEmpty() == false && randomBoolean()) {
            snapshots = snapshots.withRetainedSnapshots(Set.copyOf(randomSubsetOf(snapshotIds)));
        }
        return snapshots;
    }

    private static List<FileInfo> randomFiles(int count) {
        final List<FileInfo> files = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            final var metadata = new StoreFileMetadata(
                "_" + Integer.toString(i, Character.MAX_RADIX) + ".cfs",
                randomLongBetween(16, 1L << 30),
                Store.digestToString(randomLongBetween(0, 0xFFFFFFFFL)),
                Version.LATEST.toString(),
                null,
                new BytesRef(randomByteArrayOfLength(16))
            );
            files.add(new FileInfo("__" + randomUUID(), metadata, null));
        }
        return files;
    }

    private static byte[] serialize(BlobStoreIndexShardSnapshots snapshots) throws IOException {
        try (var out = new BytesStreamOutput()) {
            INDEX_SHARD_SNAPSHOTS_FORMAT.serialize(snapshots, "index-" + randomUUID(), randomBoolean(), out);
            return BytesReference.toBytes(out.bytes());
        }
    }

    private static InputStream stream(byte[] blob) {
        return new ByteArrayInputStream(blob);
    }
}
