/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots.backlog;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Version;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshots;
import org.elasticsearch.index.snapshots.blobstore.SnapshotFiles;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.test.AbstractAccountableFieldsTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;

public class ShardBaselineRamBytesUsedTests extends AbstractAccountableFieldsTestCase {

    @Override
    protected Class<? extends Accountable> classUnderTest() {
        return ShardBaseline.class;
    }

    @Override
    protected Set<String> fieldsAccountedForInRamBytesUsed() {
        return Set.of("names", "fileStarts", "lengths", "checksums", "writerUuids", "stateIds");
    }

    @Override
    protected Accountable createRandomTestInstance() {
        final List<FileInfo> files = randomFiles(between(0, 50));
        var snapshots = BlobStoreIndexShardSnapshots.EMPTY;
        for (int i = between(0, 5); i > 0; i--) {
            snapshots = snapshots.withAddedSnapshot(
                new SnapshotFiles("snap-" + i, randomSubsetOf(files), randomBoolean() ? null : randomStateId())
            );
        }
        return ShardBaseline.of(snapshots);
    }

    public void testFilesOfSeveralSnapshotsAreKeptOnce() {
        final List<FileInfo> files = randomFiles(between(1, 50));
        final var stateId = randomStateId();
        final var oneSnapshot = BlobStoreIndexShardSnapshots.EMPTY.withAddedSnapshot(new SnapshotFiles("snap-0", files, stateId));
        var manySnapshots = oneSnapshot;
        for (int i = between(1, 400); i > 0; i--) {
            // an idle shard: every snapshot has the same files and state id
            manySnapshots = manySnapshots.withAddedSnapshot(new SnapshotFiles("snap-" + i, files, stateId));
        }

        assertThat(ShardBaseline.of(manySnapshots).ramBytesUsed(), equalTo(ShardBaseline.of(oneSnapshot).ramBytesUsed()));
    }

    /**
     * Apart from its name, a file costs its length and checksum (8 bytes each), its writer uuid (16 bytes) and three array offsets (4
     * bytes each), 44 bytes; a state id costs its characters and an offset.
     */
    public void testBytesPerFileAndStateId() {
        final int fileCount = 1000;
        final List<FileInfo> fileInfos = randomFiles(fileCount);
        final long nameBytes = fileInfos.stream().mapToLong(file -> file.physicalName().length()).sum();
        final var files = BlobStoreIndexShardSnapshots.EMPTY.withAddedSnapshot(new SnapshotFiles("snap", fileInfos, null));
        final long empty = ShardBaseline.EMPTY.ramBytesUsed();
        assertThat((ShardBaseline.of(files).ramBytesUsed() - empty - nameBytes) / fileCount, lessThan(50L));

        final int stateIdCount = 400;
        long stateIdBytes = 0;
        var stateIds = BlobStoreIndexShardSnapshots.EMPTY;
        for (int i = 0; i < stateIdCount; i++) {
            final var stateId = randomStateId();
            stateIdBytes += stateId.length();
            stateIds = stateIds.withAddedSnapshot(new SnapshotFiles("snap-" + i, List.of(), stateId));
        }
        assertThat((ShardBaseline.of(stateIds).ramBytesUsed() - empty - stateIdBytes) / stateIdCount, lessThan(8L));
    }

    /**
     * Files as a shard has them: names of about 6 to 45 characters, the longer ones of per-field formats, and the files of a
     * segment sharing its writer uuid.
     */
    private static List<FileInfo> randomFiles(int count) {
        final List<FileInfo> files = new ArrayList<>(count);
        BytesRef writerUuid = null;
        for (int i = 0; i < count; i++) {
            if (writerUuid == null || randomBoolean()) {
                writerUuid = new BytesRef(randomByteArrayOfLength(16));
            }
            final var name = "_"
                + Integer.toString(i, Character.MAX_RADIX)
                + randomFrom(".cfs", "_Lucene912_0.doc", "_ES87TSDB_0.dvd", "_ES818HnswBinaryQuantizedVectorsFormat_0.vec");
            files.add(
                new FileInfo(
                    "__" + UUIDs.randomBase64UUID(random()),
                    new StoreFileMetadata(
                        name,
                        randomLongBetween(16, 5L << 30),
                        Store.digestToString(randomLongBetween(0, 0xFFFFFFFFL)),
                        Version.LATEST.toString(),
                        null,
                        writerUuid
                    ),
                    null
                )
            );
        }
        return files;
    }

    /**
     * As {@code SnapshotShardsService#getShardStateId}: history uuid, force merge uuid ("na" if none) and max seq no.
     */
    private static String randomStateId() {
        return UUIDs.randomBase64UUID(random())
            + "-"
            + (randomBoolean() ? "na" : UUIDs.randomBase64UUID(random()))
            + "-"
            + randomNonNegativeLong();
    }
}
