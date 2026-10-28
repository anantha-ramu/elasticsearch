/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots.backlog;

import org.apache.lucene.tests.util.RamUsageTester;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.Version;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshots;
import org.elasticsearch.index.snapshots.blobstore.SnapshotFiles;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.test.AbstractAccountableFieldsTestCase;
import org.elasticsearch.xpack.stateless.snapshots.backlog.ShardBacklog.LuceneFile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class ShardBaselineRamBytesUsedTests extends AbstractAccountableFieldsTestCase {

    @Override
    protected Class<? extends Accountable> classUnderTest() {
        return ShardBaseline.class;
    }

    @Override
    protected Set<String> fieldsAccountedForInRamBytesUsed() {
        return Set.of("backedUpFilesByName", "latestStateId");
    }

    @Override
    protected Accountable createRandomTestInstance() {
        final List<FileInfo> files = randomBackedUpFiles(0, between(0, 50));
        // some files of an index created again under the same name
        for (FileInfo file : randomSubsetOf(files)) {
            files.add(new FileInfo("__" + UUIDs.randomBase64UUID(random()), metadata(file.physicalName(), randomWriterUuid()), null));
        }
        var snapshots = BlobStoreIndexShardSnapshots.EMPTY;
        for (int i = between(0, 5); i > 0; i--) {
            snapshots = snapshots.withAddedSnapshot(
                new SnapshotFiles("snap-" + i, randomSubsetOf(files), randomBoolean() ? null : randomStateId())
            );
        }
        return ShardBaseline.of(readBack(snapshots), luceneFiles(randomSubsetOf(files)));
    }

    public void testFilesOfSeveralSnapshotsAreKeptOnce() {
        final List<FileInfo> files = randomBackedUpFiles(0, between(1, 50));
        final var stateId = randomStateId();
        final var oneSnapshot = BlobStoreIndexShardSnapshots.EMPTY.withAddedSnapshot(new SnapshotFiles("snap-0", files, stateId));
        var manySnapshots = oneSnapshot;
        for (int i = between(1, 400); i > 0; i--) {
            // an idle shard: every snapshot has the same files and state id
            manySnapshots = manySnapshots.withAddedSnapshot(new SnapshotFiles("snap-" + i, files, stateId));
        }

        assertThat(
            ShardBaseline.of(manySnapshots, luceneFiles(files)).ramBytesUsed(),
            equalTo(ShardBaseline.of(oneSnapshot, luceneFiles(files)).ramBytesUsed())
        );
    }

    public void testFilesOnlyOlderSnapshotsHaveCostNothing() {
        final List<FileInfo> mergedAway = randomBackedUpFiles(0, between(1, 50));
        final List<FileInfo> current = randomBackedUpFiles(mergedAway.size(), between(1, 50));
        final var olderAndLatest = BlobStoreIndexShardSnapshots.EMPTY.withAddedSnapshot(new SnapshotFiles("snap-0", mergedAway, null))
            .withAddedSnapshot(new SnapshotFiles("snap-1", current, null));
        final var latest = BlobStoreIndexShardSnapshots.EMPTY.withAddedSnapshot(new SnapshotFiles("snap-1", current, null));

        assertThat(
            ShardBaseline.of(olderAndLatest, luceneFiles(current)).ramBytesUsed(),
            equalTo(ShardBaseline.of(latest, luceneFiles(current)).ramBytesUsed())
        );
    }

    public void testOnlyTheLatestStateIdIsKept() {
        final List<FileInfo> files = randomBackedUpFiles(0, between(1, 50));
        // the latest after a force merge, so that its state id is longer than the others and keeping any other one costs less
        final var latest = new SnapshotFiles("snap-latest", files, stateId(true));
        var manySnapshots = BlobStoreIndexShardSnapshots.EMPTY;
        for (int i = between(1, 400); i > 0; i--) {
            // a shard written to between snapshots: a new state id each time
            manySnapshots = manySnapshots.withAddedSnapshot(new SnapshotFiles("snap-" + i, files, stateId(false)));
        }
        manySnapshots = manySnapshots.withAddedSnapshot(latest);

        assertThat(
            ShardBaseline.of(manySnapshots, luceneFiles(files)).ramBytesUsed(),
            equalTo(ShardBaseline.of(BlobStoreIndexShardSnapshots.EMPTY.withAddedSnapshot(latest), luceneFiles(files)).ramBytesUsed())
        );
    }

    public void testFilesOfTheSameNameAndAnotherLengthCostNothing() {
        final List<FileInfo> current = randomBackedUpFiles(0, between(1, 50));
        // files of an earlier copy of the index, deleted and created again under the same name
        final List<FileInfo> earlier = new ArrayList<>();
        for (FileInfo file : current) {
            earlier.add(
                new FileInfo(
                    "__" + UUIDs.randomBase64UUID(random()),
                    metadata(file.physicalName(), file.length() + between(1, 1000), randomWriterUuid()),
                    null
                )
            );
        }
        final var earlierAndCurrent = BlobStoreIndexShardSnapshots.EMPTY.withAddedSnapshot(new SnapshotFiles("snap-0", earlier, null))
            .withAddedSnapshot(new SnapshotFiles("snap-1", current, null));
        final var onlyCurrent = BlobStoreIndexShardSnapshots.EMPTY.withAddedSnapshot(new SnapshotFiles("snap-1", current, null));

        assertThat(
            ShardBaseline.of(earlierAndCurrent, luceneFiles(current)).ramBytesUsed(),
            equalTo(ShardBaseline.of(onlyCurrent, luceneFiles(current)).ramBytesUsed())
        );
    }

    public void testFilesKeptInTheShardLevelMetadataAreLeftOut() {
        final List<FileInfo> files = new ArrayList<>();
        for (String name : List.of("_0.si", "_1.si", "segments_2")) {
            files.add(new FileInfo("v__" + UUIDs.randomBase64UUID(random()), metadata(name, randomWriterUuid()), null));
        }
        final var snapshots = BlobStoreIndexShardSnapshots.EMPTY.withAddedSnapshot(new SnapshotFiles("snap", files, null));

        assertThat(ShardBaseline.of(snapshots, luceneFiles(files)).ramBytesUsed(), equalTo(ShardBaseline.EMPTY.ramBytesUsed()));
    }

    /**
     * Measured with {@link RamUsageTester} on shard-level metadata read back the way a node reads it. Apart from its name, a file costs
     * about 290 bytes: its {@link StoreFileMetadata} with the checksum, Lucene version, hash and writer uuid, and its place in the map
     * and list. That figure is for compressed object pointers, which the JVM uses for heaps under 32 GB. The estimate is a little
     * higher, because it also counts the map's table and the empty hash that files share.
     */
    public void testBytesPerFile() {
        assumeTrue("measured with compressed object pointers", RamUsageEstimator.COMPRESSED_REFS_ENABLED);
        final int fileCount = 1000;
        final List<FileInfo> files = randomBackedUpFiles(0, fileCount);
        final long nameBytes = files.stream().mapToLong(file -> RamUsageEstimator.sizeOf(file.physicalName())).sum();
        final var baseline = ShardBaseline.of(
            readBack(BlobStoreIndexShardSnapshots.EMPTY.withAddedSnapshot(new SnapshotFiles("snap", files, null))),
            luceneFiles(files)
        );

        final long measured = RamUsageTester.ramUsed(baseline) - RamUsageTester.ramUsed(ShardBaseline.EMPTY);
        assertThat((measured - nameBytes) / fileCount, lessThanOrEqualTo(300L));
        final long estimated = baseline.ramBytesUsed() - ShardBaseline.EMPTY.ramBytesUsed();
        assertThat(estimated, greaterThanOrEqualTo(measured));
        assertThat(estimated, lessThanOrEqualTo(measured + measured / 4));
    }

    /**
     * Files as a shard has them, named from {@code _<first>} on: names of about 6 to 45 characters, the longer ones of per-field
     * formats, and the files of a segment sharing its writer uuid.
     */
    private static List<FileInfo> randomBackedUpFiles(int first, int count) {
        final List<FileInfo> files = new ArrayList<>(count);
        BytesRef writerUuid = null;
        for (int i = first; i < first + count; i++) {
            if (writerUuid == null || randomBoolean()) {
                writerUuid = randomWriterUuid();
            }
            final var name = "_"
                + Integer.toString(i, Character.MAX_RADIX)
                + randomFrom(".cfs", "_Lucene912_0.doc", "_ES87TSDB_0.dvd", "_ES818HnswBinaryQuantizedVectorsFormat_0.vec");
            files.add(new FileInfo("__" + UUIDs.randomBase64UUID(random()), metadata(name, writerUuid), null));
        }
        return files;
    }

    private static List<LuceneFile> luceneFiles(List<FileInfo> files) {
        return files.stream().map(file -> new LuceneFile(file.physicalName(), file.length())).toList();
    }

    private static BytesRef randomWriterUuid() {
        return new BytesRef(randomByteArrayOfLength(16));
    }

    private static StoreFileMetadata metadata(String name, BytesRef writerUuid) {
        return metadata(name, randomLongBetween(16, 5L << 30), writerUuid);
    }

    private static StoreFileMetadata metadata(String name, long length, BytesRef writerUuid) {
        return new StoreFileMetadata(
            name,
            length,
            Store.digestToString(randomLongBetween(0, 0xFFFFFFFFL)),
            Version.LATEST.toString(),
            null,
            writerUuid
        );
    }

    /**
     * As a node reads the shard-level metadata from the repository.
     */
    private static BlobStoreIndexShardSnapshots readBack(BlobStoreIndexShardSnapshots snapshots) {
        try {
            return ShardBacklogTests.readBack(snapshots);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * As {@code SnapshotShardsService#getShardStateId}: history uuid, force merge uuid ("na" if none) and max seq no.
     */
    private static String randomStateId() {
        return stateId(randomBoolean());
    }

    private static String stateId(boolean forceMerged) {
        return UUIDs.randomBase64UUID(random())
            + "-"
            + (forceMerged ? UUIDs.randomBase64UUID(random()) : "na")
            + "-"
            + randomNonNegativeLong();
    }
}
