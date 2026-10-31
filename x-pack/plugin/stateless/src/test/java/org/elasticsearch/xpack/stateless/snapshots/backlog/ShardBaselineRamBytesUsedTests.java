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
import org.elasticsearch.xpack.stateless.snapshots.backlog.ShardBaseline.LuceneFile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.allOf;
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
        return Set.of("backedUpWriterUuids", "latestStateId");
    }

    @Override
    protected Accountable createRandomTestInstance() {
        final List<FileInfo> current = randomBackedUpFiles(0, between(0, 50));
        final List<FileInfo> backedUp = new ArrayList<>(current);
        // Add files with the same names and lengths as some of the current ones, as from an index created again under the same name.
        for (FileInfo file : randomSubsetOf(current)) {
            backedUp.add(withWriterUuid(file, randomBoolean() ? randomWriterUuid() : StoreFileMetadata.UNAVAILABLE_WRITER_UUID));
        }
        var snapshots = BlobStoreIndexShardSnapshots.EMPTY;
        for (int i = between(0, 5); i > 0; i--) {
            snapshots = snapshots.withAddedSnapshot(
                new SnapshotFiles("snap-" + i, randomSubsetOf(backedUp), randomBoolean() ? null : randomStateId())
            );
        }
        return ShardBaseline.of(readBack(snapshots, randomBoolean()), luceneFiles(randomSubsetOf(current)));
    }

    public void testFilesOfSeveralSnapshotsAreKeptOnce() {
        // The baseline's memory must not grow with the number of snapshots kept. Here the same files are listed in every snapshot,
        // and the baseline must keep each file once.
        final List<FileInfo> files = randomBackedUpFiles(0, between(1, 50));
        final var stateId = randomStateId();
        final var oneSnapshot = BlobStoreIndexShardSnapshots.EMPTY.withAddedSnapshot(new SnapshotFiles("snap-0", files, stateId));
        var manySnapshots = oneSnapshot;
        for (int i = between(1, 400); i > 0; i--) {
            // An idle shard: every snapshot has the same files and the same state id.
            manySnapshots = manySnapshots.withAddedSnapshot(new SnapshotFiles("snap-" + i, files, stateId));
        }

        assertThat(
            ShardBaseline.of(manySnapshots, luceneFiles(files)).ramBytesUsed(),
            equalTo(ShardBaseline.of(oneSnapshot, luceneFiles(files)).ramBytesUsed())
        );
    }

    public void testFilesOnlyOlderSnapshotsHaveCostNothing() {
        // Only the older snapshot has the files in mergedAway: the shard has since merged them away, so it no longer has files with
        // those names. The baseline must drop them.
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
        // The latest snapshot was taken after a force merge, so its state id has a force merge id and is longer than the others.
        // If the baseline kept any other state id, it would cost less, and the test would fail.
        final var latest = new SnapshotFiles("snap-latest", files, stateId(true));
        var manySnapshots = BlobStoreIndexShardSnapshots.EMPTY;
        for (int i = between(1, 400); i > 0; i--) {
            // The shard is written to between snapshots, so each snapshot has a new state id. The files are kept the same on
            // purpose, so that only the state ids differ.
            manySnapshots = manySnapshots.withAddedSnapshot(new SnapshotFiles("snap-" + i, files, stateId(false)));
        }
        manySnapshots = manySnapshots.withAddedSnapshot(latest);

        assertThat(
            ShardBaseline.of(manySnapshots, luceneFiles(files)).ramBytesUsed(),
            equalTo(ShardBaseline.of(BlobStoreIndexShardSnapshots.EMPTY.withAddedSnapshot(latest), luceneFiles(files)).ramBytesUsed())
        );
    }

    public void testFilesOfAnEarlierCopyOfTheIndexCostNothing() {
        final List<FileInfo> current = randomBackedUpFiles(0, between(1, 50));
        // Files with the same names and lengths, from an earlier copy of the index that was deleted and created again under the same
        // name. They have other writer uuids, or none if a version before 7.16 backed them up, so they can never match, and the
        // baseline must drop them.
        final List<FileInfo> earlier = new ArrayList<>();
        for (FileInfo file : current) {
            earlier.add(withWriterUuid(file, randomBoolean() ? randomWriterUuid() : StoreFileMetadata.UNAVAILABLE_WRITER_UUID));
        }
        final var earlierAndCurrent = BlobStoreIndexShardSnapshots.EMPTY.withAddedSnapshot(new SnapshotFiles("snap-0", earlier, null))
            .withAddedSnapshot(new SnapshotFiles("snap-1", current, null));
        final var onlyCurrent = BlobStoreIndexShardSnapshots.EMPTY.withAddedSnapshot(new SnapshotFiles("snap-1", current, null));

        assertThat(
            ShardBaseline.of(earlierAndCurrent, luceneFiles(current)).ramBytesUsed(),
            equalTo(ShardBaseline.of(onlyCurrent, luceneFiles(current)).ramBytesUsed())
        );
    }

    public void testFilesWithoutAWriterUuidCostNothing() {
        // A file without a writer uuid is always counted, so the baseline keeps nothing for it, even when the repository has a file
        // with the same name and length and no writer uuid either.
        final List<FileInfo> files = new ArrayList<>();
        for (FileInfo file : randomBackedUpFiles(0, between(1, 50))) {
            files.add(withWriterUuid(file, StoreFileMetadata.UNAVAILABLE_WRITER_UUID));
        }
        final var snapshots = BlobStoreIndexShardSnapshots.EMPTY.withAddedSnapshot(new SnapshotFiles("snap", files, null));

        assertThat(ShardBaseline.of(snapshots, luceneFiles(files)).ramBytesUsed(), equalTo(ShardBaseline.EMPTY.ramBytesUsed()));
    }

    public void testFilesKeptInTheBackupMetadataAreLeftOut() {
        // A snapshot keeps the contents of .si and segments_N files in the shard-level metadata, and the backlog never counts them.
        // So when the shard's only files are these, the baseline keeps nothing, and costs as much as an empty baseline.
        final List<FileInfo> files = new ArrayList<>();
        for (String name : List.of("_0.si", "_1.si", "segments_2")) {
            files.add(new FileInfo("v__" + UUIDs.randomBase64UUID(random()), metadata(name, randomWriterUuid()), null));
        }
        final var snapshots = BlobStoreIndexShardSnapshots.EMPTY.withAddedSnapshot(new SnapshotFiles("snap", files, null));

        assertThat(ShardBaseline.of(snapshots, luceneFiles(files)).ramBytesUsed(), equalTo(ShardBaseline.EMPTY.ramBytesUsed()));
    }

    /**
     * Measured with {@link RamUsageTester} on shard-level metadata read back the way a node reads it, with writer uuids, as every file
     * in a serverless repository has. Apart from its name, a file costs about 56 bytes: its 16-byte writer uuid in a {@link BytesRef},
     * and its place in the map. These figures are for compressed object pointers, which the JVM uses for heaps under 32 GB.
     */
    public void testBytesPerFile() throws IOException {
        assumeTrue("measured with compressed object pointers", RamUsageEstimator.COMPRESSED_REFS_ENABLED);
        final int fileCount = 1000;
        final List<FileInfo> files = randomBackedUpFiles(0, fileCount);
        final long nameBytes = files.stream().mapToLong(file -> RamUsageEstimator.sizeOf(file.physicalName())).sum();
        final var baseline = ShardBaseline.of(
            ShardBaselineTests.readBack(BlobStoreIndexShardSnapshots.EMPTY.withAddedSnapshot(new SnapshotFiles("snap", files, null)), true),
            luceneFiles(files)
        );

        // What the files really cost, apart from their names. The bounds keep the figure in this Javadoc, and the memory estimate
        // built on it, honest: if the cost moves outside them, for example after a JVM change, measure it again and update both.
        final long measured = RamUsageTester.ramUsed(baseline) - RamUsageTester.ramUsed(ShardBaseline.EMPTY);
        assertThat(
            "bytes per file apart from its name; if this moved, update the documented figure",
            (measured - nameBytes) / fileCount,
            allOf(greaterThanOrEqualTo(50L), lessThanOrEqualTo(70L))
        );
        // The estimate is never below the real cost, and at most a quarter above it.
        final long estimated = baseline.ramBytesUsed() - ShardBaseline.EMPTY.ramBytesUsed();
        assertThat(estimated, greaterThanOrEqualTo(measured));
        assertThat(estimated, lessThanOrEqualTo(measured + measured / 4));
    }

    /**
     * Random backed-up files, named {@code _<first>}, {@code _<first + 1>} and so on, with an extension. The names are about 6 to 45
     * characters long, since per-field formats add long suffixes. Neighbouring files often share a writer uuid, as the files of one
     * segment do.
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
        return files.stream().map(file -> new LuceneFile(file.physicalName(), file.length(), file.metadata().writerUuid())).toList();
    }

    /**
     * A backed-up file with the name and length of the given one, but another blob and the given writer uuid.
     */
    private static FileInfo withWriterUuid(FileInfo file, BytesRef writerUuid) {
        return new FileInfo("__" + UUIDs.randomBase64UUID(random()), metadata(file.physicalName(), file.length(), writerUuid), null);
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
     * Reads the shard-level metadata back the way a node reads it from the repository, written with writer uuids or without them.
     */
    private static BlobStoreIndexShardSnapshots readBack(BlobStoreIndexShardSnapshots snapshots, boolean withWriterUuids) {
        try {
            return ShardBaselineTests.readBack(snapshots, withWriterUuids);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * A random state id in the format of {@code SnapshotShardsService#getShardStateId}: history uuid, force merge uuid ("na" if there
     * is none) and max seq no.
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
