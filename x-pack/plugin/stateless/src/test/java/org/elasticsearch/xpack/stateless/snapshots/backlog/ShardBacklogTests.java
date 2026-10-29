/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots.backlog;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Version;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.lucene.store.ByteArrayIndexInput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshots;
import org.elasticsearch.index.snapshots.blobstore.SnapshotFiles;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.repositories.ProjectRepo;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.stateless.snapshots.backlog.ShardBaseline.LuceneFile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.repositories.blobstore.BlobStoreRepository.INDEX_SHARD_SNAPSHOTS_FORMAT;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class ShardBacklogTests extends ESTestCase {

    private static final BytesRef NO_WRITER_UUID = StoreFileMetadata.UNAVAILABLE_WRITER_UUID;

    public void testFilesTheRepositoryHoldsAreNotCounted() {
        final var cfs = file("_0.cfs", 1000, 11, randomWriterUuid());
        final var cfe = file("_0.cfe", 100, 12, randomWriterUuid());
        final var repository = repository(snapshot("snap-1", null, cfs, cfe));

        assertBacklog(repository, commit(null, cfs, cfe), 0);
    }

    public void testFilesMissingFromTheRepositoryAreCounted() {
        final var cfs = file("_0.cfs", 1000, 11, randomWriterUuid());
        final var repository = repository(snapshot("snap-1", null, cfs));

        assertBacklog(repository, commit(null, cfs, file("_1.cfs", 2000, 13, randomWriterUuid())), 2000);
        assertBacklog(BlobStoreIndexShardSnapshots.EMPTY, commit(null, cfs, file("_1.cfs", 2000, 13, randomWriterUuid())), 3000);
    }

    public void testTheLatestStateIdMeansNothingToUpload() {
        final var repository = repository(
            snapshot("snap-1", "state-1", file("_0.cfs", 1000, 11, randomWriterUuid())),
            snapshot("snap-2", "state-2", file("_1.cfs", 2000, 12, randomWriterUuid()))
        );
        // merged since the latest snapshot without any new writes: other files, the same state
        final var merged = commit("state-2", file("_2.cfs", 3000, 13, randomWriterUuid()));

        assertBacklog(repository, merged, 0);
        assertThat(ShardBacklog.filesNeedingChecksum(baseline(repository, merged), "state-2", luceneFiles(merged)), empty());
    }

    public void testTheLatestSnapshotWithAStateIdCounts() {
        // snap-0 was taken while writes were still in flight, so it has no state id.
        // snap-1 was taken after the writes finished and the shard had settled.
        // A clone copies the state id of its source, so clone-of-snap-0, the newest snapshot, has no state id either.
        final var repository = repository(
            snapshot("snap-0", null, file("_0.cfs", 1000, 11, randomWriterUuid())),
            snapshot("snap-1", "state-1", file("_1.cfs", 2000, 12, randomWriterUuid()))
        ).withClone("snap-0", "clone-of-snap-0");

        // There were no writes since snap-1, but a merge rewrote _1.cfs into _2.cfs, so no file name matches.
        // Only the state id of snap-1, the latest snapshot that has one, can show that nothing needs uploading.
        assertBacklog(repository, commit("state-1", file("_2.cfs", 2000, 13, randomWriterUuid())), 0);
    }

    public void testACloneOfAnOlderSnapshotCountsUntilTheNextSnapshot() {
        for (boolean originalDeleted : List.of(false, true)) {
            final var repository = repositoryWithACloneOfAnOlderSnapshot(originalDeleted);
            // merged since snap-2 without new writes: other files, the same state
            final var merged = commit("state-2", file("_2.cfs", 3000, 13, randomWriterUuid()));

            assertThat(uploadedBytes(repository, merged), equalTo(0L));
            // the clone put state-1 last, so the backlog counts the merged file
            assertThat(ShardBacklog.bytesToUpload(baseline(repository, merged), "state-2", luceneFiles(merged), Map.of()), equalTo(3000L));
            // the next snapshot, taken once the shard has settled, takes the shortcut and puts state-2 last again
            assertBacklog(snapshotShard(repository, "snap-3", merged), merged, 0);
        }
    }

    public void testACloneOfAnOlderSnapshotIsAlsoFixedByASnapshotTakenMidWrite() {
        for (boolean originalDeleted : List.of(false, true)) {
            final var repository = repositoryWithACloneOfAnOlderSnapshot(originalDeleted);
            final var merged = commit("state-2", file("_2.cfs", 3000, 13, randomWriterUuid()));
            // the next snapshot is taken while writes are in flight: it records no state id, and uploads the merged file
            final var afterNextSnapshot = snapshotShard(repository, "snap-3", new Commit(null, merged.files()));

            // the clone's state-1 is still the last state id, but the merged file is now backed up, so comparing files is right
            assertBacklog(afterNextSnapshot, merged, 0);
        }
    }

    /**
     * snap-1 (state-1) and snap-2 (state-2), then a clone of snap-1, which puts state-1 last. If {@code originalDeleted}, snap-1 is
     * then deleted, as retention does, and keeps only the clone.
     */
    private static BlobStoreIndexShardSnapshots repositoryWithACloneOfAnOlderSnapshot(boolean originalDeleted) {
        final var cfs0 = file("_0.cfs", 1000, 11, randomWriterUuid());
        final var cfs1 = file("_1.cfs", 2000, 12, randomWriterUuid());
        final var repository = repository(snapshot("snap-1", "state-1", cfs0), snapshot("snap-2", "state-2", cfs0, cfs1)).withClone(
            "snap-1",
            "clone-1"
        );
        if (originalDeleted) {
            return repository.withRetainedSnapshots(
                Set.of(new SnapshotId("snap-2", randomUUID()), new SnapshotId("clone-1", randomUUID()))
            );
        }
        return repository;
    }

    public void testADeletedSnapshotNoLongerCounts() {
        final var cfs0 = file("_0.cfs", 1000, 11, randomWriterUuid());
        final var cfs1 = file("_1.cfs", 2000, 12, randomWriterUuid());
        // snap-2, the latest snapshot and the only one with _1.cfs, is deleted
        final var repository = repository(snapshot("snap-1", "state-1", cfs0), snapshot("snap-2", "state-2", cfs0, cfs1))
            .withRetainedSnapshots(Set.of(new SnapshotId("snap-1", randomUUID())));

        // the shard hasn't changed since snap-2, but its state id is gone with it, and so is the only backed-up copy of _1.cfs
        assertBacklog(repository, commit("state-2", cfs0, cfs1), 2000);
    }

    public void testRepairedMetadataWithoutStateIdsComparesTheFiles() {
        final var cfs0 = file("_0.cfs", 1000, 11, randomWriterUuid());
        // metadata rebuilt from the per-snapshot metadata files has no state ids at all
        final var repository = repository(snapshot("snap-1", null, cfs0), snapshot("snap-2", null, cfs0));

        // the shard has a state id but no snapshot does, so even a merge with no new writes counts the merged file
        assertBacklog(repository, commit("state-2", file("_1.cfs", 3000, 13, randomWriterUuid())), 3000);
        assertBacklog(repository, commit("state-2", cfs0), 0);
    }

    public void testABaselineBuiltBeforeADeletionCanCountTooLittle() {
        final var cfs0 = file("_0.cfs", 1000, 11, randomWriterUuid());
        final var cfs1 = file("_1.cfs", 2000, 12, randomWriterUuid());
        final var beforeDeletion = repository(snapshot("snap-1", "state-1", cfs0), snapshot("snap-2", "state-2", cfs0, cfs1));
        final var shard = commit("state-3", cfs0, cfs1, file("_2.cfs", 500, 13, randomWriterUuid()));
        final var outdated = baseline(beforeDeletion, shard);
        // snap-2, the only snapshot with _1.cfs, is deleted after the baseline was built
        final var afterDeletion = beforeDeletion.withRetainedSnapshots(Set.of(new SnapshotId("snap-1", randomUUID())));

        assertThat(uploadedBytes(afterDeletion, shard), equalTo(2500L));
        // this is why the caller must not use a baseline built from metadata that has changed since
        final var metadata = Map.of("_0.cfs", cfs0, "_1.cfs", cfs1);
        assertThat(ShardBacklog.bytesToUpload(outdated, "state-3", luceneFiles(shard), metadata), equalTo(500L));
        assertBacklog(afterDeletion, shard, 2500);
    }

    public void testARestoredSnapshotMatchesByItsFiles() {
        final var cfs0 = file("_0.cfs", 1000, 11, randomWriterUuid());
        final var repository = repository(
            snapshot("snap-1", "history-1-na-5", cfs0),
            snapshot("snap-2", "history-1-na-9", file("_1.cfs", 2000, 12, randomWriterUuid()))
        );
        // snap-1 restored: its files again, under a new history
        final var restored = commit("history-2-na-5", cfs0);

        assertBacklog(repository, restored, 0);
        // a new file after the restore gets the name and length of a file of snap-2, but it is another file
        assertBacklog(repository, restored, commit("history-2-na-7", cfs0, file("_1.cfs", 2000, 12, randomWriterUuid())), 2000);
    }

    public void testAnotherStateIdComparesTheFiles() {
        final var cfs = file("_0.cfs", 1000, 11, randomWriterUuid());
        final var repository = repository(snapshot("snap-1", "state-1", cfs));

        assertBacklog(repository, commit("state-2", cfs, file("_1.cfs", 2000, 12, randomWriterUuid())), 2000);
    }

    public void testACommitWithoutStateIdComparesTheFiles() {
        final var cfs = file("_0.cfs", 1000, 11, randomWriterUuid());
        // a snapshot taken while the shard was not in a steady state has no state id either
        final var repository = repository(snapshot("snap-1", null, cfs));

        assertBacklog(repository, commit(null, cfs, file("_1.cfs", 2000, 12, randomWriterUuid())), 2000);
    }

    public void testARecreatedIndexWithTheSameFileNamesIsCounted() {
        final var repository = repository(
            snapshot("snap-1", "state-1", file("_0.cfs", 1000, 11, randomWriterUuid()), file("_0.cfe", 100, 12, randomWriterUuid()))
        );
        // the new index starts again from _0, with files of the same names and maybe the same lengths, but other contents
        final var recreated = commit("state-2", file("_0.cfs", 1000, 21, randomWriterUuid()), file("_0.cfe", 100, 22, NO_WRITER_UUID));

        assertBacklog(repository, recreated, 1100);
    }

    public void testTheSameChecksumWithAnotherWriterUuidIsCounted() {
        final var repository = repository(snapshot("snap-1", null, file("_0.cfs", 1000, 11, randomWriterUuid())));

        assertBacklog(repository, commit(null, file("_0.cfs", 1000, 11, randomWriterUuid())), 1000);
    }

    public void testTheWriterUuidIsIgnoredWhenEitherSideHasNone() {
        final var repository = repository(
            snapshot("snap-1", null, file("_0.cfs", 1000, 11, randomWriterUuid()), file("_0.cfe", 100, 12, NO_WRITER_UUID))
        );

        assertBacklog(repository, commit(null, file("_0.cfs", 1000, 11, NO_WRITER_UUID)), 0);
        assertBacklog(repository, commit(null, file("_0.cfe", 100, 12, randomFrom(randomWriterUuid(), NO_WRITER_UUID))), 0);
    }

    public void testFilesKeptInTheBackupMetadataAreNotCounted() {
        final var cfs = file("_1.cfs", 1000, 11, randomWriterUuid());
        final var repository = repository(snapshot("snap-1", null, inlinedFile("_0.si"), inlinedFile("segments_2")));

        // new and changed .si and segments_N files are written into the shard-level metadata, never uploaded
        assertBacklog(repository, commit(null, inlinedFile("_0.si"), inlinedFile("_1.si"), inlinedFile("segments_3"), cfs), 1000);
    }

    public void testAFileOfAnyRetainedSnapshotMatches() {
        final var older = file("_0.cfs", 1000, 11, randomWriterUuid());
        final var newer = file("_0.cfs", 1000, 12, randomWriterUuid());
        // the index was deleted and created again between the two snapshots, so the repository has two files named _0.cfs
        final var repository = repository(snapshot("snap-1", null, older), snapshot("snap-2", null, newer));

        assertBacklog(repository, commit(null, older), 0);
        assertBacklog(repository, commit(null, newer), 0);
    }

    public void testChecksumsAreOnlyNeededForFilesOfTheSameNameAndLength() {
        final var repository = repository(
            snapshot(
                "snap-1",
                null,
                file("_0.cfs", 1000, 11, randomWriterUuid()),
                file("_0.cfe", 100, 12, randomWriterUuid()),
                inlinedFile("_0.si")
            )
        );
        // the checksum is needed whatever the writer uuids, since isSame decides
        final var sameNameAndLength = file("_0.cfs", 1000, 21, randomFrom(randomWriterUuid(), NO_WRITER_UUID));
        final var commit = commit(
            null,
            sameNameAndLength,
            file("_0.cfe", 200, 12, NO_WRITER_UUID),
            file("_1.cfs", 1000, 11, NO_WRITER_UUID),
            inlinedFile("_0.si")
        );

        final var needingChecksum = ShardBacklog.filesNeedingChecksum(baseline(repository, commit), null, luceneFiles(commit));

        assertThat(needingChecksum, equalTo(List.of(luceneFile(sameNameAndLength))));
        assertBacklog(repository, commit, 2200);
    }

    public void testMissingOrWrongMetadataIsRejected() {
        final var cfs = file("_0.cfs", 1000, 11, randomWriterUuid());
        final var repository = repository(snapshot("snap-1", null, cfs));
        final var commit = commit(null, cfs);

        final var missing = expectThrows(
            IllegalArgumentException.class,
            () -> ShardBacklog.bytesToUpload(baseline(repository, commit), null, luceneFiles(commit), Map.of())
        );
        assertThat(missing.getMessage(), containsString("[_0.cfs]"));

        final var otherFile = randomFrom(file("_0.cfs", 2000, 11, randomWriterUuid()), file("_1.cfs", 1000, 11, randomWriterUuid()));
        final var wrong = expectThrows(
            IllegalArgumentException.class,
            () -> ShardBacklog.bytesToUpload(baseline(repository, commit), null, luceneFiles(commit), Map.of("_0.cfs", otherFile))
        );
        assertThat(wrong.getMessage(), containsString("is not the metadata of"));
    }

    public void testABaselineIsForItsCommitAndTheLaterOnes() {
        final var merged = file("_2.cfs", 3000, 13, randomWriterUuid());
        // _0.cfs and _1.cfs were merged into _2.cfs after the first snapshot
        final var repository = repository(
            snapshot("snap-1", null, file("_0.cfs", 1000, 11, randomWriterUuid()), file("_1.cfs", 2000, 12, randomWriterUuid())),
            snapshot("snap-2", null, merged)
        );
        final var baselineCommit = commit(null, merged);

        assertBacklog(repository, baselineCommit, baselineCommit, 0);
        assertBacklog(repository, baselineCommit, commit(null, merged, file("_3.cfs", 500, 14, randomWriterUuid())), 500);
    }

    public void testARepositoryWithoutWriterUuidsCanCountAFileTheSnapshotWouldReuse() {
        // files recorded by a version before 7.16, which kept no writer uuids; serverless repositories are all newer than that
        final var repository = repository(
            snapshot("snap-1", null, file("_0.cfs", 1000, 11, NO_WRITER_UUID), file("_1.cfs", 2000, 12, NO_WRITER_UUID))
        );
        // the index is created again under the same name, and the baseline for its first commit leaves out the old _1.cfs
        final var newCfs = file("_0.cfs", 500, 21, randomWriterUuid());
        final var baseline = baseline(repository, commit(null, newCfs));
        // by chance its new _1.cfs has the length and checksum of the old one, which the snapshot would then reuse
        final var later = commit(null, newCfs, file("_1.cfs", 2000, 12, randomWriterUuid()));

        assertThat(uploadedBytes(repository, later), equalTo(500L));
        assertThat(ShardBacklog.bytesToUpload(baseline, null, luceneFiles(later), Map.of()), equalTo(2500L));
    }

    /**
     * Builds a repository the way snapshots do, from a random history of a shard. Commits are drawn from a few file names, each with a
     * few versions that often share their length and checksum, sometimes without writer uuid. The state id changes with new writes and
     * stays the same when only the files change, as after a merge; a snapshot taken, or a commit checked, before the shard settled has
     * none. Some histories clone
     * older snapshots, some add a snapshot of an older commit, as a primary of an older term might, and some snapshots are removed as
     * retention does. After every step, and against the shard-level metadata read back at the end, the backlog must never be below what
     * the snapshot would upload, and must equal it unless the latest state id came from a clone or the history is out of order.
     */
    public void testRandomHistoriesMatchWhatTheSnapshotWouldUpload() throws IOException {
        final var versions = randomFileVersions();
        final boolean outOfOrder = randomInt(4) == 0;
        final List<Commit> commits = new ArrayList<>();
        final Set<String> clones = new HashSet<>();
        final List<SnapshotId> snapshotIds = new ArrayList<>();
        var snapshots = BlobStoreIndexShardSnapshots.EMPTY;
        // snapshot names are never reused, even after retention removed the snapshot
        int names = 0;
        int writes = 0;
        var live = new Commit(stateIdAfterWrites(writes), randomFiles(versions));
        commits.add(live);
        boolean disordered = false;
        for (int step = between(1, 12); step > 0; step--) {
            switch (between(0, 4)) {
                case 0 -> {
                    writes++;
                    live = new Commit(stateIdAfterWrites(writes), randomFiles(versions));
                    commits.add(live);
                }
                case 1 -> {
                    live = new Commit(live.stateId(), randomFiles(versions));
                    commits.add(live);
                }
                case 2 -> {
                    final boolean older = outOfOrder && randomBoolean();
                    final var snapshotted = older ? randomFrom(commits) : live;
                    disordered |= older;
                    final var snapshotId = new SnapshotId("snap-" + names++, randomUUID());
                    final var recorded = new Commit(randomBoolean() ? snapshotted.stateId() : null, snapshotted.files());
                    snapshots = snapshotShard(snapshots, snapshotId.getName(), recorded);
                    snapshotIds.add(snapshotId);
                }
                case 3 -> {
                    if (snapshotIds.isEmpty() == false) {
                        final var cloneId = new SnapshotId("clone-" + names++, randomUUID());
                        snapshots = snapshots.withClone(randomFrom(snapshots.snapshots()).snapshot(), cloneId.getName());
                        snapshotIds.add(cloneId);
                        clones.add(cloneId.getName());
                    }
                }
                case 4 -> {
                    if (snapshotIds.isEmpty() == false) {
                        final var retained = randomSubsetOf(snapshotIds);
                        snapshots = snapshots.withRetainedSnapshots(Set.copyOf(retained));
                        snapshotIds.retainAll(retained);
                    }
                }
                default -> throw new AssertionError();
            }
            final var checked = maybeWithoutStateId(live);
            assertAgainstWhatTheSnapshotWouldUpload(snapshots, checked, checked, isExact(snapshots, clones, disordered));
        }

        final var readBack = readBack(snapshots);
        for (int i = between(1, 3); i > 0; i--) {
            final var checked = maybeWithoutStateId(live);
            assertAgainstWhatTheSnapshotWouldUpload(readBack, checked, checked, isExact(readBack, clones, disordered));
        }
    }

    /**
     * Commits as Lucene writes them and a snapshot reads them ({@link Store.MetadataSnapshot#loadFromIndexCommit}), with deletes and
     * merges, and then an index created again under the same file names. As on a node, the baseline is built for the first commit of
     * each index and again after each snapshot, and then used for the later commits until the next snapshot. The first index never
     * gives a later file the name of a file the baseline left out; the recreated one does, as it reaches the old index's names. Also
     * checks what {@link ShardBacklog} assumes of such commits: only the files it leaves out by name have a hash.
     */
    public void testLuceneCommitsMatchWhatTheSnapshotWouldUpload() throws IOException {
        var snapshots = BlobStoreIndexShardSnapshots.EMPTY;
        // both indices use the same file format, so that their file names can be the same
        final boolean useCompoundFile = randomBoolean();
        final int recreateAt = between(2, 6);
        boolean reusedALeftOutName = false;
        for (int index = 0; index < 2; index++) {
            try (var directory = newDirectory()) {
                final var config = new IndexWriterConfig().setUseCompoundFile(useCompoundFile)
                    .setMergeScheduler(new SerialMergeScheduler());
                try (var writer = new IndexWriter(directory, config)) {
                    Commit baselineCommit = null;
                    Set<String> leftOut = Set.of();
                    int docs = 0;
                    final int steps = index == 0 ? recreateAt : 6;
                    for (int step = 0; step < steps; step++) {
                        for (int i = between(5, 20); i > 0; i--) {
                            writer.addDocument(List.of(new StringField("id", Integer.toString(docs++), Field.Store.NO)));
                        }
                        if (randomBoolean()) {
                            writer.deleteDocuments(new Term("id", Integer.toString(between(0, docs - 1))));
                        }
                        // the first index keeps its segments, so that the recreated one reaches their names
                        if (index == 1 && rarely()) {
                            writer.forceMerge(1);
                        }
                        writer.commit();
                        final var files = new ArrayList<StoreFileMetadata>();
                        for (StoreFileMetadata file : Store.MetadataSnapshot.loadFromIndexCommit(null, directory, logger)) {
                            // what ShardBacklog and its caller rely on: the files it leaves out by name are exactly those kept
                            // as their hash, and the others have no hash
                            final boolean inlined = Store.MetadataSnapshot.isReadAsHash(file.name());
                            assertThat(file.name(), file.hashEqualsContents(), equalTo(inlined));
                            assertThat(file.name(), file.hash().length == 0, equalTo(inlined == false));
                            files.add(file);
                        }
                        final var commit = new Commit(null, files);
                        if (baselineCommit == null) {
                            baselineCommit = commit;
                            leftOut = leftOutNames(snapshots, baselineCommit);
                        }
                        boolean reusesALeftOutName = false;
                        for (StoreFileMetadata file : files) {
                            reusesALeftOutName |= leftOut.contains(file.name());
                        }
                        if (index == 0) {
                            assertFalse("a later commit of an index has the name of a file merged away", reusesALeftOutName);
                        } else {
                            reusedALeftOutName |= reusesALeftOutName;
                        }

                        assertAgainstWhatTheSnapshotWouldUpload(snapshots, baselineCommit, commit, true);
                        if ((index == 0 && step == steps - 1) || randomBoolean()) {
                            snapshots = readBack(snapshotShard(snapshots, "snap-" + index + "-" + step, commit));
                            baselineCommit = commit;
                            leftOut = leftOutNames(snapshots, baselineCommit);
                        }
                    }
                }
            }
        }
        assertTrue("the recreated index reached a name of the old index that its baseline left out", reusedALeftOutName);
    }

    private record Commit(@Nullable String stateId, List<StoreFileMetadata> files) {}

    private record Snapshot(String name, @Nullable String stateId, List<StoreFileMetadata> files) {}

    private static Commit commit(@Nullable String stateId, StoreFileMetadata... files) {
        return new Commit(stateId, List.of(files));
    }

    private static Snapshot snapshot(String name, @Nullable String stateId, StoreFileMetadata... files) {
        return new Snapshot(name, stateId, List.of(files));
    }

    private static BlobStoreIndexShardSnapshots repository(Snapshot... snapshots) {
        var shardSnapshots = BlobStoreIndexShardSnapshots.EMPTY;
        for (Snapshot snapshot : snapshots) {
            shardSnapshots = snapshotShard(shardSnapshots, snapshot.name(), new Commit(snapshot.stateId(), snapshot.files()));
        }
        return shardSnapshots;
    }

    private static ShardBaseline baseline(BlobStoreIndexShardSnapshots snapshots, Commit commit) {
        return ShardBaseline.of(snapshots, luceneFiles(commit));
    }

    private static void assertBacklog(BlobStoreIndexShardSnapshots snapshots, Commit commit, long expectedBytes) {
        assertBacklog(snapshots, commit, commit, expectedBytes);
    }

    private static void assertBacklog(BlobStoreIndexShardSnapshots snapshots, Commit baselineCommit, Commit commit, long expectedBytes) {
        assertThat("what the snapshot would upload", uploadedBytes(snapshots, commit), equalTo(expectedBytes));
        assertAgainstWhatTheSnapshotWouldUpload(snapshots, baselineCommit, commit, true);
    }

    /**
     * Checks a baseline built for one commit, used for that commit or a later one, against the snapshot's own matching
     * ({@link BlobStoreIndexShardSnapshots#findPhysicalIndexFile}). It must ask for the checksums of exactly the files that have a
     * kept repository file of the same name, and given only those, it must never count less than the snapshot would upload, and
     * exactly as much if {@code exact}.
     */
    private static void assertAgainstWhatTheSnapshotWouldUpload(
        BlobStoreIndexShardSnapshots snapshots,
        Commit baselineCommit,
        Commit commit,
        boolean exact
    ) {
        final var baseline = baseline(snapshots, baselineCommit);
        final var needingChecksum = ShardBacklog.filesNeedingChecksum(baseline, commit.stateId(), luceneFiles(commit));
        assertThat("files needing a checksum", needingChecksum, equalTo(expectedFilesNeedingChecksum(snapshots, baselineCommit, commit)));
        final Map<String, StoreFileMetadata> metadata = new HashMap<>();
        for (StoreFileMetadata file : commit.files()) {
            if (needingChecksum.contains(luceneFile(file))) {
                metadata.put(file.name(), file);
            }
        }
        final long bytes = ShardBacklog.bytesToUpload(baseline, commit.stateId(), luceneFiles(commit), metadata);
        final long uploaded = uploadedBytes(snapshots, commit);
        assertThat("bytes to upload", bytes, greaterThanOrEqualTo(uploaded));
        if (exact) {
            assertThat("bytes to upload", bytes, equalTo(uploaded));
        }
    }

    // TODO: snapshotShard and uploadedBytes copy the decision logic of BlobStoreRepository#doSnapshotShard around the server's own
    // methods. When the backlog is tracked on index nodes, add an integration test that compares it with what real snapshots upload,
    // and consider moving this decision into BlobStoreRepository so that production and these tests share one copy.

    /**
     * The shard-level metadata after a snapshot of the commit, as {@code BlobStoreRepository#doSnapshotShard} writes it: the files of
     * the snapshot with the same state id, or else for each file the one the repository has ({@link
     * BlobStoreIndexShardSnapshots#findPhysicalIndexFile}) or a new one.
     */
    private static BlobStoreIndexShardSnapshots snapshotShard(BlobStoreIndexShardSnapshots snapshots, String name, Commit commit) {
        List<FileInfo> files = null;
        if (commit.stateId() != null) {
            for (SnapshotFiles snapshot : snapshots.snapshots()) {
                if (commit.stateId().equals(snapshot.shardStateIdentifier())) {
                    files = snapshot.indexFiles();
                    break;
                }
            }
        }
        if (files == null) {
            files = new ArrayList<>();
            for (StoreFileMetadata file : commit.files()) {
                final var existing = snapshots.findPhysicalIndexFile(file);
                files.add(
                    existing != null ? existing : new FileInfo((file.hashEqualsContents() ? "v__" : "__") + randomUUID(), file, null)
                );
            }
        }
        return snapshots.withAddedSnapshot(new SnapshotFiles(name, files, commit.stateId()));
    }

    /**
     * What {@code BlobStoreRepository#doSnapshotShard} uploads for the commit: nothing if a snapshot has the commit's state id, else the
     * files the repository does not have ({@link BlobStoreIndexShardSnapshots#findPhysicalIndexFile}) and whose contents are not their
     * hash.
     */
    private static long uploadedBytes(BlobStoreIndexShardSnapshots snapshots, Commit commit) {
        if (commit.stateId() != null) {
            for (SnapshotFiles snapshot : snapshots.snapshots()) {
                if (commit.stateId().equals(snapshot.shardStateIdentifier())) {
                    return 0;
                }
            }
        }
        long bytes = 0;
        for (StoreFileMetadata file : commit.files()) {
            if (snapshots.findPhysicalIndexFile(file) == null && file.hashEqualsContents() == false) {
                bytes += file.length();
            }
        }
        return bytes;
    }

    /**
     * The commit files a snapshot would upload unless the repository holds them, and whose name the repository has a file of, among the
     * files with the name and length of a file of the commit the baseline was built for. None if the latest snapshot with a state id
     * has the commit's.
     */
    private static List<LuceneFile> expectedFilesNeedingChecksum(
        BlobStoreIndexShardSnapshots snapshots,
        Commit baselineCommit,
        Commit commit
    ) {
        final Map<String, Long> kept = new HashMap<>();
        for (StoreFileMetadata file : baselineCommit.files()) {
            kept.put(file.name(), file.length());
        }
        if (commit.stateId() != null && commit.stateId().equals(latestStateId(snapshots))) {
            return List.of();
        }
        final List<LuceneFile> files = new ArrayList<>();
        for (StoreFileMetadata file : commit.files()) {
            if (file.hashEqualsContents() == false
                && snapshots.snapshots()
                    .stream()
                    .flatMap(snapshot -> snapshot.indexFiles().stream())
                    .anyMatch(
                        held -> held.physicalName().equals(file.name()) && Long.valueOf(held.length()).equals(kept.get(held.physicalName()))
                    )) {
                files.add(luceneFile(file));
            }
        }
        return files;
    }

    /**
     * Whether the backlog must equal what the snapshot would upload: when the history is in order and the latest state id did not come
     * from a clone, which may have put an older one last.
     */
    private static boolean isExact(BlobStoreIndexShardSnapshots snapshots, Set<String> clones, boolean disordered) {
        String latest = null;
        for (SnapshotFiles snapshot : snapshots.snapshots()) {
            if (snapshot.shardStateIdentifier() != null) {
                latest = snapshot.snapshot();
            }
        }
        return disordered == false && (latest == null || clones.contains(latest) == false);
    }

    @Nullable
    private static String latestStateId(BlobStoreIndexShardSnapshots snapshots) {
        String stateId = null;
        for (SnapshotFiles snapshot : snapshots.snapshots()) {
            if (snapshot.shardStateIdentifier() != null) {
                stateId = snapshot.shardStateIdentifier();
            }
        }
        return stateId;
    }

    /**
     * A state id as {@code SnapshotShardsService#getShardStateId} builds it, after the given number of writes.
     */
    private static String stateIdAfterWrites(int writes) {
        return "history-na-" + writes;
    }

    /**
     * The commit as checked or snapshotted: with its state id if the shard has settled, without one if not.
     */
    private static Commit maybeWithoutStateId(Commit commit) {
        return randomBoolean() ? commit : new Commit(null, commit.files());
    }

    /**
     * The names of the files the repository holds that the commit does not have, which a baseline for the commit leaves out.
     */
    private static Set<String> leftOutNames(BlobStoreIndexShardSnapshots snapshots, Commit commit) {
        final Set<String> names = new HashSet<>();
        for (SnapshotFiles snapshot : snapshots.snapshots()) {
            for (FileInfo file : snapshot.indexFiles()) {
                if (Store.MetadataSnapshot.isReadAsHash(file.physicalName()) == false) {
                    names.add(file.physicalName());
                }
            }
        }
        for (StoreFileMetadata file : commit.files()) {
            names.remove(file.name());
        }
        return names;
    }

    /**
     * The shard-level metadata as a node reads it from the repository, written with or without writer uuids as repositories of older
     * versions are.
     */
    static BlobStoreIndexShardSnapshots readBack(BlobStoreIndexShardSnapshots snapshots) throws IOException {
        try (var out = new BytesStreamOutput()) {
            INDEX_SHARD_SNAPSHOTS_FORMAT.serialize(
                snapshots,
                "index-" + randomUUID(),
                randomBoolean(),
                Map.of(FileInfo.SERIALIZE_WRITER_UUID, Boolean.toString(randomBoolean())),
                out
            );
            return INDEX_SHARD_SNAPSHOTS_FORMAT.deserialize(
                new ProjectRepo(ProjectId.DEFAULT, "repo"),
                NamedXContentRegistry.EMPTY,
                out.bytes().streamInput()
            );
        }
    }

    /**
     * A few file names, each with one to three versions as an index deleted and created again has. The versions of a name often have
     * the same length and checksum, and sometimes no writer uuid, so that only the rest of {@link StoreFileMetadata#isSame} tells them
     * apart. Each writer uuid is used by one version only: equal writer uuids mean equal files.
     */
    private static Map<String, List<StoreFileMetadata>> randomFileVersions() {
        final Map<String, List<StoreFileMetadata>> versions = new HashMap<>();
        for (String name : List.of("_0.cfs", "_0.cfe", "_0.si", "_1.cfs", "_1.si", "_1_1.liv", "_2.fdt", "segments_2", "segments_3")) {
            final List<StoreFileMetadata> fileVersions = new ArrayList<>();
            for (int i = between(1, 3); i > 0; i--) {
                final var writerUuid = randomBoolean() ? randomWriterUuid() : NO_WRITER_UUID;
                fileVersions.add(
                    Store.MetadataSnapshot.isReadAsHash(name)
                        ? inlinedFile(name, writerUuid)
                        : file(name, randomFrom(1000L, 2000L), randomFrom(11L, 12L), writerUuid)
                );
            }
            versions.put(name, fileVersions);
        }
        return versions;
    }

    private static List<StoreFileMetadata> randomFiles(Map<String, List<StoreFileMetadata>> versions) {
        final List<StoreFileMetadata> files = new ArrayList<>();
        for (List<StoreFileMetadata> fileVersions : versions.values()) {
            if (randomBoolean()) {
                files.add(randomFrom(fileVersions));
            }
        }
        return files;
    }

    private static StoreFileMetadata file(String name, long length, long checksum, BytesRef writerUuid) {
        assert Store.MetadataSnapshot.isReadAsHash(name) == false : name;
        return new StoreFileMetadata(name, length, Store.digestToString(checksum), Version.LATEST.toString(), null, writerUuid);
    }

    private static StoreFileMetadata inlinedFile(String name) {
        return inlinedFile(name, randomWriterUuid());
    }

    /**
     * A file whose contents a snapshot keeps inside the shard-level metadata: its hash is its contents, which end with a Lucene footer
     * that has its checksum, as {@link Store.MetadataSnapshot#loadFromIndexCommit} reads them.
     */
    private static StoreFileMetadata inlinedFile(String name, BytesRef writerUuid) {
        assert Store.MetadataSnapshot.isReadAsHash(name) : name;
        try {
            final var output = new ByteBuffersDataOutput();
            try (var indexOutput = new ByteBuffersIndexOutput(output, name, name)) {
                indexOutput.writeBytes(randomByteArrayOfLength(between(0, 100)), 0);
                CodecUtil.writeFooter(indexOutput);
            }
            final byte[] contents = output.toArrayCopy();
            final long checksum = CodecUtil.retrieveChecksum(new ByteArrayIndexInput(name, contents));
            return new StoreFileMetadata(
                name,
                contents.length,
                Store.digestToString(checksum),
                Version.LATEST.toString(),
                new BytesRef(contents),
                writerUuid
            );
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private static BytesRef randomWriterUuid() {
        return new BytesRef(randomByteArrayOfLength(16));
    }

    private static LuceneFile luceneFile(StoreFileMetadata file) {
        return new LuceneFile(file.name(), file.length());
    }

    private static List<LuceneFile> luceneFiles(Commit commit) {
        return commit.files().stream().map(ShardBacklogTests::luceneFile).toList();
    }
}
