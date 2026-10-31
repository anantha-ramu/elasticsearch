/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots.backlog;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Version;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.lucene.Lucene;
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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

/**
 * Tests for {@link ShardBaseline}. Most tests use three helpers:
 * <ul>
 *     <li>{@code repository(...)} builds the shard-level metadata ({@code index-{gen}}) from a list of snapshots. It adds each snapshot
 *     the way a real snapshot does: a file the repository already has is reused, and any other file is uploaded.</li>
 *     <li>{@code commit(...)} is the shard's current commit: its state id, or {@code null} if it has none, and its files.</li>
 *     <li>{@code assertBacklog(repository, commit, bytes)} checks that a snapshot of the commit would upload {@code bytes}, and that the
 *     backlog counts exactly the same. {@code assertBacklog(repository, baselineCommit, commit, bytes)} does the same, but builds the
 *     baseline for an earlier commit and uses it for a later one, as a node does between snapshots.</li>
 * </ul>
 * What a snapshot would upload is worked out by this test's copy of the snapshot's decision logic ({@code snapshotShard} and
 * {@code uploadedBytes}), which asks {@link StoreFileMetadata#isSame}: the same length and checksum, and the same writer uuid when both
 * files have one. The backlog never sees the checksums, only names, lengths and writer uuids, so comparing the two checks that the writer
 * uuid gives the same answer as the checksum. Lengths and checksums are small made-up numbers.
 * <p>
 * Most tests use made-up file metadata rather than real Lucene files. The backlog never reads a file's contents, so made-up metadata runs
 * the same code. It also lets a test state the expected number of bytes, and build cases that plain Lucene can't produce on demand:
 * <ul>
 *     <li>state ids, which Elasticsearch adds to a commit and plain Lucene doesn't;</li>
 *     <li>files without a writer uuid, as in a repository with files backed up before 7.16;</li>
 *     <li>two different files with the same name, length and checksum;</li>
 *     <li>repository events such as clones, restores and rebuilt metadata.</li>
 * </ul>
 * The tests with real Lucene commits check what the made-up metadata assumes: {@link ShardBaseline#luceneFiles} gives every file the
 * name, length and writer uuid a snapshot reads; only .si and segments_N files carry their contents as a hash; a file name comes back
 * only when the index is created again; and a segment that changes gives its kept change files a new writer uuid.
 * <p>
 * Comparing the backlog with what the real snapshot code uploads needs a real shard and repository, and is left to an integration test
 * (see the TODO above {@code snapshotShard}).
 * <p>
 * The tests go from simple to complex, in the order of the cases in the {@link ShardBaseline} Javadoc:
 * <ul>
 *     <li>comparing files: which files count and which don't;</li>
 *     <li>the state id, which lets the backlog skip comparing files;</li>
 *     <li>repository events the backlog handles exactly: a deleted snapshot, rebuilt metadata and a restore;</li>
 *     <li>the known over-counts: a clone of an older snapshot, and files without a writer uuid;</li>
 *     <li>what the caller must do: build a new baseline when the metadata changes;</li>
 *     <li>random histories and real Lucene commits, checked against what the snapshot would upload.</li>
 * </ul>
 */
public class ShardBaselineTests extends ESTestCase {

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

    public void testFilesKeptInTheBackupMetadataAreNotCounted() {
        final var cfs = file("_1.cfs", 1000, 11, randomWriterUuid());
        final var repository = repository(snapshot("snap-1", null, inlinedFile("_0.si"), inlinedFile("segments_2")));

        // A snapshot keeps the contents of .si and segments_N files in the shard-level metadata, instead of uploading them. So
        // they are never counted, new or changed. Only _1.cfs is counted.
        assertBacklog(repository, commit(null, inlinedFile("_0.si"), inlinedFile("_1.si"), inlinedFile("segments_3"), cfs), 1000);
    }

    public void testAFileOfAnyRetainedSnapshotMatches() {
        final var older = file("_0.cfs", 1000, 11, randomWriterUuid());
        final var newer = file("_0.cfs", 1000, 12, randomWriterUuid());
        // The index was deleted and created again between the two snapshots, so the repository has two different files named
        // _0.cfs. A file that matches either of them is not counted.
        final var repository = repository(snapshot("snap-1", null, older), snapshot("snap-2", null, newer));

        assertBacklog(repository, commit(null, older), 0);
        assertBacklog(repository, commit(null, newer), 0);
    }

    public void testARecreatedIndexWithTheSameFileNamesIsCounted() {
        final var repository = repository(snapshot("snap-1", "state-1", file("_0.cfs", 1000, 11, randomWriterUuid())));
        // The index is deleted and created again under the same name. The new index starts again from _0, so its first file has the
        // same name, and here even the same length and checksum. But the writer uuid is the id Lucene gives a segment when it writes
        // it, so another writer uuid means another segment, and the snapshot uploads the file again.
        assertBacklog(repository, commit("state-2", file("_0.cfs", 1000, 11, randomWriterUuid())), 1000);
    }

    public void testTheLatestStateIdMeansNothingToUpload() {
        final var cfs1 = file("_1.cfs", 2000, 12, randomWriterUuid());
        final var repository = repository(
            snapshot("snap-1", "state-1", file("_0.cfs", 1000, 11, randomWriterUuid())),
            snapshot("snap-2", "state-2", cfs1)
        );
        // There were no writes since snap-2, but a background merge rewrote the files into _2.cfs. The shard is still at state-2,
        // the state id of the latest snapshot, so the backlog is 0 without comparing any files.
        assertBacklog(repository, commit("state-2", file("_2.cfs", 3000, 13, randomWriterUuid())), 0);

        // Had there been new writes instead, adding _3.cfs, the shard would have another state id, state-3, or none while the
        // writes are still in flight. Either way it matches no snapshot, so the backlog compares files: _1.cfs is in the
        // repository, and only _3.cfs is counted.
        assertBacklog(repository, commit(randomBoolean() ? "state-3" : null, cfs1, file("_3.cfs", 500, 14, randomWriterUuid())), 500);
    }

    public void testTheLatestSnapshotWithAStateIdCounts() {
        // snap-0 was taken while writes were still in flight, so it has no state id.
        // snap-1 was taken after the writes finished and the shard had settled.
        // A clone copies the state id of its source, so clone-of-snap-0, the newest snapshot, has no state id either.
        final var repository = repository(
            snapshot("snap-0", null, file("_0.cfs", 1000, 11, randomWriterUuid())),
            snapshot("snap-1", "state-1", file("_1.cfs", 2000, 12, randomWriterUuid()))
        ).withClone("snap-0", "clone-of-snap-0");

        // The baseline skips snapshots without a state id, so the state id it keeps is state-1, from snap-1.
        // There were no writes since snap-1, but a background merge rewrote _1.cfs into _2.cfs, for example to drop
        // deleted documents, so no file name matches. (A force merge would change the state id.)
        // The shard is still at state-1, so the backlog is 0 without comparing any files.
        assertBacklog(repository, commit("state-1", file("_2.cfs", 2000, 13, randomWriterUuid())), 0);
    }

    public void testADeletedSnapshotNoLongerCounts() {
        final var cfs0 = file("_0.cfs", 1000, 11, randomWriterUuid());
        final var cfs1 = file("_1.cfs", 2000, 12, randomWriterUuid());
        // snap-2 is deleted. It was the latest snapshot and the only one with _1.cfs.
        final var repository = repository(snapshot("snap-1", "state-1", cfs0), snapshot("snap-2", "state-2", cfs0, cfs1))
            .withRetainedSnapshots(Set.of(new SnapshotId("snap-1", randomUUID())));

        // The shard hasn't changed since snap-2, but snap-2's state id was deleted with it, and so was the only copy of _1.cfs in
        // the repository. The next snapshot would upload _1.cfs again, so the backlog counts it.
        assertBacklog(repository, commit("state-2", cfs0, cfs1), 2000);
    }

    public void testRepairedMetadataWithoutStateIdsComparesTheFiles() {
        final var cfs0 = file("_0.cfs", 1000, 11, randomWriterUuid());
        // Shard-level metadata that was rebuilt from the metadata file of each snapshot has no state ids at all.
        final var repository = repository(snapshot("snap-1", null, cfs0), snapshot("snap-2", null, cfs0));

        // The shard has a state id, but no snapshot has one to match it. So the backlog compares files, and even a merge with no
        // new writes counts the merged file, which the snapshot would upload too.
        assertBacklog(repository, commit("state-2", file("_1.cfs", 3000, 13, randomWriterUuid())), 3000);
        // Files the repository already has are still not counted.
        assertBacklog(repository, commit("state-2", cfs0), 0);
    }

    public void testARestoredSnapshotMatchesByItsFiles() {
        final var cfs0 = file("_0.cfs", 1000, 11, randomWriterUuid());
        final var repository = repository(
            snapshot("snap-1", "history-1-na-5", cfs0),
            snapshot("snap-2", "history-1-na-9", file("_1.cfs", 2000, 12, randomWriterUuid()))
        );
        // snap-1 is restored. The shard has snap-1's files again, with their writer uuids, but a restore starts a new history, so
        // the state id is new.
        final var restored = commit("history-2-na-5", cfs0);

        assertBacklog(repository, restored, 0);
        // After the restore, a new file gets the name, length and checksum of snap-2's _1.cfs. Its writer uuid is different, so it
        // is another file, and the backlog counts it.
        assertBacklog(repository, restored, commit("history-2-na-7", cfs0, file("_1.cfs", 2000, 12, randomWriterUuid())), 2000);
    }

    public void testACloneOfAnOlderSnapshotCountsUntilTheNextSnapshot() {
        for (boolean originalDeleted : List.of(false, true)) {
            final var repository = repositoryWithACloneOfAnOlderSnapshot(originalDeleted);
            // There were no writes since snap-2, but a background merge rewrote the files into _2.cfs, so the shard is still at
            // state-2. The snapshot finds snap-2 with that state id and uploads nothing.
            final var merged = commit("state-2", file("_2.cfs", 3000, 13, randomWriterUuid()));

            assertThat(uploadedBytes(repository, merged), equalTo(0L));
            // The clone is the newest snapshot, so the baseline keeps its state-1, not state-2. The shard's state-2 doesn't match,
            // so the backlog compares files and counts _2.cfs, which no snapshot has. The backlog and the snapshot disagree here,
            // so this checks each one on its own instead of using assertBacklog.
            assertThat(baseline(repository, merged).bytesToUpload("state-2", luceneFiles(merged)), equalTo(3000L));
            // The next snapshot is taken with the shard still at state-2. It uploads nothing and records state-2, which is now the
            // newest state id again, so the backlog is back to 0.
            assertBacklog(snapshotShard(repository, "snap-3", merged), merged, 0);
        }
    }

    public void testACloneOfAnOlderSnapshotStopsMatteringOnceTheShardIsWrittenTo() {
        final var cfs0 = file("_0.cfs", 1000, 11, randomWriterUuid());
        final var cfs1 = file("_1.cfs", 2000, 12, randomWriterUuid());
        // The clone of snap-1 is the newest snapshot, so the baseline keeps state-1, as in
        // testACloneOfAnOlderSnapshotCountsUntilTheNextSnapshot.
        final var repository = repository(snapshot("snap-1", "state-1", cfs0), snapshot("snap-2", "state-2", cfs0, cfs1)).withClone(
            "snap-1",
            "clone-1"
        );
        // New writes since snap-2 added the segment _2.cfs. The shard now has the state id state-3, or none while writes are
        // still in flight. Either way it matches no snapshot, so the backlog and the snapshot both compare files, and the
        // backlog counts exactly the new segment.
        final var written = commit(randomBoolean() ? "state-3" : null, cfs0, cfs1, file("_2.cfs", 4000, 13, randomWriterUuid()));

        assertBacklog(repository, written, 4000);
    }

    /**
     * Builds snap-1 (state-1) and snap-2 (state-2), then a clone of snap-1, which makes state-1 the newest state id again. If
     * {@code originalDeleted}, snap-1 is then deleted, as retention might do, so the clone is the only snapshot left with state-1.
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

    public void testAFileWithoutAWriterUuidIsCounted() {
        // This can't happen in serverless. A file has no writer uuid if Lucene before 8.6 wrote it, or, in the repository, if a
        // version before 7.16 backed it up. When either side has none, the snapshot ignores writer uuids and compares the
        // checksums, which the backlog doesn't have. So the backlog counts the file, even where the snapshot reuses it: it counts
        // too much, never too little.
        final var repository = repository(
            snapshot(
                "snap-1",
                null,
                file("_0.cfs", 1000, 11, randomWriterUuid()),
                file("_1.cfs", 2000, 12, NO_WRITER_UUID),
                file("_2.cfs", 4000, 13, NO_WRITER_UUID)
            )
        );
        // Each file has the length and checksum of the repository's file of that name. The writer uuid is missing in the shard,
        // in the repository, or in both.
        final var commit = commit(
            null,
            file("_0.cfs", 1000, 11, NO_WRITER_UUID),
            file("_1.cfs", 2000, 12, randomWriterUuid()),
            file("_2.cfs", 4000, 13, NO_WRITER_UUID)
        );

        assertThat(uploadedBytes(repository, commit), equalTo(0L));
        assertThat(baseline(repository, commit).bytesToUpload(null, luceneFiles(commit)), equalTo(7000L));
    }

    public void testABaselineIsForItsCommitAndTheLaterOnes() {
        final var merged = file("_2.cfs", 3000, 13, randomWriterUuid());
        // After the first snapshot, _0.cfs and _1.cfs were merged into _2.cfs.
        final var repository = repository(
            snapshot("snap-1", null, file("_0.cfs", 1000, 11, randomWriterUuid()), file("_1.cfs", 2000, 12, randomWriterUuid())),
            snapshot("snap-2", null, merged)
        );
        // The baseline is built for the commit that has only _2.cfs, so it leaves out the repository's _0.cfs and _1.cfs.
        final var baselineCommit = commit(null, merged);

        assertBacklog(repository, baselineCommit, baselineCommit, 0);
        // A node builds the baseline once and keeps using it for later commits, until the shard-level metadata (index-{gen})
        // changes, for example after a snapshot, a clone or a deletion. Reusing it is safe here because _0.cfs and _1.cfs, the
        // repository files the baseline left out, never come back: within one index, Lucene never reuses a file name. So a new
        // file such as _3.cfs is counted, as the snapshot would count it.
        assertBacklog(repository, baselineCommit, commit(null, merged, file("_3.cfs", 500, 14, randomWriterUuid())), 500);
    }

    public void testABaselineBuiltBeforeADeletionCanCountTooLittle() {
        final var cfs0 = file("_0.cfs", 1000, 11, randomWriterUuid());
        final var cfs1 = file("_1.cfs", 2000, 12, randomWriterUuid());
        final var beforeDeletion = repository(snapshot("snap-1", "state-1", cfs0), snapshot("snap-2", "state-2", cfs0, cfs1));
        final var shard = commit("state-3", cfs0, cfs1, file("_2.cfs", 500, 13, randomWriterUuid()));
        final var outdated = baseline(beforeDeletion, shard);
        // After the baseline was built, snap-2 is deleted. It was the only snapshot with _1.cfs.
        final var afterDeletion = beforeDeletion.withRetainedSnapshots(Set.of(new SnapshotId("snap-1", randomUUID())));

        // The snapshot would now upload _1.cfs again, as well as the new _2.cfs.
        assertThat(uploadedBytes(afterDeletion, shard), equalTo(2500L));
        // The outdated baseline still has _1.cfs, so it counts only _2.cfs, less than the snapshot would upload. This is why the
        // caller must build a new baseline whenever the shard-level metadata changes.
        assertThat(outdated.bytesToUpload("state-3", luceneFiles(shard)), equalTo(500L));
        // A new baseline counts the right amount.
        assertBacklog(afterDeletion, shard, 2500);
    }

    /**
     * Builds a repository the way snapshots do, from a random history of a shard. Commits are drawn from a few file names, each with a
     * few versions that often share their length and checksum. The state id changes with new writes and stays the same when only the
     * files change, as after a merge. A snapshot taken, or a commit checked, before the shard settled has no state id. Some histories
     * clone older snapshots, and some snapshots are removed as retention does. After every step, and against the shard-level metadata
     * read back at the end, the backlog must never be below what the snapshot would upload. It must equal it unless the latest state id
     * came from a clone, or the history has files without a writer uuid, which about half of the histories have.
     * <p>
     * The histories are looser than serverless in two ways: a snapshot of a settled shard sometimes has no state id, and a file name
     * can come back as a different file within one index, which Lucene never does. Neither breaks the checks, because every check
     * builds a new baseline for the commit it checks. Reusing a baseline for later commits is covered by
     * {@link #testLuceneCommitsMatchWhatTheSnapshotWouldUpload}.
     */
    public void testRandomHistoriesMatchWhatTheSnapshotWouldUpload() throws IOException {
        final boolean everyFileHasAWriterUuid = randomBoolean();
        final var versions = randomFileVersions(everyFileHasAWriterUuid);
        final Set<String> clones = new HashSet<>();
        final List<SnapshotId> snapshotIds = new ArrayList<>();
        var snapshots = BlobStoreIndexShardSnapshots.EMPTY;
        // Snapshot names are never reused, even after retention removed the snapshot.
        int names = 0;
        int writes = 0;
        var live = new Commit(stateIdAfterWrites(writes), randomFiles(versions));
        for (int step = between(1, 12); step > 0; step--) {
            switch (between(0, 4)) {
                case 0 -> {
                    writes++;
                    live = new Commit(stateIdAfterWrites(writes), randomFiles(versions));
                }
                case 1 -> {
                    live = new Commit(live.stateId(), randomFiles(versions));
                }
                case 2 -> {
                    final var snapshotId = new SnapshotId("snap-" + names++, randomUUID());
                    final var recorded = new Commit(randomBoolean() ? live.stateId() : null, live.files());
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
            assertAgainstWhatTheSnapshotWouldUpload(snapshots, checked, checked, everyFileHasAWriterUuid && isExact(snapshots, clones));
        }

        // A version before 7.16 would have written the metadata without writer uuids.
        final var readBack = readBack(snapshots, everyFileHasAWriterUuid || randomBoolean());
        for (int i = between(1, 3); i > 0; i--) {
            final var checked = maybeWithoutStateId(live);
            assertAgainstWhatTheSnapshotWouldUpload(readBack, checked, checked, everyFileHasAWriterUuid && isExact(readBack, clones));
        }
    }

    /**
     * Uses real Lucene commits, with deletes, updates and merges, read the way a snapshot reads them
     * ({@link Store.MetadataSnapshot#loadFromIndexCommit}). After the first index, the index is created again and reuses the same file
     * names. As a node would, the test builds a baseline for the first commit of each index and again after each snapshot, and uses it
     * for the later commits until the next snapshot. The first index never gives a new file the name of a file the baseline left out.
     * The recreated index does, because it starts again from the same names.
     * <p>
     * These are plain Lucene commits, without the user data Elasticsearch adds, so they have no state id. The state id rule is covered
     * by the other tests.
     */
    public void testLuceneCommitsMatchWhatTheSnapshotWouldUpload() throws IOException {
        // Both indices use the same file format, compound or not, so that the recreated index can reach the same file names.
        final boolean useCompoundFile = randomBoolean();
        // The first index never brings back a name its baseline left out.
        final var first = checkCommitsOfOneIndex(BlobStoreIndexShardSnapshots.EMPTY, useCompoundFile, between(2, 6), false);
        assertFalse("a later commit of an index has the name of a file merged away", first.reusedALeftOutName());
        // The index is deleted and created again under the same name. Lucene starts again from _0, so the recreated index reaches
        // the old index's file names, which its baseline left out. Its second commit always has _1, which the first index had too.
        final var recreated = checkCommitsOfOneIndex(first.snapshots(), useCompoundFile, 6, true);
        assertTrue("the recreated index reached a name of the old index that its baseline left out", recreated.reusedALeftOutName());
    }

    /**
     * A segment that already had a file of deleted documents changes again: a document in it is updated, which with soft deletes
     * Lucene records as a doc values update. Lucene keeps the file of deleted documents, unchanged, but gives it a new writer uuid,
     * so the snapshot uploads it again, and the backlog must count it.
     */
    public void testAChangedSegmentIsCountedAgain() throws IOException {
        final var config = new IndexWriterConfig().setSoftDeletesField(Lucene.SOFT_DELETES_FIELD)
            .setMergePolicy(NoMergePolicy.INSTANCE)
            .setUseCompoundFile(randomBoolean());
        try (var directory = newDirectory(); var writer = new IndexWriter(directory, config)) {
            for (int id = 0; id < 10; id++) {
                writer.addDocument(document(id));
            }
            writer.deleteDocuments(new Term("id", "0"));
            writer.commit();
            final var first = readCommit(directory);
            final var repository = readBack(snapshotShard(BlobStoreIndexShardSnapshots.EMPTY, "snap-1", first), true);

            writer.softUpdateDocument(new Term("id", "1"), document(1), new NumericDocValuesField(Lucene.SOFT_DELETES_FIELD, 1));
            writer.commit();
            final var second = readCommit(directory);

            final var deletedDocuments = first.files().stream().filter(file -> file.name().endsWith(".liv")).toList();
            assertThat(deletedDocuments, hasSize(1));
            final var before = deletedDocuments.getFirst();
            final var after = second.files().stream().filter(file -> file.name().equals(before.name())).findFirst().orElseThrow();
            assertThat("the same file", after.checksum(), equalTo(before.checksum()));
            assertThat("with a new writer uuid", after.writerUuid(), not(equalTo(before.writerUuid())));

            // The snapshot uploads the file of deleted documents again, and every file the first commit didn't have, apart from the
            // .si and segments_N files.
            long expected = after.length();
            for (StoreFileMetadata file : second.files()) {
                if (file.hashEqualsContents() == false && first.files().stream().noneMatch(old -> old.name().equals(file.name()))) {
                    expected += file.length();
                }
            }
            // With a baseline built before the change, as well as with a new one.
            assertBacklog(repository, first, second, expected);
            assertBacklog(repository, second, expected);
        }
    }

    private record CheckedIndex(BlobStoreIndexShardSnapshots snapshots, boolean reusedALeftOutName) {}

    /**
     * Writes one index in {@code steps} commits, and checks the backlog of each commit against what the snapshot would upload. The
     * repository is shared with any earlier index of the same name. The baseline is built at the index's first commit, as a node
     * builds one when a shard starts, and again after each snapshot. Snapshots are taken at random, and always of the last commit,
     * so that an index created again under the same name finds this index's files in the repository. Only a recreated index may
     * force merge ({@code mayForceMerge}): the first index keeps its segments, so that the recreated one can reach their names. Even
     * then, the first two commits are never force merged, so that the second commit always has the segment _1. A merged segment can
     * be written in another file format than a new one, so after a force merge the names might not match the first index's.
     */
    private CheckedIndex checkCommitsOfOneIndex(
        BlobStoreIndexShardSnapshots snapshots,
        boolean useCompoundFile,
        int steps,
        boolean mayForceMerge
    ) throws IOException {
        final var config = new IndexWriterConfig().setUseCompoundFile(useCompoundFile)
            .setSoftDeletesField(Lucene.SOFT_DELETES_FIELD)
            .setMergeScheduler(new SerialMergeScheduler());
        try (var directory = newDirectory(); var writer = new IndexWriter(directory, config)) {
            Commit baselineCommit = null;
            Set<String> leftOut = Set.of();
            boolean reusedALeftOutName = false;
            int docs = 0;
            for (int step = 0; step < steps; step++) {
                docs = addDocumentsAndChangeSome(writer, docs);
                if (mayForceMerge && step >= 2 && rarely()) {
                    writer.forceMerge(1);
                }
                writer.commit();
                final var commit = readCommit(directory);
                if (baselineCommit == null) {
                    baselineCommit = commit;
                    leftOut = leftOutNames(snapshots, baselineCommit);
                }
                for (StoreFileMetadata file : commit.files()) {
                    reusedALeftOutName |= leftOut.contains(file.name());
                }

                assertAgainstWhatTheSnapshotWouldUpload(snapshots, baselineCommit, commit, true);
                if (step == steps - 1 || randomBoolean()) {
                    snapshots = readBack(snapshotShard(snapshots, "snap-" + randomUUID(), commit), true);
                    baselineCommit = commit;
                    leftOut = leftOutNames(snapshots, baselineCommit);
                }
            }
            return new CheckedIndex(snapshots, reusedALeftOutName);
        }
    }

    /**
     * Adds 5 to 20 documents, numbered on from {@code docs}, and sometimes deletes or updates one of the documents so far. Returns the
     * new number of documents. A delete writes a new file of deleted documents for the segment, and an update, which soft deletes the
     * old copy, writes doc values for it; either gives the segment's change files a new writer uuid.
     */
    private static int addDocumentsAndChangeSome(IndexWriter writer, int docs) throws IOException {
        for (int i = between(5, 20); i > 0; i--) {
            writer.addDocument(document(docs++));
        }
        final int id = between(0, docs - 1);
        switch (between(0, 2)) {
            case 0 -> {
            }
            case 1 -> writer.deleteDocuments(new Term("id", Integer.toString(id)));
            case 2 -> writer.softUpdateDocument(
                new Term("id", Integer.toString(id)),
                document(id),
                new NumericDocValuesField(Lucene.SOFT_DELETES_FIELD, 1)
            );
            default -> throw new AssertionError();
        }
        return docs;
    }

    private static List<Field> document(int id) {
        return List.of(new StringField("id", Integer.toString(id), Field.Store.NO));
    }

    /**
     * The files of the latest commit, read the way a snapshot reads them. Also checks what the backlog relies on:
     * <ul>
     *     <li>{@link ShardBaseline#luceneFiles} gives every file of the commit the name, length and writer uuid the snapshot reads;</li>
     *     <li>the files it leaves out by name, .si and segments_N, are exactly the files whose contents are kept as their hash, and the
     *     other files have no hash.</li>
     * </ul>
     */
    private Commit readCommit(Directory directory) throws IOException {
        final var files = new ArrayList<StoreFileMetadata>();
        for (StoreFileMetadata file : Store.MetadataSnapshot.loadFromIndexCommit(null, directory, logger)) {
            final boolean inlined = Store.MetadataSnapshot.isReadAsHash(file.name());
            assertThat(file.name(), file.hashEqualsContents(), equalTo(inlined));
            assertThat(file.name(), file.hash().length == 0, equalTo(inlined == false));
            files.add(file);
        }
        final var commit = new Commit(null, files);
        final var luceneFiles = ShardBaseline.luceneFiles(SegmentInfos.readLatestCommit(directory), directory);
        assertThat(luceneFiles, hasSize(files.size()));
        assertThat(Set.copyOf(luceneFiles), equalTo(Set.copyOf(luceneFiles(commit))));
        return commit;
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
     * Checks the backlog against what the snapshot would upload. The baseline is built for {@code baselineCommit} and used for
     * {@code commit}, which is the same commit or a later one. The backlog must never count less than the snapshot would upload, and
     * if {@code exact}, it must count exactly as much.
     */
    private static void assertAgainstWhatTheSnapshotWouldUpload(
        BlobStoreIndexShardSnapshots snapshots,
        Commit baselineCommit,
        Commit commit,
        boolean exact
    ) {
        final long bytes = baseline(snapshots, baselineCommit).bytesToUpload(commit.stateId(), luceneFiles(commit));
        final long uploaded = uploadedBytes(snapshots, commit);
        assertThat("bytes to upload", bytes, greaterThanOrEqualTo(uploaded));
        if (exact) {
            assertThat("bytes to upload", bytes, equalTo(uploaded));
        }
    }

    // TODO: snapshotShard and uploadedBytes copy the decision logic of BlobStoreRepository#doSnapshotShard around the server's own
    // methods. When the backlog is tracked on index nodes, add an integration test that compares it with what real snapshots upload,
    // and consider moving this decision into BlobStoreRepository so that production and these tests share one copy. A real snapshot
    // flushes first, so that test must compare with a snapshot of the same commit.

    /**
     * The shard-level metadata after a snapshot of the commit, as {@code BlobStoreRepository#doSnapshotShard} writes it. If a snapshot
     * has the commit's state id, the new snapshot reuses that snapshot's files. Otherwise, for each file of the commit, it reuses the
     * repository's copy if there is one ({@link BlobStoreIndexShardSnapshots#findPhysicalIndexFile}), and uploads the file if not.
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
     * The bytes {@code BlobStoreRepository#doSnapshotShard} would upload for the commit. If a snapshot has the commit's state id, that
     * is nothing. Otherwise it is the files the repository doesn't have ({@link BlobStoreIndexShardSnapshots#findPhysicalIndexFile}),
     * leaving out the .si and segments_N files, whose contents go into the metadata instead.
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
     * Whether the backlog must equal what the snapshot would upload, as far as state ids go: when the latest state id did not come
     * from a clone, which may have put an older state id last.
     */
    private static boolean isExact(BlobStoreIndexShardSnapshots snapshots, Set<String> clones) {
        String latest = null;
        for (SnapshotFiles snapshot : snapshots.snapshots()) {
            if (snapshot.shardStateIdentifier() != null) {
                latest = snapshot.snapshot();
            }
        }
        return latest == null || clones.contains(latest) == false;
    }

    /**
     * A state id as {@code SnapshotShardsService#getShardStateId} builds it, after the given number of writes.
     */
    private static String stateIdAfterWrites(int writes) {
        return "history-na-" + writes;
    }

    /**
     * Returns the commit as it is or without its state id, at random. A shard has no state id while writes are in flight, so the
     * backlog must also be right for a commit without one.
     */
    private static Commit maybeWithoutStateId(Commit commit) {
        return randomBoolean() ? commit : new Commit(null, commit.files());
    }

    /**
     * The names of the repository's files that the commit doesn't have. A baseline built for the commit leaves these files out.
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
     * The shard-level metadata as a node reads it back from the repository. It is written with writer uuids, or without them as a
     * version before 7.16 would write it.
     */
    static BlobStoreIndexShardSnapshots readBack(BlobStoreIndexShardSnapshots snapshots, boolean withWriterUuids) throws IOException {
        try (var out = new BytesStreamOutput()) {
            INDEX_SHARD_SNAPSHOTS_FORMAT.serialize(
                snapshots,
                "index-" + randomUUID(),
                randomBoolean(),
                Map.of(FileInfo.SERIALIZE_WRITER_UUID, Boolean.toString(withWriterUuids)),
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
     * A few file names, each with one to three versions, as when an index is deleted and created again under the same name. The
     * versions of one name often have the same length and checksum, so that only the writer uuid tells them apart. Unless
     * {@code everyFileHasAWriterUuid}, some versions have no writer uuid. Each writer uuid belongs to one version only, so equal writer
     * uuids mean equal files, as in Lucene.
     */
    private static Map<String, List<StoreFileMetadata>> randomFileVersions(boolean everyFileHasAWriterUuid) {
        final Map<String, List<StoreFileMetadata>> versions = new HashMap<>();
        for (String name : List.of("_0.cfs", "_0.cfe", "_0.si", "_1.cfs", "_1.si", "_1_1.liv", "_2.fdt", "segments_2", "segments_3")) {
            final List<StoreFileMetadata> fileVersions = new ArrayList<>();
            for (int i = between(1, 3); i > 0; i--) {
                final var writerUuid = everyFileHasAWriterUuid || randomBoolean() ? randomWriterUuid() : NO_WRITER_UUID;
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
     * A .si or segments_N file, whose contents a snapshot keeps inside the shard-level metadata instead of uploading the file. The
     * hash holds the file's contents, which end with a Lucene footer containing the checksum, the same way
     * {@link Store.MetadataSnapshot#loadFromIndexCommit} reads such a file.
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
        return new LuceneFile(file.name(), file.length(), file.writerUuid());
    }

    private static List<LuceneFile> luceneFiles(Commit commit) {
        return commit.files().stream().map(ShardBaselineTests::luceneFile).toList();
    }
}
