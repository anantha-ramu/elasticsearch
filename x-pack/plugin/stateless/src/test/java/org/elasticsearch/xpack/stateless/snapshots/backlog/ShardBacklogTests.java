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

/**
 * Tests for {@link ShardBacklog} and {@link ShardBaseline}. Most tests use three helpers:
 * <ul>
 *     <li>{@code repository(...)} builds the shard-level metadata ({@code index-{gen}}) from a list of snapshots. It adds each snapshot
 *     the way a real snapshot does: a file the repository already has is reused, and any other file is uploaded.</li>
 *     <li>{@code commit(...)} is the shard's current commit: its state id, or {@code null} if it has none, and its files.</li>
 *     <li>{@code assertBacklog(repository, commit, bytes)} checks that a snapshot of the commit would upload {@code bytes}, and that the
 *     backlog counts exactly the same. {@code assertBacklog(repository, baselineCommit, commit, bytes)} does the same, but builds the
 *     baseline for an earlier commit and uses it for a later one, as a node does between snapshots.</li>
 * </ul>
 * Whether a file of the commit matches a file of the repository is decided by {@link StoreFileMetadata#isSame}: the same name, length
 * and checksum, and the same writer uuid when both files have one. Lengths and checksums are small made-up numbers.
 */
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
        final var cfs1 = file("_1.cfs", 2000, 12, randomWriterUuid());
        final var repository = repository(
            snapshot("snap-1", "state-1", file("_0.cfs", 1000, 11, randomWriterUuid())),
            snapshot("snap-2", "state-2", cfs1)
        );
        // There were no writes since snap-2, but a background merge rewrote the files into _2.cfs. The shard is still at state-2,
        // the state id of the latest snapshot, so the backlog is 0 without comparing any files.
        final var merged = commit("state-2", file("_2.cfs", 3000, 13, randomWriterUuid()));

        assertBacklog(repository, merged, 0);
        // Since the backlog compares no files, it needs no checksums either.
        assertThat(ShardBacklog.filesNeedingChecksum(baseline(repository, merged), "state-2", luceneFiles(merged)), empty());

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

    public void testACloneOfAnOlderSnapshotCountsUntilTheNextSnapshot() {
        for (boolean originalDeleted : List.of(false, true)) {
            final var repository = repositoryWithACloneOfAnOlderSnapshot(originalDeleted);
            // There were no writes since snap-2, but a background merge rewrote the files into _2.cfs, so the shard is still at
            // state-2. The snapshot finds snap-2 with that state id and uploads nothing.
            final var merged = commit("state-2", file("_2.cfs", 3000, 13, randomWriterUuid()));

            assertThat(uploadedBytes(repository, merged), equalTo(0L));
            // The clone is the newest snapshot, so the baseline keeps its state-1, not state-2. The shard's state-2 doesn't match,
            // so the backlog compares files and counts _2.cfs, which no snapshot has. This is the one known over-count. The backlog
            // and the snapshot disagree here, so this checks each one on its own instead of using assertBacklog.
            assertThat(ShardBacklog.bytesToUpload(baseline(repository, merged), "state-2", luceneFiles(merged), Map.of()), equalTo(3000L));
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
        final var metadata = Map.of("_0.cfs", cfs0, "_1.cfs", cfs1);
        assertThat(ShardBacklog.bytesToUpload(outdated, "state-3", luceneFiles(shard), metadata), equalTo(500L));
        // A new baseline counts the right amount.
        assertBacklog(afterDeletion, shard, 2500);
    }

    public void testARestoredSnapshotMatchesByItsFiles() {
        final var cfs0 = file("_0.cfs", 1000, 11, randomWriterUuid());
        final var repository = repository(
            snapshot("snap-1", "history-1-na-5", cfs0),
            snapshot("snap-2", "history-1-na-9", file("_1.cfs", 2000, 12, randomWriterUuid()))
        );
        // snap-1 is restored. The shard has snap-1's files again, but a restore starts a new history, so the state id is new.
        final var restored = commit("history-2-na-5", cfs0);

        assertBacklog(repository, restored, 0);
        // After the restore, a new file gets the name and length of snap-2's _1.cfs. Its writer uuid is different, so it is
        // another file, and the backlog counts it.
        assertBacklog(repository, restored, commit("history-2-na-7", cfs0, file("_1.cfs", 2000, 12, randomWriterUuid())), 2000);
    }

    public void testARecreatedIndexWithTheSameFileNamesIsCounted() {
        final var repository = repository(
            snapshot("snap-1", "state-1", file("_0.cfs", 1000, 11, randomWriterUuid()), file("_0.cfe", 100, 12, randomWriterUuid()))
        );
        // The index is deleted and created again under the same name. The new index starts again from _0, so its files have the
        // same names, and here the same lengths, but other contents.

        // Even the same checksum doesn't make the new _0.cfs the same file. The writer uuid is the id Lucene gives a segment when
        // it writes it, so another writer uuid means another segment, and the snapshot uploads the file again.
        assertBacklog(repository, commit("state-2", file("_0.cfs", 1000, 11, randomWriterUuid())), 1000);
        // Usually the checksums differ as well. They also tell apart a file that has no writer uuid, like the new _0.cfe.
        assertBacklog(
            repository,
            commit("state-2", file("_0.cfs", 1000, 21, randomWriterUuid()), file("_0.cfe", 100, 22, NO_WRITER_UUID)),
            1100
        );
    }

    public void testTheWriterUuidIsIgnoredWhenEitherSideHasNone() {
        // A file has no writer uuid if, for example, a version before 7.16 recorded it in the repository. The snapshot then
        // compares the other fields, the length, checksum and hash, and so does the backlog.
        final var repository = repository(
            snapshot("snap-1", null, file("_0.cfs", 1000, 11, randomWriterUuid()), file("_0.cfe", 100, 12, NO_WRITER_UUID))
        );

        assertBacklog(repository, commit(null, file("_0.cfs", 1000, 11, NO_WRITER_UUID)), 0);
        assertBacklog(repository, commit(null, file("_0.cfe", 100, 12, randomFrom(randomWriterUuid(), NO_WRITER_UUID))), 0);
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
        // This file has the name and length of a repository file, so the backlog needs its checksum to compare them, with or
        // without a writer uuid. Its checksum is different, so it is counted. The other files have another length or a new name,
        // so they are counted without a checksum. _0.si is kept in the metadata, so it needs no checksum and is not counted.
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
        final var baseline = baseline(repository, commit);
        // The backlog works in two steps, because getting a file's checksum means reading the end of the file. In the first step,
        // filesNeedingChecksum says which of the shard's files it can't judge from their name and length alone. Here that is
        // _0.cfs, because the repository has a file with the same name and length. Only the rest of the file's metadata, such as
        // its checksum and writer uuid, can tell whether it is the same file.
        assertThat(ShardBacklog.filesNeedingChecksum(baseline, null, luceneFiles(commit)), equalTo(List.of(luceneFile(cfs))));

        // In the second step, the caller reads the metadata of those files, with their checksums, and passes it to bytesToUpload.
        // If the caller leaves a file out, that is a bug in the caller. Counting the file anyway, or not counting it, could give a
        // wrong number and hide the bug, so bytesToUpload throws an exception instead.
        final var missing = expectThrows(
            IllegalArgumentException.class,
            () -> ShardBacklog.bytesToUpload(baseline, null, luceneFiles(commit), Map.of())
        );
        assertThat(missing.getMessage(), containsString("[_0.cfs]"));

        // Passing the metadata of a different file is also a bug in the caller, and bytesToUpload throws for it too. Here the
        // metadata passed for _0.cfs is either of a file with another length, or of a file with another name.
        final var otherFile = randomFrom(file("_0.cfs", 2000, 11, randomWriterUuid()), file("_1.cfs", 1000, 11, randomWriterUuid()));
        final var wrong = expectThrows(
            IllegalArgumentException.class,
            () -> ShardBacklog.bytesToUpload(baseline, null, luceneFiles(commit), Map.of("_0.cfs", otherFile))
        );
        assertThat(wrong.getMessage(), containsString("is not the metadata of"));
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

    public void testARepositoryWithoutWriterUuidsCanCountAFileTheSnapshotWouldReuse() {
        // These files were recorded by a version before 7.16, which kept no writer uuids. Serverless repositories are all newer
        // than that.
        final var repository = repository(
            snapshot("snap-1", null, file("_0.cfs", 1000, 11, NO_WRITER_UUID), file("_1.cfs", 2000, 12, NO_WRITER_UUID))
        );
        // The index is created again under the same name. Its first commit has only _0.cfs, so the baseline leaves out the old
        // _1.cfs.
        final var newCfs = file("_0.cfs", 500, 21, randomWriterUuid());
        final var baseline = baseline(repository, commit(null, newCfs));
        // Later, by chance, the new _1.cfs has the length and checksum of the old one. The old file has no writer uuid, so isSame
        // can't tell the two files apart, and the snapshot reuses the old file. The baseline no longer has it, so the backlog
        // counts it: 2500 instead of 500.
        final var later = commit(null, newCfs, file("_1.cfs", 2000, 12, randomWriterUuid()));

        assertThat(uploadedBytes(repository, later), equalTo(500L));
        assertThat(ShardBacklog.bytesToUpload(baseline, null, luceneFiles(later), Map.of()), equalTo(2500L));
    }

    /**
     * Builds a repository the way snapshots do, from a random history of a shard. Commits are drawn from a few file names, each with a
     * few versions that often share their length and checksum, sometimes without writer uuid. The state id changes with new writes and
     * stays the same when only the files change, as after a merge. A snapshot taken, or a commit checked, before the shard settled has
     * no state id. Some histories clone older snapshots, and some snapshots are removed as retention does. After every step, and against
     * the shard-level metadata read back at the end, the backlog must never be below what the snapshot would upload, and must equal it
     * unless the latest state id came from a clone. The test also records snapshots without a state id when the shard has settled,
     * which serverless doesn't do. That only makes the test stricter.
     */
    public void testRandomHistoriesMatchWhatTheSnapshotWouldUpload() throws IOException {
        final var versions = randomFileVersions();
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
            assertAgainstWhatTheSnapshotWouldUpload(snapshots, checked, checked, isExact(snapshots, clones));
        }

        final var readBack = readBack(snapshots);
        for (int i = between(1, 3); i > 0; i--) {
            final var checked = maybeWithoutStateId(live);
            assertAgainstWhatTheSnapshotWouldUpload(readBack, checked, checked, isExact(readBack, clones));
        }
    }

    /**
     * Uses real Lucene commits, with deletes and merges, read the way a snapshot reads them
     * ({@link Store.MetadataSnapshot#loadFromIndexCommit}). After the first index, the index is created again and reuses the same file
     * names. As a node would, the test builds a baseline for the first commit of each index and again after each snapshot, and uses it
     * for the later commits until the next snapshot. The first index never gives a new file the name of a file the baseline left out.
     * The recreated index does, because it starts again from the same names. The test also checks an assumption of
     * {@link ShardBacklog}: only the files it leaves out by name, the .si and segments_N files, have a hash.
     */
    public void testLuceneCommitsMatchWhatTheSnapshotWouldUpload() throws IOException {
        var snapshots = BlobStoreIndexShardSnapshots.EMPTY;
        // Both indices use the same file format, compound or not, so that the recreated index can reach the same file names.
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
                        // Only the recreated index is sometimes force merged. The first index keeps its segments, so that the recreated
                        // index can reach their file names.
                        if (index == 1 && rarely()) {
                            writer.forceMerge(1);
                        }
                        writer.commit();
                        final var files = new ArrayList<StoreFileMetadata>();
                        for (StoreFileMetadata file : Store.MetadataSnapshot.loadFromIndexCommit(null, directory, logger)) {
                            // ShardBacklog relies on this: the files it leaves out by name, .si and segments_N, are exactly the
                            // files whose contents are kept as their hash, and the other files have no hash.
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
     * Checks the backlog against what the snapshot would upload. The baseline is built for {@code baselineCommit} and used for
     * {@code commit}, which is the same commit or a later one. Two things are checked:
     * <ul>
     *     <li>The backlog asks for the checksums of exactly the files it needs to compare, as worked out by
     *     {@link #expectedFilesNeedingChecksum}.</li>
     *     <li>Given the checksums of only those files, it never counts less than the snapshot would upload, and if {@code exact}, it
     *     counts exactly as much.</li>
     * </ul>
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
     * The files whose checksums the backlog should ask for, worked out independently of {@link ShardBaseline}. If the latest snapshot
     * with a state id has the commit's state id, there are none. Otherwise they are the commit's files that meet all three of these
     * conditions:
     * <ul>
     *     <li>The file is not a .si or segments_N file.</li>
     *     <li>The repository has a file of the same name.</li>
     *     <li>That repository file has the same length as the file of that name in the commit the baseline was built for.</li>
     * </ul>
     * The length of the file in the current commit isn't compared. Within one index, a file name always refers to the same file, so
     * that length is the same as in the baseline's commit.
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
     * Whether the backlog must equal what the snapshot would upload: when the latest state id did not come from a clone, which may have
     * put an older state id last.
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
     * The shard-level metadata as a node reads it back from the repository. It is written either with writer uuids, or without them as
     * a version before 7.16 would write it.
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
     * A few file names, each with one to three versions, as when an index is deleted and created again under the same name. The
     * versions of one name often have the same length and checksum, and sometimes no writer uuid, so that only the remaining checks
     * of {@link StoreFileMetadata#isSame} tell them apart. Each writer uuid belongs to one version only, so equal writer uuids mean
     * equal files.
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
        return new LuceneFile(file.name(), file.length());
    }

    private static List<LuceneFile> luceneFiles(Commit commit) {
        return commit.files().stream().map(ShardBacklogTests::luceneFile).toList();
    }
}
