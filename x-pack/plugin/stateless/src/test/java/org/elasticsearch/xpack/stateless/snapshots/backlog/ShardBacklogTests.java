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
import org.elasticsearch.xpack.stateless.snapshots.backlog.ShardBacklog.CommitFile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.repositories.blobstore.BlobStoreRepository.INDEX_SHARD_SNAPSHOTS_FORMAT;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

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

    public void testAStateIdOfAnyRetainedSnapshotMeansNothingToUpload() {
        final var repository = repository(
            snapshot("snap-1", "state-1", file("_0.cfs", 1000, 11, randomWriterUuid())),
            snapshot("snap-2", "state-2", file("_1.cfs", 2000, 12, randomWriterUuid()))
        );
        for (String stateId : List.of("state-1", "state-2")) {
            // e.g. merged since the snapshot without any new writes: other files, same state
            final var mergedFiles = commit(stateId, file("_2.cfs", 3000, 13, randomWriterUuid()));

            assertBacklog(repository, mergedFiles, 0);
            assertThat(ShardBacklog.filesNeedingChecksum(baseline(repository, mergedFiles), stateId, commitFiles(mergedFiles)), empty());
        }
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

    public void testFilesKeptInTheShardLevelMetadataAreNotCounted() {
        final var cfs = file("_1.cfs", 1000, 11, randomWriterUuid());
        final var repository = repository(snapshot("snap-1", null, inlinedFile("_0.si"), inlinedFile("segments_2")));

        // new and changed .si and segments_N files are written into the shard-level metadata, never uploaded
        assertBacklog(repository, commit(null, inlinedFile("_0.si"), inlinedFile("_1.si"), inlinedFile("segments_3"), cfs), 1000);
    }

    public void testAFileOfAnyRetainedSnapshotMatches() {
        final var older = file("_0.cfs", 1000, 11, randomWriterUuid());
        final var newer = file("_0.cfs", 2000, 12, randomWriterUuid());
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

        final var needingChecksum = ShardBacklog.filesNeedingChecksum(baseline(repository, commit), null, commitFiles(commit));

        assertThat(needingChecksum, equalTo(List.of(commitFile(sameNameAndLength))));
        assertBacklog(repository, commit, 2200);
    }

    public void testMissingOrWrongMetadataIsRejected() {
        final var cfs = file("_0.cfs", 1000, 11, randomWriterUuid());
        final var repository = repository(snapshot("snap-1", null, cfs));
        final var commit = commit(null, cfs);

        final var missing = expectThrows(
            IllegalArgumentException.class,
            () -> ShardBacklog.bytesToUpload(baseline(repository, commit), null, commitFiles(commit), Map.of())
        );
        assertThat(missing.getMessage(), containsString("[_0.cfs]"));

        final var otherFile = randomFrom(file("_0.cfs", 2000, 11, randomWriterUuid()), file("_1.cfs", 1000, 11, randomWriterUuid()));
        final var wrong = expectThrows(
            IllegalArgumentException.class,
            () -> ShardBacklog.bytesToUpload(baseline(repository, commit), null, commitFiles(commit), Map.of("_0.cfs", otherFile))
        );
        assertThat(wrong.getMessage(), containsString("is not the metadata of"));
    }

    public void testARepositoryChecksumWrittenOtherwiseNeverMatches() {
        // all of them are 11, which Store#digestToString writes "b", but isSame compares the strings
        final var repository = repository(
            snapshot(
                "snap-1",
                null,
                new StoreFileMetadata("_0.cfs", 1000, randomFrom("0b", "B"), Version.LATEST.toString(), null, NO_WRITER_UUID)
            )
        );

        assertBacklog(repository, commit(null, file("_0.cfs", 1000, 11, NO_WRITER_UUID)), 1000);
    }

    public void testARepositoryFileWithAPartialHashNeverMatches() {
        // a hash that is not the whole file: isSame compares it, and a commit file a snapshot uploads has none
        final var withHash = new StoreFileMetadata(
            "_0.cfs",
            1000,
            Store.digestToString(11),
            Version.LATEST.toString(),
            new BytesRef(randomByteArrayOfLength(between(1, 100))),
            NO_WRITER_UUID
        );
        final var repository = repository(snapshot("snap-1", null, withHash));

        assertBacklog(repository, commit(null, file("_0.cfs", 1000, 11, NO_WRITER_UUID)), 1000);
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

    public void testARecreatedIndexCanCountAFileTheSnapshotWouldReuse() {
        // the old index's files, recorded without writer uuid as older versions did
        final var repository = repository(
            snapshot("snap-1", null, file("_0.cfs", 1000, 11, NO_WRITER_UUID), file("_1.cfs", 2000, 12, NO_WRITER_UUID))
        );
        // the index is created again under the same name, and the baseline for its first commit leaves out the old _1.cfs
        final var newCfs = file("_0.cfs", 500, 21, randomWriterUuid());
        final var baseline = baseline(repository, commit(null, newCfs));
        // by chance its new _1.cfs has the length and checksum of the old one, which the snapshot would then reuse
        final var later = commit(null, newCfs, file("_1.cfs", 2000, 12, randomWriterUuid()));

        assertThat(uploadedBytes(repository, later), equalTo(500L));
        assertThat(ShardBacklog.bytesToUpload(baseline, null, commitFiles(later), Map.of()), equalTo(2500L));
    }

    /**
     * Builds a repository the way snapshots do, from a history of random commits of a few file names, each with a few versions that
     * often share their length and checksum, sometimes without writer uuid, plus files only older versions write (a hash, a checksum
     * written otherwise). Then removes some of the snapshots as retention does and reads the shard-level metadata back as a node would.
     * A random commit is checked after every snapshot and a few at the end. These commits are not those of one Lucene index, so each
     * check builds the baseline for the commit it checks.
     */
    public void testRandomHistoriesMatchWhatTheSnapshotWouldUpload() throws IOException {
        final var versions = randomFileVersions();
        final var stateIds = List.of("state-1", "state-2", "state-3");

        var snapshots = BlobStoreIndexShardSnapshots.EMPTY;
        final List<SnapshotId> snapshotIds = new ArrayList<>();
        for (int i = between(0, 8); i > 0; i--) {
            final var snapshotId = new SnapshotId("snap-" + snapshotIds.size(), randomUUID());
            final var commit = randomCommit(versions, stateIds);
            snapshots = snapshotShard(snapshots, snapshotId.getName(), randomBoolean() ? commit : withOlderVersionFiles(commit));
            snapshotIds.add(snapshotId);
            assertMatchesWhatTheSnapshotWouldUpload(snapshots, randomCommit(versions, stateIds));
        }
        if (snapshotIds.isEmpty() == false && randomBoolean()) {
            snapshots = snapshots.withRetainedSnapshots(Set.copyOf(randomSubsetOf(snapshotIds)));
        }

        final var readBack = readBack(snapshots);
        for (int i = between(1, 5); i > 0; i--) {
            assertMatchesWhatTheSnapshotWouldUpload(readBack, randomCommit(versions, stateIds));
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

                        assertMatchesWhatTheSnapshotWouldUpload(snapshots, baselineCommit, commit);
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
        return ShardBaseline.of(snapshots, commitFiles(commit));
    }

    private static void assertBacklog(BlobStoreIndexShardSnapshots snapshots, Commit commit, long expectedBytes) {
        assertBacklog(snapshots, commit, commit, expectedBytes);
    }

    private static void assertBacklog(BlobStoreIndexShardSnapshots snapshots, Commit baselineCommit, Commit commit, long expectedBytes) {
        assertThat("what the snapshot would upload", uploadedBytes(snapshots, commit), equalTo(expectedBytes));
        assertMatchesWhatTheSnapshotWouldUpload(snapshots, baselineCommit, commit);
    }

    private static void assertMatchesWhatTheSnapshotWouldUpload(BlobStoreIndexShardSnapshots snapshots, Commit commit) {
        assertMatchesWhatTheSnapshotWouldUpload(snapshots, commit, commit);
    }

    /**
     * Checks a baseline built for one commit, used for that commit or a later one, against the snapshot's own matching
     * ({@link BlobStoreIndexShardSnapshots#findPhysicalIndexFile}): it must ask for the checksums of exactly the files that have a
     * repository file of the same name and length that it kept, and given only those, it must count what the snapshot would upload.
     */
    private static void assertMatchesWhatTheSnapshotWouldUpload(
        BlobStoreIndexShardSnapshots snapshots,
        Commit baselineCommit,
        Commit commit
    ) {
        final var baseline = baseline(snapshots, baselineCommit);
        final var needingChecksum = ShardBacklog.filesNeedingChecksum(baseline, commit.stateId(), commitFiles(commit));
        assertThat("files needing a checksum", needingChecksum, equalTo(filesOfTheSameNameAndLength(snapshots, baselineCommit, commit)));
        final Map<String, StoreFileMetadata> metadata = new HashMap<>();
        for (StoreFileMetadata file : commit.files()) {
            if (needingChecksum.contains(commitFile(file))) {
                metadata.put(file.name(), file);
            }
        }
        assertThat(
            "bytes to upload",
            ShardBacklog.bytesToUpload(baseline, commit.stateId(), commitFiles(commit), metadata),
            equalTo(uploadedBytes(snapshots, commit))
        );
    }

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
     * The commit files a snapshot would upload unless the repository holds them, and that have a repository file of the same name and
     * length, among those named in the commit the baseline was built for. None if a snapshot has the commit's state id.
     */
    private static List<CommitFile> filesOfTheSameNameAndLength(
        BlobStoreIndexShardSnapshots snapshots,
        Commit baselineCommit,
        Commit commit
    ) {
        final Set<String> kept = new HashSet<>();
        for (StoreFileMetadata file : baselineCommit.files()) {
            kept.add(file.name());
        }
        if (commit.stateId() != null && snapshots.snapshots().stream().anyMatch(s -> commit.stateId().equals(s.shardStateIdentifier()))) {
            return List.of();
        }
        final List<CommitFile> files = new ArrayList<>();
        for (StoreFileMetadata file : commit.files()) {
            if (file.hashEqualsContents() == false
                && snapshots.snapshots()
                    .stream()
                    .flatMap(snapshot -> snapshot.indexFiles().stream())
                    .anyMatch(
                        held -> kept.contains(held.physicalName())
                            && held.physicalName().equals(file.name())
                            && held.length() == file.length()
                    )) {
                files.add(commitFile(file));
            }
        }
        return files;
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

    private static Commit randomCommit(Map<String, List<StoreFileMetadata>> versions, List<String> stateIds) {
        final List<StoreFileMetadata> files = new ArrayList<>();
        for (List<StoreFileMetadata> fileVersions : versions.values()) {
            if (randomBoolean()) {
                files.add(randomFrom(fileVersions));
            }
        }
        return new Commit(randomBoolean() ? null : randomFrom(stateIds), files);
    }

    /**
     * The commit with some of its uploaded files as older Elasticsearch versions may have recorded them in the repository: with a hash
     * that is not the whole file, or with the checksum written with a leading zero or in upper case, and without writer uuid, which
     * older versions did not record. No commit file looks like that today, so these only appear in the repository.
     */
    private static Commit withOlderVersionFiles(Commit commit) {
        final List<StoreFileMetadata> files = new ArrayList<>();
        for (StoreFileMetadata file : commit.files()) {
            if (file.hashEqualsContents() || randomBoolean()) {
                files.add(file);
            } else {
                final boolean withHash = randomBoolean();
                files.add(
                    new StoreFileMetadata(
                        file.name(),
                        file.length(),
                        withHash ? file.checksum() : randomFrom("0" + file.checksum(), file.checksum().toUpperCase(Locale.ROOT)),
                        file.writtenBy(),
                        withHash ? new BytesRef(randomByteArrayOfLength(between(1, 100))) : null,
                        NO_WRITER_UUID
                    )
                );
            }
        }
        return new Commit(commit.stateId(), files);
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

    private static CommitFile commitFile(StoreFileMetadata file) {
        return new CommitFile(file.name(), file.length());
    }

    private static List<CommitFile> commitFiles(Commit commit) {
        return commit.files().stream().map(ShardBacklogTests::commitFile).toList();
    }
}
