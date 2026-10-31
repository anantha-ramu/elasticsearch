/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots.backlog;

import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshots;
import org.elasticsearch.index.snapshots.blobstore.SnapshotFiles;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A summary of what a repository has already backed up for one shard, used to work out how many bytes a snapshot of the shard's latest
 * commit would upload ({@link #bytesToUpload}). It follows the same rules as the snapshot itself ({@code BlobStoreRepository
 * #doSnapshotShard}):
 * <ul>
 *     <li>if the shard hasn't changed since the latest snapshot (it has the same state id), nothing is uploaded;</li>
 *     <li>otherwise every file the repository doesn't already have is uploaded, except the {@code .si} and {@code segments_N} files,
 *     whose contents a snapshot stores in the backup metadata itself.</li>
 * </ul>
 * <p>
 * <b>How a file counts as already backed up.</b> The snapshot asks {@link StoreFileMetadata#isSame}, which looks at the length, the
 * checksum and the writer uuid of the two files. A writer uuid is a random id that Lucene gives the files it writes ({@link #luceneFiles}
 * says which). When both files have one and they differ, {@code isSame} says the files are different. When they are equal, it treats
 * them as the same file: it asserts that their lengths and checksums are equal too. So here a file counts as backed up if the repository
 * has a file with the same name and writer uuid. The checksum is never needed, which matters because reading it means reading the end
 * of the file. A file without a writer uuid, in the shard or in the repository, is counted as not backed up.
 * <p>
 * <b>What it keeps.</b> To build it, we read the shard's backup metadata from the repository ({@code index-{generation}}, a
 * {@link BlobStoreIndexShardSnapshots}) and compare it with the files of the shard's latest commit, which we call the build commit. We
 * keep two things:
 * <ul>
 *     <li>For each file of the build commit that the repository has, with the same name and writer uuid, that writer uuid.</li>
 *     <li>The state id of the latest snapshot that has one. If the shard still has that state id, its data hasn't changed since that
 *     snapshot, even if a background merge rewrote its files, and the next snapshot has nothing to upload.</li>
 * </ul>
 * <p>
 * <b>When it stays valid.</b> A baseline gives the right answer for the build commit and every later commit of the shard: new files are
 * simply counted as not backed up yet. It only goes out of date when the repository changes, that is, when a snapshot is taken, deleted
 * or cloned. From then until a new baseline is built, its answer shouldn't be used, because it can be wrong either way: too high after a
 * new snapshot, or too low if a deleted snapshot held some of the shard's files.
 * <p>
 * <b>Why so little is enough.</b> The baseline holds at most one entry per file of the build commit, however many snapshots are kept.
 * Three facts make this enough:
 * <ul>
 *     <li>Within one shard (each shard is its own Lucene index), Lucene never changes a file once written, and doesn't reuse a file
 *     name. (A new primary can reuse the name of a segment that the old primary wrote but never uploaded. That file was never backed
 *     up, and the new one gets a new writer uuid, so they can't be mixed up.)</li>
 *     <li>A writer uuid never comes back once it is replaced, because Lucene draws a new random one each time. The repository only holds
 *     snapshots of commits up to the build commit. So if a later commit has a file whose name and writer uuid the repository has, the
 *     build commit had that same file with that same writer uuid, and the baseline kept it.</li>
 *     <li>The state id only moves forward. It is made of the shard's history uuid, a force merge id and the highest sequence number.
 *     Within one history the sequence number only grows, and the force merge id only changes to a new random value. So once the
 *     shard has moved past a snapshot's state id, it never has it again. Snapshots are recorded one at a time, in the order they were
 *     taken, so the last state id in the metadata is the newest one, and the only one worth keeping.</li>
 * </ul>
 * These events give the same result as the snapshot:
 * <ul>
 *     <li><b>Snapshots are taken or deleted.</b> The facts above still hold.</li>
 *     <li><b>A snapshot is taken while writes are in flight.</b> It records no state id, so we keep the one from the snapshot before
 *     it. The shard has moved past that state id by then, so it doesn't match, and the backlog and the snapshot both compare
 *     files.</li>
 *     <li><b>The shard's metadata file goes missing and is rebuilt.</b> Elasticsearch rebuilds it from the per-snapshot metadata files,
 *     and the rebuilt copy has no state ids. Then neither the snapshot nor the backlog can use the "nothing changed" shortcut, and
 *     both compare files.</li>
 *     <li><b>A segment changes after it was written</b>, because documents in it were deleted or updated. Lucene writes new files for
 *     the change, and gives all of the segment's change files a new writer uuid, including ones it keeps, such as the file of deleted
 *     documents after a doc values update. The snapshot then uploads those files again, and they are counted.</li>
 *     <li><b>A snapshot is restored.</b> The shard gets a new history uuid, so no old state id can match. The restored files keep their
 *     writer uuids, so they match their backed-up copies. Files written after the restore may reuse old names, but they have new
 *     writer uuids, so they don't match.</li>
 *     <li><b>An index is deleted and created again under the same name.</b> This works like a restore: the same folder in the
 *     repository, but a new history. Its new files can reuse old names, lengths and even checksums, but they have new writer uuids,
 *     so they don't match.</li>
 *     <li><b>A primary relocates or fails over</b> in stateless. The history stays the same, the new primary starts from the latest
 *     uploaded commit, and no snapshot is of a newer commit.</li>
 *     <li><b>The index is resharded.</b> Each new shard gets its own folder in the repository, and any files an earlier index of the
 *     same name left there are handled as for a recreated index. The original shard gets a new state id when it deletes the
 *     documents that moved.</li>
 * </ul>
 * With an up-to-date baseline, the backlog never counts less than a snapshot of the same commit would upload, as long as two different
 * files never get the same name and writer uuid. Elasticsearch assumes the same (the assertion in {@code isSame}), and with random
 * 128-bit ids it doesn't happen in practice. If it did, {@code isSame} would still tell the files apart by their checksums, and we
 * wouldn't. The backlog counts more than the snapshot uploads in two known cases:
 * <ul>
 *     <li><b>An older snapshot is cloned.</b> The clone copies the old snapshot's state id to the end of the list, so we no longer
 *     keep the newest one. This only matters if the shard has had no new writes but a background merge rewrote its files: the
 *     snapshot would see the state id is unchanged and upload nothing, while we count the merged files. The next snapshot puts the
 *     newest state id back at the end.</li>
 *     <li><b>A file has no writer uuid</b>, in the shard or in the repository. The snapshot may still find it backed up by its
 *     checksum, while we count it. Lucene has given every file a writer uuid since 8.6, and Elasticsearch has recorded it for every
 *     file it backed up since 7.16, so this can't happen in serverless.</li>
 * </ul>
 * A snapshot also reports the {@code .si} and {@code segments_N} files it adds in its incremental size, so that size can be a little
 * more than this. A real snapshot also flushes the shard first, so it also uploads writes that aren't in a commit yet.
 * <p>
 * This assumes every snapshot in the metadata is of a commit at or before the build commit (see {@link #of}), and that the caller
 * builds a new baseline whenever the metadata changes.
 */
final class ShardBaseline implements Accountable {

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(ShardBaseline.class);
    private static final long BYTES_REF_SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(BytesRef.class);
    // Map.copyOf puts each key and value in its own slot of one array, and keeps that array half empty, so each entry takes four
    // slots. This is a JDK detail. If it changes, the memory test fails.
    private static final long MAP_SLOTS_PER_ENTRY = 4;

    static final ShardBaseline EMPTY = of(BlobStoreIndexShardSnapshots.EMPTY, List.of());

    /**
     * A file of one of the shard's commits: its name, its length, and its writer uuid, which is empty if the file has none. A commit
     * lists each name once.
     */
    record LuceneFile(String name, long length, BytesRef writerUuid) {
        LuceneFile {
            Objects.requireNonNull(name);
            Objects.requireNonNull(writerUuid);
        }
    }

    // The writer uuid of each file of the build commit that the repository has, by file name.
    private final Map<String, BytesRef> backedUpWriterUuids;
    @Nullable
    private final String latestStateId;
    private final long ramBytesUsed;

    private ShardBaseline(Map<String, BytesRef> backedUpWriterUuids, @Nullable String latestStateId) {
        this.backedUpWriterUuids = backedUpWriterUuids;
        this.latestStateId = latestStateId;
        this.ramBytesUsed = ramBytesUsed(backedUpWriterUuids, latestStateId);
    }

    /**
     * The files of a commit, with the writer uuids a snapshot would give them ({@link Store.MetadataSnapshot#loadFromIndexCommit}).
     * All of them come from the commit's {@link SegmentInfos}: a segment's own files get the segment's id, the files that record later
     * changes to the segment get the id of its latest change, and the {@code segments_N} file gets the commit's id. The lengths come from
     * the directory, which knows them without reading the files.
     *
     * @param segmentInfos the commit's segment infos
     * @param directory    the directory of the commit's files
     */
    static List<LuceneFile> luceneFiles(SegmentInfos segmentInfos, Directory directory) throws IOException {
        final List<LuceneFile> files = new ArrayList<>();
        for (SegmentCommitInfo segment : segmentInfos) {
            final BytesRef segmentWriterUuid = writerUuid(segment.info.getId());
            final BytesRef changesWriterUuid = writerUuid(segment.getId());
            for (String name : segment.files()) {
                final BytesRef writerUuid = IndexFileNames.parseGeneration(name) == 0 ? segmentWriterUuid : changesWriterUuid;
                files.add(new LuceneFile(name, directory.fileLength(name), writerUuid));
            }
        }
        final String segmentsFileName = segmentInfos.getSegmentsFileName();
        files.add(new LuceneFile(segmentsFileName, directory.fileLength(segmentsFileName), writerUuid(segmentInfos.getId())));
        return files;
    }

    private static BytesRef writerUuid(@Nullable byte[] id) {
        // Lucene before 8.6 gave a segment's changes no id.
        return id == null ? StoreFileMetadata.UNAVAILABLE_WRITER_UUID : new BytesRef(id);
    }

    /**
     * Builds a baseline from the shard's backup metadata and the files of one of the shard's commits, the build commit. Every
     * snapshot in the metadata must be of a commit at or before the build commit, which holds when the metadata is the blob of a
     * generation that was known before the commit was taken: a generation's blob is written in full before the generation is
     * published, and never changes. Otherwise a snapshot newer than the build commit could hold files the baseline leaves out.
     *
     * @param snapshots    the shard's backup metadata
     * @param currentFiles the files of the build commit, from {@link #luceneFiles}
     */
    static ShardBaseline of(BlobStoreIndexShardSnapshots snapshots, Collection<LuceneFile> currentFiles) {
        final Builder builder = new Builder(currentFiles);
        for (SnapshotFiles snapshot : snapshots) {
            final List<String> blobNames = new ArrayList<>(snapshot.indexFiles().size());
            for (BlobStoreIndexShardSnapshot.FileInfo backedUpFile : snapshot.indexFiles()) {
                builder.addFile(backedUpFile);
                blobNames.add(backedUpFile.name());
            }
            builder.addSnapshot(blobNames, snapshot.shardStateIdentifier());
        }
        return builder.build();
    }

    /**
     * Builds a baseline while the shard's backup metadata is read one entry at a time, so that only what the baseline keeps is held,
     * however many snapshots the metadata has. The metadata lists each backed-up file once ({@link #addFile}), and then each snapshot
     * in the order they were taken, with the files it refers to ({@link #addSnapshot}). A backed-up file is kept only once a snapshot
     * refers to it, so a file the metadata lists but no snapshot holds can't make a file look backed up. The same rules apply as for
     * {@link #of}.
     */
    static final class Builder {
        // The files a snapshot could find in the repository, by name.
        private final Map<String, LuceneFile> currentFilesByName = new HashMap<>();
        // Backed-up files with the name and writer uuid of a current file, by blob name, until a snapshot refers to them.
        private final Map<String, LuceneFile> matchingBlobs = new HashMap<>();
        private final Map<String, LuceneFile> backedUpFiles = new HashMap<>();
        @Nullable
        private String latestStateId;

        Builder(Collection<LuceneFile> currentFiles) {
            for (LuceneFile file : currentFiles) {
                // A snapshot never looks for .si and segments_N files, because it keeps their contents in the metadata itself, and a
                // file without a writer uuid is always counted.
                if (isStoredInBackupMetadata(file) == false && file.writerUuid().length > 0) {
                    currentFilesByName.put(file.name(), file);
                }
            }
        }

        /**
         * A backed-up file, as the metadata lists it.
         */
        void addFile(BlobStoreIndexShardSnapshot.FileInfo backedUpFile) {
            final LuceneFile currentFile = currentFilesByName.get(backedUpFile.physicalName());
            if (currentFile != null && currentFile.writerUuid().equals(backedUpFile.metadata().writerUuid())) {
                matchingBlobs.put(backedUpFile.name(), currentFile);
            }
        }

        /**
         * A snapshot, in the order the metadata lists them, which is the order they were taken.
         *
         * @param blobNames the names of the blobs of the files the snapshot holds ({@link BlobStoreIndexShardSnapshot.FileInfo#name})
         * @param stateId   the snapshot's state id, or {@code null} if it has none
         */
        void addSnapshot(Collection<String> blobNames, @Nullable String stateId) {
            if (stateId != null) {
                latestStateId = stateId;
            }
            for (String blobName : blobNames) {
                final LuceneFile currentFile = matchingBlobs.get(blobName);
                if (currentFile != null) {
                    backedUpFiles.put(currentFile.name(), currentFile);
                }
            }
        }

        ShardBaseline build() {
            final Map<String, BytesRef> backedUpWriterUuids = new HashMap<>();
            for (LuceneFile file : backedUpFiles.values()) {
                // A copy, so that the baseline owns exactly the bytes it accounts for.
                backedUpWriterUuids.put(file.name(), BytesRef.deepCopyOf(file.writerUuid()));
            }
            return new ShardBaseline(Map.copyOf(backedUpWriterUuids), latestStateId);
        }
    }

    /**
     * The bytes a snapshot of the commit would upload, if the commit is the build commit or a later commit of the shard.
     *
     * @param currentStateId the commit's state id as a snapshot would work it out ({@code SnapshotShardsService#getShardStateId}), or
     *                       {@code null} while writes are still in flight
     * @param currentFiles   the commit's files, from {@link #luceneFiles}
     */
    long bytesToUpload(@Nullable String currentStateId, Collection<LuceneFile> currentFiles) {
        if (currentStateId != null && currentStateId.equals(latestStateId)) {
            return 0;
        }
        long bytes = 0;
        for (LuceneFile file : currentFiles) {
            if (isStoredInBackupMetadata(file) == false && isBackedUp(file) == false) {
                bytes += file.length();
            }
        }
        return bytes;
    }

    /**
     * Whether the repository has this file. Every kept writer uuid is non-empty, so a file without one is never backed up.
     */
    private boolean isBackedUp(LuceneFile file) {
        return file.writerUuid().equals(backedUpWriterUuids.get(file.name()));
    }

    /**
     * Whether a snapshot stores this file's contents inside the backup metadata instead of uploading it, which is true for the
     * {@code .si} and {@code segments_N} files. (The snapshot itself checks {@link StoreFileMetadata#hashEqualsContents}, which is true
     * for exactly these files.)
     */
    private static boolean isStoredInBackupMetadata(LuceneFile file) {
        return Store.MetadataSnapshot.isReadAsHash(file.name());
    }

    @Override
    public long ramBytesUsed() {
        return ramBytesUsed;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof ShardBaseline other
            && backedUpWriterUuids.equals(other.backedUpWriterUuids)
            && Objects.equals(latestStateId, other.latestStateId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(backedUpWriterUuids, latestStateId);
    }

    private static long ramBytesUsed(Map<String, BytesRef> backedUpWriterUuids, @Nullable String latestStateId) {
        long bytes = SHALLOW_SIZE + RamUsageEstimator.sizeOf(latestStateId);
        bytes += sizeOfMap(backedUpWriterUuids);
        for (Map.Entry<String, BytesRef> entry : backedUpWriterUuids.entrySet()) {
            bytes += RamUsageEstimator.sizeOf(entry.getKey());
            bytes += BYTES_REF_SHALLOW_SIZE + RamUsageEstimator.sizeOf(entry.getValue().bytes);
        }
        return bytes;
    }

    /**
     * The size of a map made by {@link Map#copyOf}: the object, its table of references, and one extra array header. The extra header
     * isn't real memory. It's there because {@code RamUsageTester}, which the tests compare this estimate with, counts a collection
     * that way, and without it a very small baseline would come out below the test's figure.
     */
    private static long sizeOfMap(Map<String, BytesRef> map) {
        final long table = RamUsageEstimator.alignObjectSize(
            RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + MAP_SLOTS_PER_ENTRY * map.size() * RamUsageEstimator.NUM_BYTES_OBJECT_REF
        );
        return RamUsageEstimator.shallowSizeOf(map) + table + RamUsageEstimator.NUM_BYTES_ARRAY_HEADER;
    }
}
