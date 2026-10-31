/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots.backlog;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshots;
import org.elasticsearch.index.snapshots.blobstore.SnapshotFiles;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A summary of what a repository has already backed up for one shard. {@link ShardBacklog} compares it with the shard's files to
 * work out how much a snapshot of the shard's latest commit would upload.
 * <p>
 * To build it, we read the shard's backup metadata from the repository ({@code index-{generation}}, a
 * {@link BlobStoreIndexShardSnapshots}) and compare it with the files the shard has right now. We keep two things:
 * <ul>
 *     <li>For each of the shard's current files, the repository's record of any backed-up file with the same name and length (its
 *     length, checksum, writer uuid and so on), never the file's contents. {@link StoreFileMetadata#isSame} uses this record to tell
 *     whether the backed-up file really is the same file, just as the snapshot does.</li>
 *     <li>The state id from the latest snapshot. If the shard still has the same state id, its data hasn't changed since that
 *     snapshot, even if a background merge rewrote its files, and the next snapshot has nothing to upload.</li>
 * </ul>
 * <p>
 * A baseline stays correct as the shard writes new files: they are simply counted as not backed up yet. It only goes out of date when
 * the repository changes, that is, when a snapshot is taken, deleted or cloned. From then until a new baseline is built, the backlog
 * worked out from it shouldn't be used, because it can be wrong either way: too high after a new snapshot, or too low if a deleted
 * snapshot held some of the shard's files.
 * <p>
 * The baseline stays small, about the size of the shard's file list, no matter how many snapshots are kept. The only extra entries are
 * old files with matching names and lengths, left behind when an index was deleted and recreated under the same name. Two rules make
 * this possible:
 * <ul>
 *     <li>Within one shard (each shard is its own Lucene index), Lucene never reuses a file name and never changes a file once
 *     written. So a file that was merged away never comes back, and a file the shard keeps always has the same length.</li>
 *     <li>The state id only moves forward. It is made of the shard's history uuid, a force merge id and the highest sequence number.
 *     Within one history the sequence number only grows, and the force merge id only changes to a new random value. So once the
 *     shard has moved past a snapshot's state id, it never has it again. Snapshots are recorded one at a time, in the order they were
 *     taken, so the last state id in the metadata is the newest one, and the only one worth keeping.</li>
 * </ul>
 * These events are handled exactly, with the same result as the snapshot:
 * <ul>
 *     <li><b>Snapshots are taken or deleted.</b> Both rules still hold.</li>
 *     <li><b>A snapshot is taken while writes are in flight.</b> It records no state id, so we keep the one from the snapshot before
 *     it. The shard has moved past that state id by then, so it doesn't match, and the backlog and the snapshot both compare
 *     files.</li>
 *     <li><b>The shard's metadata file goes missing and is rebuilt.</b> Elasticsearch rebuilds it from the per-snapshot metadata files,
 *     and the rebuilt copy has no state ids. Then neither the snapshot nor the backlog can use the "nothing changed" shortcut, and
 *     both compare files.</li>
 *     <li><b>A snapshot is restored.</b> The shard gets a new history uuid, so no old state id can match. The restored files match
 *     their backed-up copies. Files written after the restore may reuse old names, but they have new writer uuids, so they don't
 *     match.</li>
 *     <li><b>An index is deleted and created again under the same name.</b> This works like a restore: the same folder in the
 *     repository, but a new history. Its new files can reuse old names, but they have new writer uuids, so they don't match. (Every
 *     serverless repository records writer uuids, as Elasticsearch has for every file it backed up since 7.16.)</li>
 *     <li><b>A primary relocates or fails over</b> in stateless. The history stays the same, the new primary starts from the latest
 *     uploaded commit, and no snapshot is of a newer commit.</li>
 *     <li><b>The index is resharded.</b> Each new shard gets its own folder in the repository, and any files an earlier index of the
 *     same name left there are handled as for a recreated index. The original shard gets a new state id when it deletes the
 *     documents that moved.</li>
 * </ul>
 * With an up-to-date baseline, there is one known case in serverless where the backlog counts more than a snapshot of the same
 * commit would upload, and none where it counts less. (A repository with files backed up before Elasticsearch 7.16 would add another, which a test pins
 * down.)
 * <ul>
 *     <li><b>An older snapshot is cloned.</b> The clone copies the old snapshot's state id to the end of the list, so we no longer
 *     keep the newest one. This only matters if the shard has had no new writes but a background merge rewrote its files: the
 *     snapshot would see the state id is unchanged and upload nothing, while we count the merged files. The next snapshot puts the
 *     newest state id back at the end.</li>
 * </ul>
 * This assumes the caller reads the metadata before looking at the shard's files (see {@link #of}) and builds a new baseline whenever
 * the metadata changes. Even if a shard somehow went back to an older state without a new history, the backlog could only count more
 * than the snapshot would upload, never less.
 */
final class ShardBaseline implements Accountable {

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(ShardBaseline.class);
    private static final long STORE_FILE_METADATA_SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(StoreFileMetadata.class);
    private static final long BYTES_REF_SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(BytesRef.class);
    // Map.copyOf puts each key and value in its own slot of one array, and keeps that array half empty, so each entry takes four
    // slots. This is a JDK detail. If it changes, the memory test fails.
    private static final long MAP_SLOTS_PER_ENTRY = 4;

    static final ShardBaseline EMPTY = of(BlobStoreIndexShardSnapshots.EMPTY, List.of());

    /**
     * A Lucene file the shard has now, by name and length, before its checksum is read. A commit lists each name once.
     */
    record LuceneFile(String name, long length) {
        LuceneFile {
            Objects.requireNonNull(name);
        }
    }

    // The backed-up files, by file name. A file that several snapshots share is kept once.
    private final Map<String, List<StoreFileMetadata>> backedUpFilesByName;
    @Nullable
    private final String latestStateId;
    private final long ramBytesUsed;

    private ShardBaseline(Map<String, List<StoreFileMetadata>> backedUpFilesByName, @Nullable String latestStateId) {
        this.backedUpFilesByName = backedUpFilesByName;
        this.latestStateId = latestStateId;
        this.ramBytesUsed = ramBytesUsed(backedUpFilesByName, latestStateId);
    }

    /**
     * Builds a baseline from the shard's backup metadata and the files the shard has right now. Read the metadata first and take the
     * files afterwards. If a snapshot finished in between, the metadata would include files newer than the ones passed in, and the
     * baseline would wrongly leave them out.
     *
     * @param snapshots   the shard's backup metadata, as read from the repository
     * @param currentFiles the files of the shard's latest commit
     */
    static ShardBaseline of(BlobStoreIndexShardSnapshots snapshots, Collection<LuceneFile> currentFiles) {
        // The name and length of each file a snapshot would look for in the repository. It never looks for .si and segments_N
        // files, because it keeps their contents in the metadata itself.
        final Map<String, Long> currentFileLengths = new HashMap<>();
        for (LuceneFile file : currentFiles) {
            if (isStoredInBackupMetadata(file) == false) {
                currentFileLengths.put(file.name(), file.length());
            }
        }
        // Keep a backed-up file only if it has the same name and length as one of the shard's files, since isSame never matches
        // files of different lengths. Keep it once, since all the snapshots that share a file refer to the same blob.
        final Set<String> seenBlobNames = new HashSet<>();
        final Map<String, List<StoreFileMetadata>> backedUpFilesByName = new HashMap<>();
        String latestStateId = null;
        // Snapshots are listed in the order they were taken, so the last state id we see is the newest.
        for (SnapshotFiles snapshot : snapshots) {
            if (snapshot.shardStateIdentifier() != null) {
                latestStateId = snapshot.shardStateIdentifier();
            }
            for (BlobStoreIndexShardSnapshot.FileInfo backedUpFile : snapshot.indexFiles()) {
                final Long currentLength = currentFileLengths.get(backedUpFile.physicalName());
                if (currentLength != null && currentLength == backedUpFile.length() && seenBlobNames.add(backedUpFile.name())) {
                    backedUpFilesByName.computeIfAbsent(backedUpFile.physicalName(), name -> new ArrayList<>())
                        .add(backedUpFile.metadata());
                }
            }
        }
        return new ShardBaseline(Map.copyOf(Maps.transformValues(backedUpFilesByName, List::copyOf)), latestStateId);
    }

    /**
     * Whether the given state id is the newest one in the shard's backup metadata. If it is, the shard's data hasn't changed since that
     * snapshot, and the next snapshot uploads nothing.
     */
    boolean isLatestStateId(@Nullable String stateId) {
        return stateId != null && stateId.equals(latestStateId);
    }

    /**
     * Whether the baseline has a backed-up file with this name. If not, the shard's file of that name isn't backed up yet. If it has
     * one, the file's checksum is needed, and {@link #isBackedUp} gives the answer. The name is enough, because the baseline only kept
     * files with the same length as the shard's, and a file never changes length.
     */
    boolean hasFileNamed(String name) {
        return backedUpFilesByName.containsKey(name);
    }

    /**
     * Whether the repository already has this file of the shard, that is, whether a snapshot would reuse a backed-up file instead of
     * uploading it. Like {@link BlobStoreIndexShardSnapshots#findPhysicalIndexFile}, it asks {@link StoreFileMetadata#isSame} about
     * each backed-up file of the same name.
     */
    boolean isBackedUp(StoreFileMetadata currentFile) {
        for (StoreFileMetadata backedUpFile : backedUpFilesByName.getOrDefault(currentFile.name(), List.of())) {
            if (backedUpFile.isSame(currentFile)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Whether a snapshot stores this file's contents inside the shard's backup metadata instead of uploading it, which is true for
     * the {@code .si} and {@code segments_N} files. (The snapshot itself checks {@link StoreFileMetadata#hashEqualsContents}, which
     * is true for exactly these files.)
     */
    static boolean isStoredInBackupMetadata(LuceneFile file) {
        return Store.MetadataSnapshot.isReadAsHash(file.name());
    }

    @Override
    public long ramBytesUsed() {
        return ramBytesUsed;
    }

    private static long ramBytesUsed(Map<String, List<StoreFileMetadata>> backedUpFilesByName, @Nullable String latestStateId) {
        long bytes = SHALLOW_SIZE + RamUsageEstimator.sizeOf(latestStateId);
        bytes += sizeOfCollection(backedUpFilesByName, MAP_SLOTS_PER_ENTRY * backedUpFilesByName.size());
        for (List<StoreFileMetadata> files : backedUpFilesByName.values()) {
            bytes += sizeOfCollection(files, files.size());
            for (StoreFileMetadata file : files) {
                bytes += STORE_FILE_METADATA_SHALLOW_SIZE;
                // The map's key is the same String as one of these names, so it isn't counted again.
                bytes += RamUsageEstimator.sizeOf(file.name());
                bytes += RamUsageEstimator.sizeOf(file.checksum());
                bytes += RamUsageEstimator.sizeOf(file.writtenBy());
                bytes += sizeOf(file.hash());
                // Files without a writer uuid all share one empty value, so it isn't counted for each file.
                if (file.writerUuid() != StoreFileMetadata.UNAVAILABLE_WRITER_UUID) {
                    bytes += sizeOf(file.writerUuid());
                }
            }
        }
        return bytes;
    }

    /**
     * The size of a {@code java.util} collection: the object, its table of references, and one extra array header. The extra header
     * isn't real memory. It's there because {@code RamUsageTester}, which the tests compare this estimate with, counts a collection
     * that way, and without it a very small baseline would come out below the test's figure.
     */
    private static long sizeOfCollection(Object collection, long references) {
        final long table = RamUsageEstimator.alignObjectSize(
            RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + references * RamUsageEstimator.NUM_BYTES_OBJECT_REF
        );
        return RamUsageEstimator.shallowSizeOf(collection) + table + RamUsageEstimator.NUM_BYTES_ARRAY_HEADER;
    }

    private static long sizeOf(BytesRef bytesRef) {
        return BYTES_REF_SHALLOW_SIZE + RamUsageEstimator.sizeOf(bytesRef.bytes);
    }
}
