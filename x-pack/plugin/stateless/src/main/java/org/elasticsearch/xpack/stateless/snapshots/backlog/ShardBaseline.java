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
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshots;
import org.elasticsearch.index.snapshots.blobstore.SnapshotFiles;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.xpack.stateless.snapshots.backlog.ShardBacklog.CommitFile;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * What a repository holds of one shard that still matters for a commit of the shard: the files in the shard-level metadata
 * ({@code index-{generation}}, a {@link BlobStoreIndexShardSnapshots}) with the name and length of one of the commit's files, and the
 * shard state id of the latest snapshot that has one. {@link ShardBacklog} uses it to work out what the next snapshot of the shard
 * would upload. Whether the repository holds a commit file is decided by {@link StoreFileMetadata#isSame}, as the snapshot decides it.
 * <p>
 * A baseline is for the commit it was built for and every later commit of the shard, until the shard-level metadata changes. What it
 * leaves out can only make the backlog larger, never smaller, and it stays about the size of the shard's commit however long snapshots
 * are retained:
 * <ul>
 *     <li>Files that only older snapshots have, merged away since. Lucene never gives a new file of an index the name of an earlier
 *     one (segment names come from a counter, and the generations of a segment's files only go up), and files never change once
 *     written, so a later commit can only have one of the commit's names with the same length.</li>
 *     <li>Every state id but the latest. Within a shard's history the state id only moves forward (it ends with the max sequence
 *     number), so the latest is the only one a later commit can have. A restore starts a new history, so its state ids match none of
 *     the old ones.</li>
 * </ul>
 * Two cases differ from the snapshot, and both only make the backlog larger:
 * <ul>
 *     <li>A clone of an older snapshot puts that snapshot's state id last. Until the next snapshot, which puts the shard's own state id
 *     last again, a shard with no new writes whose files were merged counts the merged files.</li>
 *     <li>An index deleted and created again under the same name shares its folder in the repository, and its later files can get
 *     names the old index used. Those files have other writer uuids, so {@link StoreFileMetadata#isSame} tells them apart, unless the
 *     repository has no writer uuid for the old file (older versions did not record one) and the two files have the same length and
 *     checksum by chance.</li>
 * </ul>
 */
final class ShardBaseline implements Accountable {

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(ShardBaseline.class);
    private static final long FILE_SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(StoreFileMetadata.class);
    private static final long BYTES_REF_SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(BytesRef.class);

    static final ShardBaseline EMPTY = of(BlobStoreIndexShardSnapshots.EMPTY, List.of());

    // the files by physical name, each once whichever snapshots refer to it
    private final Map<String, List<StoreFileMetadata>> filesByName;
    @Nullable
    private final String stateId;
    private final long ramBytesUsed;

    private ShardBaseline(Map<String, List<StoreFileMetadata>> filesByName, @Nullable String stateId) {
        this.filesByName = filesByName;
        this.stateId = stateId;
        this.ramBytesUsed = ramBytesUsed(filesByName, stateId);
    }

    /**
     * Builds the baseline for a commit of the shard. The commit must be taken after the shard-level metadata was read: if a snapshot
     * finished in between, the metadata would have files of a newer commit, which a baseline for an older commit leaves out.
     *
     * @param commitFiles the files of the commit the baseline is for, normally the shard's latest
     */
    static ShardBaseline of(BlobStoreIndexShardSnapshots snapshots, Collection<CommitFile> commitFiles) {
        // the files a snapshot of the commit, or of a later one, looks for in the repository: it keeps the contents of the .si and
        // segments_N files inside the shard-level metadata instead
        final Map<String, Long> lengths = new HashMap<>();
        for (CommitFile file : commitFiles) {
            if (Store.MetadataSnapshot.isReadAsHash(file.name()) == false) {
                lengths.put(file.name(), file.length());
            }
        }
        // each file once, as BlobStoreIndexShardSnapshots#findPhysicalIndexFile does: every snapshot of a file refers to its blob
        final Set<String> blobNames = new HashSet<>();
        final Map<String, List<StoreFileMetadata>> filesByName = new HashMap<>();
        String stateId = null;
        // in the order the snapshots were added, so the last state id is the latest
        for (SnapshotFiles snapshot : snapshots) {
            if (snapshot.shardStateIdentifier() != null) {
                stateId = snapshot.shardStateIdentifier();
            }
            for (FileInfo fileInfo : snapshot.indexFiles()) {
                final Long length = lengths.get(fileInfo.physicalName());
                if (length != null && length == fileInfo.length() && blobNames.add(fileInfo.name())) {
                    filesByName.computeIfAbsent(fileInfo.physicalName(), name -> new ArrayList<>()).add(fileInfo.metadata());
                }
            }
        }
        return new ShardBaseline(Map.copyOf(Maps.transformValues(filesByName, List::copyOf)), stateId);
    }

    /**
     * Whether the latest snapshot in the repository that has a state id has the given one, in which case a snapshot of a commit with
     * that state id uploads no files at all.
     */
    boolean hasStateId(@Nullable String stateId) {
        return stateId != null && stateId.equals(this.stateId);
    }

    /**
     * Whether the baseline has a repository file of the given name. Those all have the length of the commit's file of that name, which
     * a later commit of the shard has too. If not, the repository does not hold a commit file of that name: {@link
     * StoreFileMetadata#isSame} only matches files of the same length, apart from a file whose hash is its whole contents, which only
     * matches a file with the same hash, and the commit files a snapshot uploads have no hash. If so, {@link #holds} tells from the
     * commit file's checksum and writer uuid.
     */
    boolean mayHold(String name) {
        return filesByName.containsKey(name);
    }

    /**
     * Whether the repository holds the given commit file, that is, whether a snapshot would reuse one of the repository's files instead
     * of uploading it ({@link BlobStoreIndexShardSnapshots#findPhysicalIndexFile}).
     */
    boolean holds(StoreFileMetadata commitFile) {
        for (StoreFileMetadata file : filesByName.getOrDefault(commitFile.name(), List.of())) {
            if (file.isSame(commitFile)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public long ramBytesUsed() {
        return ramBytesUsed;
    }

    private static long ramBytesUsed(Map<String, List<StoreFileMetadata>> filesByName, @Nullable String stateId) {
        long bytes = SHALLOW_SIZE + RamUsageEstimator.sizeOf(stateId);
        // Map.copyOf keeps a table of twice its keys and values
        bytes += sizeOfCollection(filesByName, 4L * filesByName.size());
        for (List<StoreFileMetadata> files : filesByName.values()) {
            bytes += sizeOfCollection(files, files.size());
            for (StoreFileMetadata file : files) {
                // the map's key is one of these names, so it isn't counted again
                bytes += FILE_SHALLOW_SIZE + RamUsageEstimator.sizeOf(file.name()) + RamUsageEstimator.sizeOf(file.checksum())
                    + RamUsageEstimator.sizeOf(file.writtenBy()) + sizeOf(file.hash());
                // files without a writer uuid all share the same empty one
                if (file.writerUuid() != StoreFileMetadata.UNAVAILABLE_WRITER_UUID) {
                    bytes += sizeOf(file.writerUuid());
                }
            }
        }
        return bytes;
    }

    /**
     * A {@code java.util} collection: its shell and its table of references, plus one more array header. That header is not on the
     * heap, but {@code RamUsageTester}, which the tests check the estimate against, counts a collection as its shell and two array
     * headers, and without it the estimate of an empty or very small baseline would fall below that.
     */
    private static long sizeOfCollection(Object collection, long references) {
        return RamUsageEstimator.shallowSizeOf(collection) + RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + RamUsageEstimator.alignObjectSize(
            RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + references * RamUsageEstimator.NUM_BYTES_OBJECT_REF
        );
    }

    private static long sizeOf(BytesRef bytesRef) {
        return BYTES_REF_SHALLOW_SIZE + RamUsageEstimator.sizeOf(bytesRef.bytes);
    }
}
