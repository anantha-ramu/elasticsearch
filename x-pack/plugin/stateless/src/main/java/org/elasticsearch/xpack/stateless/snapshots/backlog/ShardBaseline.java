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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * What a repository holds of one shard: the files of every snapshot in the shard-level metadata ({@code index-{generation}}, a
 * {@link BlobStoreIndexShardSnapshots}) and the shard state ids of those snapshots. {@link ShardBacklog} uses it to work out what the
 * next snapshot of the shard would upload. Whether the repository holds a commit file is decided by {@link StoreFileMetadata#isSame},
 * as the snapshot decides it.
 */
final class ShardBaseline implements Accountable {

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(ShardBaseline.class);
    private static final long FILE_SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(StoreFileMetadata.class);
    private static final long BYTES_REF_SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(BytesRef.class);

    static final ShardBaseline EMPTY = of(BlobStoreIndexShardSnapshots.EMPTY);

    // the files by physical name, each once whichever snapshots refer to it
    private final Map<String, List<StoreFileMetadata>> filesByName;
    private final Set<String> stateIds;
    private final long ramBytesUsed;

    private ShardBaseline(Map<String, List<StoreFileMetadata>> filesByName, Set<String> stateIds) {
        this.filesByName = filesByName;
        this.stateIds = stateIds;
        this.ramBytesUsed = ramBytesUsed(filesByName, stateIds);
    }

    /**
     * Leaves out the {@code .si} and {@code segments_N} files ({@link Store.MetadataSnapshot#isReadAsHash}): a snapshot keeps their
     * contents inside the shard-level metadata, so it never looks for them in the repository.
     */
    static ShardBaseline of(BlobStoreIndexShardSnapshots snapshots) {
        // keep each file once, as BlobStoreIndexShardSnapshots#findPhysicalIndexFile does: every snapshot of a file refers to its blob
        final Map<String, StoreFileMetadata> filesByBlobName = new HashMap<>();
        final Set<String> stateIds = new HashSet<>();
        for (SnapshotFiles snapshot : snapshots) {
            if (snapshot.shardStateIdentifier() != null) {
                stateIds.add(snapshot.shardStateIdentifier());
            }
            for (FileInfo fileInfo : snapshot.indexFiles()) {
                if (Store.MetadataSnapshot.isReadAsHash(fileInfo.physicalName()) == false) {
                    filesByBlobName.putIfAbsent(fileInfo.name(), fileInfo.metadata());
                }
            }
        }
        final Map<String, List<StoreFileMetadata>> filesByName = new HashMap<>();
        for (StoreFileMetadata file : filesByBlobName.values()) {
            filesByName.computeIfAbsent(file.name(), name -> new ArrayList<>()).add(file);
        }
        return new ShardBaseline(Map.copyOf(Maps.transformValues(filesByName, List::copyOf)), Set.copyOf(stateIds));
    }

    /**
     * Whether a snapshot in the repository has the given shard state id, in which case a snapshot of a commit with that id uploads no
     * files at all.
     */
    boolean hasStateId(@Nullable String stateId) {
        return stateId != null && stateIds.contains(stateId);
    }

    /**
     * Whether the repository has a file of the given name and length. Only then can it hold a commit file of that name and length, and
     * {@link #holds} needs the commit file's checksum to tell.
     */
    boolean mayHold(String name, long length) {
        for (StoreFileMetadata file : filesByName.getOrDefault(name, List.of())) {
            if (file.length() == length) {
                return true;
            }
        }
        return false;
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

    private static long ramBytesUsed(Map<String, List<StoreFileMetadata>> filesByName, Set<String> stateIds) {
        long bytes = SHALLOW_SIZE;
        // Map.copyOf keeps a table of twice its keys and values, Set.copyOf of twice its elements
        bytes += sizeOfCollection(filesByName, 4L * filesByName.size());
        bytes += sizeOfCollection(stateIds, 2L * stateIds.size());
        for (String stateId : stateIds) {
            bytes += RamUsageEstimator.sizeOf(stateId);
        }
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
     * A {@code java.util} collection: its shell, its table of references, and one more array header, because that is how
     * {@code RamUsageTester} counts these collections.
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
