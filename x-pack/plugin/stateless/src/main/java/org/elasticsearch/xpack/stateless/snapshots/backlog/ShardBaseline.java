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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshots;
import org.elasticsearch.index.snapshots.blobstore.SnapshotFiles;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * What a repository holds of one shard, to work out what the next snapshot of the shard would upload ({@link ShardBacklog}): the files
 * of every snapshot in the shard-level metadata ({@code index-{generation}}, a {@link BlobStoreIndexShardSnapshots}) and the shard state
 * ids of those snapshots. A node keeps one for each of its primaries and each repository, for as long as it holds the shard, so it is
 * packed into a few arrays instead of an object per file.
 * <p>
 * A file matches exactly when the snapshot would reuse it ({@link BlobStoreIndexShardSnapshots#findPhysicalIndexFile}, which uses
 * {@link StoreFileMetadata#isSame}), for the commit files that a snapshot uploads: those whose contents are not kept inside the
 * shard-level metadata ({@link Store.MetadataSnapshot#isReadAsHash}), which have no hash.
 */
final class ShardBaseline implements Accountable {

    static final ShardBaseline EMPTY = of(BlobStoreIndexShardSnapshots.EMPTY);

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(ShardBaseline.class);

    // the physical names of the files, each once, in BytesRef order
    private final PackedBytes names;
    // the files named names[i] are fileStarts[i] until fileStarts[i + 1]; each is there once, whichever snapshots refer to it
    private final int[] fileStarts;
    private final long[] lengths;
    private final long[] checksums;
    // empty for a file the repository has no writer uuid for
    private final PackedBytes writerUuids;
    // the shard state ids of the snapshots, each once, in BytesRef order
    private final PackedBytes stateIds;

    private ShardBaseline(
        PackedBytes names,
        int[] fileStarts,
        long[] lengths,
        long[] checksums,
        PackedBytes writerUuids,
        PackedBytes stateIds
    ) {
        assert fileStarts.length == names.size() + 1;
        assert lengths.length == fileStarts[names.size()] && checksums.length == lengths.length && writerUuids.size() == lengths.length;
        this.names = names;
        this.fileStarts = fileStarts;
        this.lengths = lengths;
        this.checksums = checksums;
        this.writerUuids = writerUuids;
        this.stateIds = stateIds;
    }

    /**
     * Leaves out the files that never match a commit file that a snapshot uploads: files kept inside the shard-level metadata, which only
     * match files of the same name, also kept there; files with a hash, which only match files with the same hash; and files whose
     * checksum is not written the way {@link Store#digestToString} writes a commit file's checksum, which only match the same string.
     */
    static ShardBaseline of(BlobStoreIndexShardSnapshots snapshots) {
        // a file that several snapshots refer to is the same FileInfo in each, so most of them are only looked at once
        final Set<FileInfo> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        final Map<BytesRef, Set<RepositoryFile>> filesByName = new TreeMap<>();
        final Set<BytesRef> stateIds = new TreeSet<>();
        for (SnapshotFiles snapshot : snapshots) {
            if (snapshot.shardStateIdentifier() != null) {
                stateIds.add(new BytesRef(snapshot.shardStateIdentifier()));
            }
            for (FileInfo fileInfo : snapshot.indexFiles()) {
                if (seen.add(fileInfo) == false) {
                    continue;
                }
                final StoreFileMetadata metadata = fileInfo.metadata();
                if (Store.MetadataSnapshot.isReadAsHash(metadata.name())
                    || metadata.hash().length > 0
                    || isDigestString(metadata.checksum()) == false) {
                    continue;
                }
                filesByName.computeIfAbsent(new BytesRef(metadata.name()), name -> new LinkedHashSet<>())
                    .add(
                        new RepositoryFile(
                            metadata.length(),
                            Long.parseLong(metadata.checksum(), Character.MAX_RADIX),
                            metadata.writerUuid()
                        )
                    );
            }
        }

        final int[] fileStarts = new int[filesByName.size() + 1];
        final List<RepositoryFile> files = new ArrayList<>();
        int name = 0;
        for (Set<RepositoryFile> filesWithName : filesByName.values()) {
            files.addAll(filesWithName);
            fileStarts[++name] = files.size();
        }
        final long[] lengths = new long[files.size()];
        final long[] checksums = new long[files.size()];
        final List<BytesRef> writerUuids = new ArrayList<>(files.size());
        for (int file = 0; file < files.size(); file++) {
            lengths[file] = files.get(file).length();
            checksums[file] = files.get(file).checksum();
            writerUuids.add(files.get(file).writerUuid());
        }
        return new ShardBaseline(
            PackedBytes.of(filesByName.keySet()),
            fileStarts,
            lengths,
            checksums,
            PackedBytes.of(writerUuids),
            PackedBytes.of(stateIds)
        );
    }

    private static boolean isDigestString(String checksum) {
        try {
            return Store.digestToString(Long.parseLong(checksum, Character.MAX_RADIX)).equals(checksum);
        } catch (NumberFormatException e) {
            return false;
        }
    }

    /**
     * Whether a snapshot in the repository has the given shard state id, in which case a snapshot of a commit with that id uploads no
     * files at all.
     */
    boolean hasStateId(@Nullable String stateId) {
        return stateId != null && stateIds.indexOf(new BytesRef(stateId)) >= 0;
    }

    /**
     * Whether the repository may hold the given commit file, so that its checksum decides: a file of the same name and length whose
     * writer uuid does not differ. If not, the repository does not hold it.
     *
     * @param writerUuid the commit's writer uuid for the file ({@link StoreFileMetadata#writerUuid()}), empty if it has none
     */
    boolean mayHold(String name, long length, BytesRef writerUuid) {
        return findFile(name, length, writerUuid, false, 0);
    }

    /**
     * Whether the repository holds the given commit file, by {@link StoreFileMetadata#isSame}: a file of the same name, length and
     * checksum, whose writer uuid is the same if both have one.
     *
     * @param writerUuid the commit's writer uuid for the file ({@link StoreFileMetadata#writerUuid()}), empty if it has none
     * @param checksum   the checksum in the file's footer
     */
    boolean holds(String name, long length, BytesRef writerUuid, long checksum) {
        return findFile(name, length, writerUuid, true, checksum);
    }

    private boolean findFile(String name, long length, BytesRef writerUuid, boolean compareChecksum, long checksum) {
        final int nameIndex = names.indexOf(new BytesRef(name));
        if (nameIndex < 0) {
            return false;
        }
        for (int file = fileStarts[nameIndex]; file < fileStarts[nameIndex + 1]; file++) {
            if (lengths[file] != length) {
                continue;
            }
            // as isSame: the writer uuid only counts if both have one
            if (writerUuid.length > 0 && writerUuids.isEmpty(file) == false && writerUuids.equals(file, writerUuid) == false) {
                continue;
            }
            if (compareChecksum && checksums[file] != checksum) {
                continue;
            }
            return true;
        }
        return false;
    }

    @Override
    public long ramBytesUsed() {
        return SHALLOW_SIZE + names.ramBytesUsed() + RamUsageEstimator.sizeOf(fileStarts) + RamUsageEstimator.sizeOf(lengths)
            + RamUsageEstimator.sizeOf(checksums) + writerUuids.ramBytesUsed() + stateIds.ramBytesUsed();
    }

    private record RepositoryFile(long length, long checksum, BytesRef writerUuid) {}

    /**
     * Byte strings in one array, so that thousands of them cost two arrays instead of two objects each.
     */
    private static final class PackedBytes implements Accountable {

        private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(PackedBytes.class);

        private final byte[] bytes;
        // byte string i is bytes[starts[i]] until bytes[starts[i + 1]]
        private final int[] starts;

        private PackedBytes(byte[] bytes, int[] starts) {
            this.bytes = bytes;
            this.starts = starts;
        }

        static PackedBytes of(Collection<BytesRef> values) {
            int length = 0;
            for (BytesRef value : values) {
                length = Math.addExact(length, value.length);
            }
            final byte[] bytes = new byte[length];
            final int[] starts = new int[values.size() + 1];
            int i = 0;
            for (BytesRef value : values) {
                System.arraycopy(value.bytes, value.offset, bytes, starts[i], value.length);
                starts[i + 1] = starts[i] + value.length;
                i++;
            }
            return new PackedBytes(bytes, starts);
        }

        int size() {
            return starts.length - 1;
        }

        boolean isEmpty(int i) {
            return starts[i] == starts[i + 1];
        }

        boolean equals(int i, BytesRef value) {
            return Arrays.equals(bytes, starts[i], starts[i + 1], value.bytes, value.offset, value.offset + value.length);
        }

        /**
         * @return the index of the given byte string, or a negative number if it is not there; the byte strings must be in BytesRef order
         */
        int indexOf(BytesRef value) {
            int low = 0;
            int high = size() - 1;
            while (low <= high) {
                final int middle = (low + high) >>> 1;
                final int comparison = Arrays.compareUnsigned(
                    bytes,
                    starts[middle],
                    starts[middle + 1],
                    value.bytes,
                    value.offset,
                    value.offset + value.length
                );
                if (comparison < 0) {
                    low = middle + 1;
                } else if (comparison > 0) {
                    high = middle - 1;
                } else {
                    return middle;
                }
            }
            return -1;
        }

        @Override
        public long ramBytesUsed() {
            return SHALLOW_SIZE + RamUsageEstimator.sizeOf(bytes) + RamUsageEstimator.sizeOf(starts);
        }
    }
}
