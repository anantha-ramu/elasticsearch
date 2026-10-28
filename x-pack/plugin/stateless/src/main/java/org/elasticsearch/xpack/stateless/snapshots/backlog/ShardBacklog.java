/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots.backlog;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Works out how many bytes the next snapshot of a shard would upload, using the same rules as the snapshot itself
 * ({@code BlobStoreRepository#doSnapshotShard}). It compares the shard's latest commit with what the repository has already backed up
 * ({@link ShardBaseline}):
 * <ul>
 *     <li>if the shard hasn't changed since the latest snapshot (the same state id), nothing is uploaded;</li>
 *     <li>otherwise every file the repository doesn't already have is uploaded, except the {@code .si} and {@code segments_N} files,
 *     whose contents a snapshot stores in the metadata itself ({@link Store.MetadataSnapshot#isReadAsHash}).</li>
 * </ul>
 * Nothing is read here. Checking whether a file is already backed up needs its checksum, which means reading the end of the file, so
 * this works in two steps: {@link #filesNeedingChecksum} says which files need one, the caller reads just those, and
 * {@link #bytesToUpload} gives the answer. Only a file whose name is in the baseline needs a checksum, because the baseline already
 * kept just the files with the right length.
 * <p>
 * The incremental size a snapshot reports also counts the {@code .si} and {@code segments_N} files it adds, so it can be a little more
 * than this.
 * <p>
 * {@link ShardBaseline} explains why what it keeps is enough, and how each case is handled.
 */
final class ShardBacklog {

    private ShardBacklog() {}

    /**
     * A file of the shard's latest commit, by name and length, before its checksum is read. A commit lists each name once.
     */
    record CommitFile(String name, long length) {
        CommitFile {
            Objects.requireNonNull(name);
        }
    }

    /**
     * @param stateId the shard's state id as a snapshot would work it out ({@code SnapshotShardsService#getShardStateId}), or
     *                {@code null} while writes are still in flight
     * @return the files whose name is in the baseline; {@link #bytesToUpload} needs their checksums
     */
    static List<CommitFile> filesNeedingChecksum(ShardBaseline baseline, @Nullable String stateId, Collection<CommitFile> commitFiles) {
        if (baseline.hasStateId(stateId)) {
            return List.of();
        }
        final List<CommitFile> files = new ArrayList<>();
        for (CommitFile file : commitFiles) {
            if (isUploaded(file) && baseline.mayHold(file.name())) {
                files.add(file);
            }
        }
        return files;
    }

    /**
     * @param stateId            the shard's state id, as for {@link #filesNeedingChecksum}
     * @param commitFileMetadata for each file that {@link #filesNeedingChecksum} returned, its metadata by file name, built the way a
     *                           snapshot reads it ({@link Store.MetadataSnapshot#loadFromIndexCommit}): with its checksum and, if the
     *                           commit has one, its writer uuid. Without the writer uuid, a different file with the same name, length
     *                           and checksum would look backed up.
     * @return the bytes the next snapshot would upload
     */
    static long bytesToUpload(
        ShardBaseline baseline,
        @Nullable String stateId,
        Collection<CommitFile> commitFiles,
        Map<String, StoreFileMetadata> commitFileMetadata
    ) {
        if (baseline.hasStateId(stateId)) {
            return 0;
        }
        long bytes = 0;
        for (CommitFile file : commitFiles) {
            if (isUploaded(file) == false) {
                continue;
            }
            if (baseline.mayHold(file.name())) {
                final StoreFileMetadata metadata = commitFileMetadata.get(file.name());
                if (metadata == null) {
                    throw new IllegalArgumentException("no checksum for [" + file.name() + "]");
                }
                if (metadata.name().equals(file.name()) == false || metadata.length() != file.length()) {
                    throw new IllegalArgumentException("[" + metadata + "] is not the metadata of " + file);
                }
                if (baseline.holds(metadata)) {
                    continue;
                }
            }
            bytes += file.length();
        }
        return bytes;
    }

    /**
     * Whether a snapshot would upload this file if the repository didn't already have it. It never uploads the {@code .si} and
     * {@code segments_N} files, because it stores their contents in the metadata itself. (The snapshot checks
     * {@link StoreFileMetadata#hashEqualsContents}, which is true for exactly these files.)
     */
    private static boolean isUploaded(CommitFile file) {
        return Store.MetadataSnapshot.isReadAsHash(file.name()) == false;
    }
}
