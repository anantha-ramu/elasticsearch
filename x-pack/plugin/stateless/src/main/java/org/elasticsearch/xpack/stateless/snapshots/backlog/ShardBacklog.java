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
 * Works out how many bytes the next snapshot of a shard would upload to a repository, the way the snapshot itself decides it
 * ({@code BlobStoreRepository#doSnapshotShard}), by comparing what the repository holds of the shard ({@link ShardBaseline}) with the
 * shard's latest commit:
 * <ul>
 *     <li>if a snapshot in the repository has the commit's shard state id, nothing is uploaded;</li>
 *     <li>otherwise every commit file the repository doesn't hold is uploaded, apart from the {@code .si} and {@code segments_N} files,
 *     whose contents a snapshot keeps inside the shard-level metadata ({@link Store.MetadataSnapshot#isReadAsHash}).</li>
 * </ul>
 * Nothing is read here. The caller first asks which files need their checksum ({@link #filesNeedingChecksum}), reads only those, and
 * passes them to {@link #bytesToUpload}. Only a file the repository has a file of the same name and length for is worth checking.
 * <p>
 * The incremental size a snapshot reports also counts the {@code .si} and {@code segments_N} files it adds, so it can be a little more
 * than this.
 */
final class ShardBacklog {

    private ShardBacklog() {}

    /**
     * A file of a shard's commit, by name and length, before its checksum is read. A commit lists each name once.
     */
    record CommitFile(String name, long length) {
        CommitFile {
            Objects.requireNonNull(name);
        }
    }

    /**
     * @param stateId the shard state id a snapshot of the commit would have ({@code SnapshotShardsService#getShardStateId}), {@code null}
     *                if the shard is not in a steady state
     * @return the commit files the repository has a file of the same name and length for, whose checksums {@link #bytesToUpload} needs
     */
    static List<CommitFile> filesNeedingChecksum(ShardBaseline baseline, @Nullable String stateId, Collection<CommitFile> commitFiles) {
        if (baseline.hasStateId(stateId)) {
            return List.of();
        }
        final List<CommitFile> files = new ArrayList<>();
        for (CommitFile file : commitFiles) {
            if (isUploaded(file) && baseline.mayHold(file.name(), file.length())) {
                files.add(file);
            }
        }
        return files;
    }

    /**
     * @param stateId          the shard state id a snapshot of the commit would have ({@code SnapshotShardsService#getShardStateId}),
     *                         {@code null} if the shard is not in a steady state
     * @param commitFileMetadata the metadata of each file that {@link #filesNeedingChecksum} returned, by file name, as a snapshot reads
     *                         it ({@link Store.MetadataSnapshot#loadFromIndexCommit}): its checksum, and its writer uuid if the commit has
     *                         one. {@link StoreFileMetadata#isSame} needs both: a file of an index created again under the same name can
     *                         have the name, length and checksum of a file the repository holds, and only the writer uuid tells them apart.
     * @return the bytes the next snapshot of the commit would upload
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
            if (baseline.mayHold(file.name(), file.length())) {
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
     * Whether a snapshot uploads the file if the repository does not hold it. It keeps the contents of the {@code .si} and
     * {@code segments_N} files inside the shard-level metadata instead. The snapshot checks {@link StoreFileMetadata#hashEqualsContents},
     * which is true for exactly these files of a commit read by {@link Store.MetadataSnapshot#loadFromIndexCommit}.
     */
    private static boolean isUploaded(CommitFile file) {
        return Store.MetadataSnapshot.isReadAsHash(file.name()) == false;
    }
}
