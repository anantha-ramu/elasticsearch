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
import org.elasticsearch.xpack.stateless.snapshots.backlog.ShardBaseline.LuceneFile;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Works out how many bytes a snapshot of a shard's latest commit would upload, using the same rules as the snapshot itself
 * ({@code BlobStoreRepository#doSnapshotShard}). It compares the shard's latest commit with what the repository has already backed up
 * ({@link ShardBaseline}):
 * <ul>
 *     <li>if the shard hasn't changed since the latest snapshot (the same state id), nothing is uploaded;</li>
 *     <li>otherwise every file the repository doesn't already have is uploaded, except the {@code .si} and {@code segments_N} files,
 *     whose contents a snapshot stores in the metadata itself ({@link ShardBaseline#isStoredInBackupMetadata}).</li>
 * </ul>
 * Nothing is read here. Checking whether a file is already backed up needs its checksum, which means reading the end of the file, so
 * this works in two steps: {@link #filesNeedingChecksum} says which files need one, the caller reads just those, and
 * {@link #bytesToUpload} gives the answer. Only a file whose name is in the baseline needs a checksum, because the baseline already
 * kept just the files with the right length.
 * <p>
 * The incremental size a snapshot reports also counts the {@code .si} and {@code segments_N} files it adds, so it can be a little more
 * than this. A real snapshot also flushes the shard first, so it also uploads writes that aren't in a commit yet.
 * <p>
 * {@link ShardBaseline} explains why what it keeps is enough, and how each case is handled.
 */
final class ShardBacklog {

    private ShardBacklog() {}

    /**
     * Step 1: which of the shard's files can't be judged from their name and length alone, so that the caller must read their
     * metadata, with checksums, for {@link #bytesToUpload}. Step 2 must be given the same baseline, state id and files.
     *
     * @param currentStateId the shard's state id as a snapshot would work it out ({@code SnapshotShardsService#getShardStateId}),
     *                       or {@code null} while writes are still in flight
     * @return the files whose name is in the baseline; {@link #bytesToUpload} needs their checksums
     */
    static List<LuceneFile> filesNeedingChecksum(
        ShardBaseline baseline,
        @Nullable String currentStateId,
        Collection<LuceneFile> currentFiles
    ) {
        if (baseline.isLatestStateId(currentStateId)) {
            return List.of();
        }
        final List<LuceneFile> needingChecksum = new ArrayList<>();
        for (LuceneFile file : currentFiles) {
            // The baseline never has .si or segments_N files, so those never need a checksum.
            if (baseline.hasFileNamed(file.name())) {
                needingChecksum.add(file);
            }
        }
        return needingChecksum;
    }

    /**
     * Step 2: the bytes a snapshot of the shard's latest commit would upload, given the metadata of the files that
     * {@link #filesNeedingChecksum} returned for the same baseline, state id and files.
     *
     * @param currentStateId      the shard's state id, as for {@link #filesNeedingChecksum}
     * @param currentFileMetadata for each file that {@link #filesNeedingChecksum} returned, its metadata by file name, built the way a
     *                            snapshot reads it ({@link Store.MetadataSnapshot#loadFromIndexCommit}): with its checksum and, if
     *                            the commit has one, its writer uuid. Without the writer uuid, a different file with the same name,
     *                            length and checksum would look backed up.
     * @return the bytes a snapshot of the shard's latest commit would upload
     * @throws IllegalArgumentException if a file that {@link #filesNeedingChecksum} returned has no metadata, or metadata of another
     *                                  file: a bug in the caller, which a guess would hide behind a wrong number
     */
    static long bytesToUpload(
        ShardBaseline baseline,
        @Nullable String currentStateId,
        Collection<LuceneFile> currentFiles,
        Map<String, StoreFileMetadata> currentFileMetadata
    ) {
        if (baseline.isLatestStateId(currentStateId)) {
            return 0;
        }
        long bytes = 0;
        for (LuceneFile file : currentFiles) {
            if (ShardBaseline.isStoredInBackupMetadata(file)) {
                continue;
            }
            if (baseline.hasFileNamed(file.name())) {
                final StoreFileMetadata metadata = currentFileMetadata.get(file.name());
                if (metadata == null) {
                    throw new IllegalArgumentException("no checksum for [" + file.name() + "]");
                }
                if (metadata.name().equals(file.name()) == false || metadata.length() != file.length()) {
                    throw new IllegalArgumentException("[" + metadata + "] is not the metadata of " + file);
                }
                if (baseline.isBackedUp(metadata)) {
                    continue;
                }
            }
            bytes += file.length();
        }
        return bytes;
    }
}
