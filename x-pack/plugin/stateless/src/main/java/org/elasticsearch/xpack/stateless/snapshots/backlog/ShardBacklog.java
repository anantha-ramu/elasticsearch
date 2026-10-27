/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots.backlog;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * How many bytes the next snapshot of a shard would upload to a repository, decided the way the snapshot decides it
 * ({@code BlobStoreRepository#doSnapshotShard}), from what the repository holds of the shard ({@link ShardBaseline}) and the shard's
 * latest commit:
 * <ul>
 *     <li>nothing, if a snapshot in the repository has the commit's shard state id;</li>
 *     <li>otherwise the length of every commit file the repository does not hold, leaving out the files whose contents a snapshot keeps
 *     inside the shard-level metadata ({@link Store.MetadataSnapshot#isReadAsHash}), which are never uploaded.</li>
 * </ul>
 * Nothing is read here. The caller reads the checksums of the commit files, and only of the files that {@link #filesNeedingChecksum}
 * names: a file that the repository does not hold by name, length and writer uuid counts without one.
 */
final class ShardBacklog {

    private ShardBacklog() {}

    /**
     * A file of a shard's commit, as a snapshot sees it ({@link Store.MetadataSnapshot#loadFromIndexCommit}).
     *
     * @param writerUuid {@link StoreFileMetadata#writerUuid()}, empty if the commit has none for the file
     */
    record CommitFile(String name, long length, BytesRef writerUuid) {
        CommitFile {
            Objects.requireNonNull(name);
            Objects.requireNonNull(writerUuid);
        }
    }

    /**
     * @param stateId the commit's shard state id, {@code null} if it has none
     * @return the files whose checksum decides whether the repository holds them
     */
    static List<CommitFile> filesNeedingChecksum(ShardBaseline baseline, @Nullable String stateId, Collection<CommitFile> commitFiles) {
        if (baseline.hasStateId(stateId)) {
            return List.of();
        }
        final List<CommitFile> files = new ArrayList<>();
        for (CommitFile file : commitFiles) {
            if (isUploaded(file) && baseline.mayHold(file.name(), file.length(), file.writerUuid())) {
                files.add(file);
            }
        }
        return files;
    }

    /**
     * @param stateId   the commit's shard state id, {@code null} if it has none
     * @param checksums the checksums in the footers of the commit files, by file name; must contain every file that
     *                  {@link #filesNeedingChecksum} names
     * @return the bytes the next snapshot of the commit would upload
     */
    static long bytesToUpload(
        ShardBaseline baseline,
        @Nullable String stateId,
        Collection<CommitFile> commitFiles,
        Map<String, Long> checksums
    ) {
        if (baseline.hasStateId(stateId)) {
            return 0;
        }
        long bytes = 0;
        for (CommitFile file : commitFiles) {
            if (isUploaded(file) == false) {
                continue;
            }
            if (baseline.mayHold(file.name(), file.length(), file.writerUuid())) {
                final Long checksum = checksums.get(file.name());
                if (checksum == null) {
                    throw new IllegalArgumentException("no checksum for [" + file.name() + "]");
                }
                if (baseline.holds(file.name(), file.length(), file.writerUuid(), checksum)) {
                    continue;
                }
            }
            bytes += file.length();
        }
        return bytes;
    }

    /**
     * A snapshot keeps the contents of the other files inside the shard-level metadata ({@link StoreFileMetadata#hashEqualsContents}),
     * whether or not the repository has them already.
     */
    private static boolean isUploaded(CommitFile file) {
        return Store.MetadataSnapshot.isReadAsHash(file.name()) == false;
    }
}
