/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots.backlog;

import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshots;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.repositories.blobstore.ChecksumBlobStoreFormat;
import org.elasticsearch.xpack.stateless.snapshots.backlog.ShardBaseline.LuceneFile;

import java.util.Collection;
import java.util.List;

/**
 * Reads a shard's backup metadata ({@code index-{generation}}) straight into a {@link ShardBaseline}, one entry at a time. Reading it
 * this way takes about as much memory as the baseline it builds, however many snapshots the repository keeps, while reading the whole
 * metadata ({@link BlobStoreRepository#INDEX_SHARD_SNAPSHOTS_FORMAT}) takes memory that grows with them. The blob is read as a
 * snapshot reads it, with the same header, compression and checksum.
 * <p>
 * Reading entry by entry can't check what {@code BlobStoreIndexShardSnapshots#fromXContent} checks at the end, that every file a
 * snapshot refers to is listed, without keeping every file name. A blob that fails that check is corrupt, and a snapshot of the shard
 * fails on it anyway. A listed file that no snapshot refers to, or one listed after the snapshots, only ever counts as not backed up.
 */
final class ShardBaselineFormat {

    // The codec of BlobStoreRepository#INDEX_SHARD_SNAPSHOTS_FORMAT, which wrote the blob. Reading checks it.
    private static final String CODEC = "snapshots";

    private ShardBaselineFormat() {}

    /**
     * The format to read a generation's blob with, building a baseline for the given files of the shard's build commit. See
     * {@link ShardBaseline#of} for which commit that may be.
     */
    static ChecksumBlobStoreFormat<ShardBaseline> forFiles(Collection<LuceneFile> currentFiles) {
        // No fallback reader: with one, ChecksumBlobStoreFormat#deserialize reads the whole blob into memory before parsing it.
        return new ChecksumBlobStoreFormat<>(CODEC, BlobStoreRepository.SNAPSHOT_INDEX_NAME_FORMAT, (repository, parser) -> {
            final ShardBaseline.Builder builder = new ShardBaseline.Builder(currentFiles);
            BlobStoreIndexShardSnapshots.parse(parser, new BlobStoreIndexShardSnapshots.Visitor() {
                @Override
                public void file(FileInfo fileInfo) {
                    builder.addFile(fileInfo);
                }

                @Override
                public void snapshot(String name, List<String> fileNames, String shardStateId) {
                    // A snapshot that lists no files is left out, as BlobStoreIndexShardSnapshots#fromXContent leaves it out.
                    if (fileNames != null) {
                        builder.addSnapshot(fileNames, shardStateId);
                    }
                }
            });
            return builder.build();
        }, baseline -> { throw new UnsupportedOperationException("a baseline is never written"); });
    }
}
