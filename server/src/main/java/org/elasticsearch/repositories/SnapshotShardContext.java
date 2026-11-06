/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories;

import org.apache.lucene.store.IndexInput;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DelegatingActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.lucene.store.InputStreamIndexInput;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.snapshots.SnapshotId;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;

/**
 * Context holding the state for creating a shard snapshot via {@link Repository#snapshotShard(SnapshotShardContext)}.
 * Wraps a {@link org.elasticsearch.index.engine.Engine.IndexCommitRef} that is released once this instances is completed by invoking
 * either its {@link #onResponse(ShardSnapshotResult)} or {@link #onFailure(Exception)} callback.
 */
public abstract class SnapshotShardContext extends DelegatingActionListener<ShardSnapshotResult, ShardSnapshotResult> {

    private final SnapshotId snapshotId;
    private final IndexId indexId;
    @Nullable
    private final String shardStateIdentifier;
    private final IndexShardSnapshotStatus snapshotStatus;
    private final IndexVersion repositoryMetaVersion;
    private final long snapshotStartTime;

    @SuppressWarnings("this-escape")
    protected SnapshotShardContext(
        SnapshotId snapshotId,
        IndexId indexId,
        @Nullable String shardStateIdentifier,
        IndexShardSnapshotStatus snapshotStatus,
        IndexVersion repositoryMetaVersion,
        long snapshotStartTime,
        ActionListener<ShardSnapshotResult> listener
    ) {
        super(new SubscribableListener<>());
        addListener(listener);
        this.snapshotId = snapshotId;
        this.indexId = indexId;
        this.shardStateIdentifier = shardStateIdentifier;
        this.snapshotStatus = snapshotStatus;
        this.repositoryMetaVersion = repositoryMetaVersion;
        this.snapshotStartTime = snapshotStartTime;
    }

    public SnapshotId snapshotId() {
        return snapshotId;
    }

    public IndexId indexId() {
        return indexId;
    }

    @Nullable
    public String stateIdentifier() {
        return shardStateIdentifier;
    }

    public IndexShardSnapshotStatus status() {
        return snapshotStatus;
    }

    public IndexVersion getRepositoryMetaVersion() {
        return repositoryMetaVersion;
    }

    public long snapshotStartTime() {
        return snapshotStartTime;
    }

    @Override
    public void onResponse(ShardSnapshotResult result) {
        delegate.onResponse(result);
    }

    public abstract ShardId shardId();

    public abstract boolean isSearchableSnapshot();

    public abstract Store.MetadataSnapshot metadataSnapshot();

    public abstract Collection<String> fileNames();

    public abstract boolean assertFileContentsMatchHash(BlobStoreIndexShardSnapshot.FileInfo fileInfo);

    public abstract void failStoreIfCorrupted(Exception e);

    public abstract FileReader fileReader(String file, StoreFileMetadata metadata) throws IOException;

    /**
     * Returns the commit blob that holds the file, so that the snapshot can copy that whole blob inside the object store instead of
     * reading the file and uploading it, or {@code null} if the file must be uploaded. A commit blob is a blob in a stateless object store
     * that holds the files of one or more Lucene commits, see {@link BlobStoreIndexShardSnapshot.FileInfo}. Only a stateless shard can
     * return a commit blob, and only when server-side copy is enabled. While a copy runs, the caller holds the shard's commit with
     * {@link #fileReader} of a file in that blob, as an upload of that file would, so that the commit blob stays in the object store.
     */
    @Nullable
    public CopySource copySource(String file) {
        return null;
    }

    /**
     * Says whether the snapshot may reuse {@code existingFileInfo}. A file info is the entry in the shard's snapshot metadata that says
     * where one file is stored. An earlier snapshot of the shard made {@code existingFileInfo} when it stored the same file as a byte
     * range of a copied commit blob. The repository calls this method only for a copied file info that it found by the name, length and
     * checksum of the file.
     * <p>
     * The default answer is {@code true}. Then the snapshot points the file at the earlier file info, and makes no new file info and no
     * copy for the file. On {@code false}, the repository handles the file as if no earlier snapshot had stored it. The repository asks
     * {@link #copySource} for the commit blob that holds the file now, and makes a new file info that points at a copy of that commit
     * blob. If an earlier snapshot already made a copy of that commit blob, the new file info points at that copy, and the repository
     * makes no new copy.
     * <p>
     * A context should answer {@code false} when the file is now in a different commit blob than the one that the earlier snapshot
     * copied, and the earlier file info would keep the copy of the old commit blob alive. The repository can then delete that copy once
     * no other file needs it. A context must answer {@code true} when {@link #copySource} returns {@code null}, because the repository
     * would then upload the file again, although the repository already holds a copy of the file.
     *
     * @param existingFileInfo a file info of an earlier snapshot of the shard that is stored as a byte range of a copied commit blob, see
     *                         {@link BlobStoreIndexShardSnapshot.FileInfo#isCopied()}
     */
    public boolean isCopiedFileReusable(BlobStoreIndexShardSnapshot.FileInfo existingFileInfo) {
        return true;
    }

    /**
     * A commit blob in an object store that holds a file, and where the file starts in it.
     *
     * @param source    path of the commit blob, unique across indices and shards. A snapshot reuses an existing copy made from the same
     *                  path, see {@link BlobStoreIndexShardSnapshot.FileInfo#source()}.
     * @param copyName  the part of the copy's name that comes after the shard generation. It must be unique among the sources that one
     *                  shard snapshot copies, so that two copies of one shard snapshot never get the same name. It may use only characters
     *                  that {@link org.elasticsearch.common.Strings#validFileName} accepts, because the repository refuses to read a file
     *                  info whose copied blob name has any other character.
     * @param container the container that holds the commit blob
     * @param blobName  the name of the commit blob in {@code container}
     * @param offset    the position in that blob where the file starts
     */
    public record CopySource(String source, String copyName, BlobContainer container, String blobName, long offset) {}

    public interface FileReader extends Closeable {

        InputStream openInput(long limit) throws IOException;

        /**
         * Maybe release the reference acquired for the index commit. We may want to release the reference
         * if the shard relocates and the commit is now retaiend by a different node so that snapshot running
         * on the old node no longer needs to maintain it.
         */
        default void maybeReleaseCommitRef() {}

        void verify() throws IOException;
    }

    public static class IndexInputFileReader implements FileReader {

        private final Releasable commitRefReleasable;
        private final IndexInput indexInput;

        public IndexInputFileReader(Releasable commitRefReleasable, IndexInput indexInput) {
            this.commitRefReleasable = commitRefReleasable;
            this.indexInput = indexInput;
        }

        @Override
        public InputStream openInput(long limit) throws IOException {
            return new InputStreamIndexInput(indexInput, limit);
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(indexInput, commitRefReleasable);
        }

        @Override
        public void verify() throws IOException {
            Store.verify(indexInput);
        }
    }

    public void addListener(ActionListener<ShardSnapshotResult> listener) {
        ((SubscribableListener<ShardSnapshotResult>) this.delegate).addListener(listener);
    }
}
