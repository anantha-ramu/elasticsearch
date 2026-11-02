/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.blobstore;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.TestUtil;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingHelper;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.engine.InternalEngineFactory;
import org.elasticsearch.index.seqno.RetentionLeaseSyncer;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotFailedException;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.repositories.FinalizeSnapshotContext;
import org.elasticsearch.repositories.FinalizeSnapshotContext.UpdatedShardGenerations;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.repositories.ShardGenerations;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;

import static org.hamcrest.Matchers.containsString;

/**
 * This class tests the behavior of {@link BlobStoreRepository} when it
 * restores a shard from a snapshot but some files with same names already
 * exist on disc.
 */
public class BlobStoreRepositoryRestoreTests extends IndexShardTestCase {

    /**
     * Restoring a snapshot that contains multiple files must succeed even when
     * some files already exist in the shard's store.
     */
    public void testRestoreSnapshotWithExistingFiles() throws IOException {
        final IndexId indexId = new IndexId(randomAlphaOfLength(10), UUIDs.randomBase64UUID());
        final ShardId shardId = new ShardId(indexId.getName(), indexId.getId(), 0);

        IndexShard shard = newShard(shardId, true);
        try {
            // index documents in the shards
            final int numDocs = scaledRandomIntBetween(1, 500);
            recoverShardFromStore(shard);
            for (int i = 0; i < numDocs; i++) {
                indexDoc(shard, "_doc", Integer.toString(i));
                if (rarely()) {
                    flushShard(shard, false);
                }
            }
            assertDocCount(shard, numDocs);

            // snapshot the shard
            @FixForMultiProject(description = "randomize when snapshot and restore support multiple projects, see also ES-10225, ES-10228")
            final Repository repository = TestUtils.createRepository(ProjectId.DEFAULT, createTempDir(), xContentRegistry());
            final Snapshot snapshot = new Snapshot(repository.getMetadata().name(), new SnapshotId(randomAlphaOfLength(10), "_uuid"));
            snapshotShard(shard, snapshot, repository);

            // capture current store files
            final Store.MetadataSnapshot storeFiles = shard.snapshotStoreMetadata();
            assertFalse(storeFiles.fileMetadataMap().isEmpty());

            // close the shard
            closeShards(shard);

            // delete some random files in the store
            List<String> deletedFiles = randomSubsetOf(randomIntBetween(1, storeFiles.size() - 1), storeFiles.fileMetadataMap().keySet());
            for (String deletedFile : deletedFiles) {
                Files.delete(shard.shardPath().resolveIndex().resolve(deletedFile));
            }

            // build a new shard using the same store directory as the closed shard, recovering from the snapshot taken above
            ShardRouting shardRouting = ShardRoutingHelper.initWithSameId(
                shard.routingEntry(),
                new RecoverySource.SnapshotRecoverySource(UUIDs.randomBase64UUID(), snapshot, IndexVersion.current(), indexId)
            );
            shard = newShard(
                shardRouting,
                null,
                shard.shardPath(),
                shard.indexSettings().getIndexMetadata(),
                null,
                null,
                new InternalEngineFactory(),
                NOOP_GCP_SYNCER,
                RetentionLeaseSyncer.EMPTY,
                IndexEventListener.NOOP
            );

            // restore the shard
            recoverShardFromSnapshot(shard, snapshot, repository);

            // check that the shard is not corrupted
            TestUtil.checkIndex(shard.store().directory());

            // check that all files have been restored
            final Directory directory = shard.store().directory();
            final List<String> directoryFiles = Arrays.asList(directory.listAll());

            for (StoreFileMetadata storeFile : storeFiles) {
                String fileName = storeFile.name();
                assertTrue("File [" + fileName + "] does not exist in store directory", directoryFiles.contains(fileName));
                assertEquals(storeFile.length(), shard.store().directory().fileLength(fileName));
            }
        } finally {
            if (shard != null && shard.state() != IndexShardState.CLOSED) {
                try {
                    closeShardNoCheck(shard);
                } finally {
                    IOUtils.close(shard.store());
                }
            }
        }
    }

    public void testRestoreFilesInsideCopiedBlob() throws IOException {
        // Every data file moves into one copied blob, back to back, so the blob holds files at offset 0, in the middle and at the end.
        restoreAfterMovingFilesIntoCopiedBlob(fileInfo -> true, false, false);
    }

    public void testRestoreMixOfCopiedFilesAndFilesInTheirOwnBlobs() throws IOException {
        // Some files move into a copied blob with filler bytes around each one, the rest stay in blobs of their own.
        restoreAfterMovingFilesIntoCopiedBlob(fileInfo -> randomBoolean(), true, false);
    }

    public void testRestoreFailsCleanlyFromCorruptedCopiedBlob() throws IOException {
        restoreAfterMovingFilesIntoCopiedBlob(fileInfo -> true, randomBoolean(), true);
    }

    /**
     * Snapshots a shard, rewrites the shard snapshot so that the chosen files are byte ranges of one copied blob, then restores it into an
     * empty store. Restore must then read each moved file from its range, or fail with a corruption error if {@code corrupt} is set, in
     * which case one bit of one moved file is flipped first.
     */
    private void restoreAfterMovingFilesIntoCopiedBlob(
        Predicate<BlobStoreIndexShardSnapshot.FileInfo> filesToMove,
        boolean withFiller,
        boolean corrupt
    ) throws IOException {
        final IndexId indexId = new IndexId(randomAlphaOfLength(10), UUIDs.randomBase64UUID());
        final ShardId shardId = new ShardId(indexId.getName(), indexId.getId(), 0);

        IndexShard shard = newShard(shardId, true);
        try {
            recoverShardFromStore(shard);
            // several segments, so that there are several data files to move
            final int segments = between(2, 4);
            for (int segment = 0; segment < segments; segment++) {
                for (int i = between(1, 50); i > 0; i--) {
                    indexDoc(shard, "_doc", UUIDs.randomBase64UUID(random()));
                }
                flushShard(shard, true);
            }

            @FixForMultiProject(description = "randomize when snapshot and restore support multiple projects, see also ES-10225, ES-10228")
            final BlobStoreRepository repository = (BlobStoreRepository) TestUtils.createRepository(
                ProjectId.DEFAULT,
                createTempDir(),
                xContentRegistry()
            );
            final Snapshot snapshot = new Snapshot(repository.getMetadata().name(), new SnapshotId(randomAlphaOfLength(10), "_uuid"));
            final ShardGeneration shardGen = snapshotShard(shard, snapshot, repository);
            final Store.MetadataSnapshot storeFiles = shard.snapshotStoreMetadata();

            final List<BlobStoreIndexShardSnapshot.FileInfo> moved = BlobStoreTestUtil.moveFilesIntoCopiedBlob(
                repository,
                indexId,
                shardId.id(),
                shardGen,
                snapshot.getSnapshotId(),
                filesToMove,
                withFiller
            );
            if (corrupt) {
                assertFalse("no file was moved", moved.isEmpty());
                final BlobStoreIndexShardSnapshot.FileInfo victim = randomFrom(moved);
                final BlobContainer shardContainer = repository.shardContainer(indexId, shardId.id());
                final byte[] copiedBytes;
                try (InputStream in = shardContainer.readBlob(OperationPurpose.SNAPSHOT_DATA, victim.blob())) {
                    copiedBytes = in.readAllBytes();
                }
                final int position = Math.toIntExact(victim.offset() + randomLongBetween(0, victim.length() - 1));
                copiedBytes[position] = (byte) (copiedBytes[position] ^ (1 << between(0, 7)));
                shardContainer.writeBlob(OperationPurpose.SNAPSHOT_DATA, victim.blob(), new BytesArray(copiedBytes), false);
            }

            // restore into an empty store, so that every file comes from the repository
            closeShards(shard);
            try (var files = Files.list(shard.shardPath().resolveIndex())) {
                for (Path file : files.toList()) {
                    if (file.getFileName().toString().equals("write.lock") == false) {
                        Files.delete(file);
                    }
                }
            }
            final ShardRouting shardRouting = ShardRoutingHelper.initWithSameId(
                shard.routingEntry(),
                new RecoverySource.SnapshotRecoverySource(UUIDs.randomBase64UUID(), snapshot, IndexVersion.current(), indexId)
            );
            shard = newShard(
                shardRouting,
                null,
                shard.shardPath(),
                shard.indexSettings().getIndexMetadata(),
                null,
                null,
                new InternalEngineFactory(),
                NOOP_GCP_SYNCER,
                RetentionLeaseSyncer.EMPTY,
                IndexEventListener.NOOP
            );

            if (corrupt) {
                final IndexShard restoringShard = shard;
                final Exception e = expectThrows(Exception.class, () -> recoverShardFromSnapshot(restoringShard, snapshot, repository));
                assertNotNull("expected a corruption error but got " + e, ExceptionsHelper.unwrap(e, CorruptIndexException.class));
                assertTrue(shard.store().isMarkedCorrupted());
            } else {
                recoverShardFromSnapshot(shard, snapshot, repository);
                TestUtil.checkIndex(shard.store().directory());
                final Store.MetadataSnapshot restoredFiles = shard.store().getMetadata(null);
                for (StoreFileMetadata storeFile : storeFiles) {
                    final StoreFileMetadata restoredFile = restoredFiles.get(storeFile.name());
                    assertNotNull("file [" + storeFile.name() + "] was not restored", restoredFile);
                    assertTrue("file [" + storeFile.name() + "] differs after restore", restoredFile.isSame(storeFile));
                }
            }
        } finally {
            if (shard != null && shard.state() != IndexShardState.CLOSED) {
                try {
                    closeShardNoCheck(shard);
                } finally {
                    IOUtils.close(shard.store());
                }
            }
        }
    }

    public void testSnapshotWithConflictingName() throws Exception {
        final IndexId indexId = new IndexId(randomAlphaOfLength(10), UUIDs.randomBase64UUID());
        final ShardId shardId = new ShardId(indexId.getName(), indexId.getId(), 0);

        IndexShard shard = newShard(shardId, true);
        try {
            // index documents in the shards
            final int numDocs = scaledRandomIntBetween(1, 500);
            recoverShardFromStore(shard);
            for (int i = 0; i < numDocs; i++) {
                indexDoc(shard, "_doc", Integer.toString(i));
                if (rarely()) {
                    flushShard(shard, false);
                }
            }
            assertDocCount(shard, numDocs);

            // snapshot the shard
            @FixForMultiProject(description = "randomize when snapshot and restore support multiple projects, see also ES-10225, ES-10228")
            final Repository repository = TestUtils.createRepository(ProjectId.DEFAULT, createTempDir(), xContentRegistry());
            final Snapshot snapshot = new Snapshot(repository.getMetadata().name(), new SnapshotId(randomAlphaOfLength(10), "_uuid"));
            final ShardGeneration shardGen = snapshotShard(shard, snapshot, repository);
            assertNotNull(shardGen);
            final Snapshot snapshotWithSameName = new Snapshot(
                repository.getMetadata().name(),
                new SnapshotId(snapshot.getSnapshotId().getName(), "_uuid2")
            );
            final var snapshotShardGenerations = new UpdatedShardGenerations(
                ShardGenerations.builder().put(indexId, 0, shardGen).build(),
                ShardGenerations.EMPTY
            );
            final RepositoryData ignoredRepositoryData = safeAwait(
                listener -> repository.finalizeSnapshot(
                    new FinalizeSnapshotContext(
                        false,
                        snapshotShardGenerations,
                        RepositoryData.EMPTY_REPO_GEN,
                        Metadata.builder().put(shard.indexSettings().getIndexMetadata(), false).build(),
                        new SnapshotInfo(
                            snapshot,
                            snapshotShardGenerations.liveIndices().indices().stream().map(IndexId::getName).toList(),
                            Collections.emptyList(),
                            Collections.emptyList(),
                            null,
                            1L,
                            6,
                            Collections.emptyList(),
                            true,
                            Collections.emptyMap(),
                            0L,
                            Collections.emptyMap()
                        ),
                        IndexVersion.current(),
                        listener,
                        () -> {}
                    )
                )
            );
            IndexShardSnapshotFailedException isfe = expectThrows(
                IndexShardSnapshotFailedException.class,
                () -> snapshotShard(shard, snapshotWithSameName, repository)
            );
            assertThat(isfe.getMessage(), containsString("Duplicate snapshot name"));
        } finally {
            if (shard != null && shard.state() != IndexShardState.CLOSED) {
                try {
                    closeShardNoCheck(shard);
                } finally {
                    IOUtils.close(shard.store());
                }
            }
        }
    }
}
