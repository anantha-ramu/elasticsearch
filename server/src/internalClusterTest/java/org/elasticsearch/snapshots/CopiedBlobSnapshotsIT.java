/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.snapshots;

import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.repositories.blobstore.BlobStoreTestUtil;

import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

/**
 * Snapshot clone, delete and restore of shard snapshots whose files are stored as byte ranges of copied blobs. Nothing creates copied
 * blobs yet, so the test rewrites a finished shard snapshot into that layout.
 */
public class CopiedBlobSnapshotsIT extends AbstractSnapshotIntegTestCase {

    public void testDeleteKeepsCopiedBlobWhileReferencedAndRemovesTheRest() throws Exception {
        final String repoName = "test-repo";
        createRepository(repoName, "fs");
        final String indexName = "test-index";
        createIndex(indexName, indexSettingsNoReplicas(1).build());
        long docCount = 0;
        for (int segment = between(2, 4); segment > 0; segment--) {
            docCount += indexDocsAndFlush(indexName);
        }
        createSnapshot(repoName, "snap-1", List.of(indexName));

        // store the data files of snap-1 in one copied blob, as a server-side copy of a commit blob would
        final BlobStoreRepository repository = getRepositoryOnMaster(repoName);
        final RepositoryData repositoryData = getRepositoryData(repoName);
        final IndexId indexId = repositoryData.resolveIndexId(indexName);
        final List<BlobStoreIndexShardSnapshot.FileInfo> moved = BlobStoreTestUtil.moveFilesIntoCopiedBlob(
            repository,
            indexId,
            0,
            repositoryData.shardGenerations().getShardGen(indexId, 0),
            snapshotId(repositoryData, "snap-1"),
            fileInfo -> true,
            randomBoolean()
        );
        assertFalse("no file was moved", moved.isEmpty());
        final String copiedBlob = moved.get(0).blob();
        final BlobContainer shardContainer = repository.shardContainer(indexId, 0);

        // a clone refers to the same copied blob, so deleting the original snapshot keeps the blob, and the clone restores from it
        assertAcked(clusterAdmin().prepareCloneSnapshot(TEST_REQUEST_TIMEOUT, repoName, "snap-1", "clone-1").setIndices(indexName).get());
        deleteSnapshotAndAwaitCleanup(repoName, repositoryData, "snap-1", shardContainer);
        assertTrue(shardContainer.blobExists(OperationPurpose.SNAPSHOT_DATA, copiedBlob));
        assertRestoredDocCount(repoName, "clone-1", indexName, docCount);

        // a copied blob that no file refers to, like one a failed snapshot leaves behind
        final String orphan = BlobStoreRepository.COPIED_DATA_BLOB_PREFIX + UUIDs.randomBase64UUID(random());
        shardContainer.writeBlob(OperationPurpose.SNAPSHOT_DATA, orphan, new BytesArray(randomByteArrayOfLength(between(1, 1024))), true);

        // snap-2 shares no file with clone-1: more documents and a force merge replace every segment
        docCount += indexDocsAndFlush(indexName);
        assertThat(indicesAdmin().prepareForceMerge(indexName).setMaxNumSegments(1).setFlush(true).get().getFailedShards(), equalTo(0));
        createSnapshot(repoName, "snap-2", List.of(indexName));

        // snap-2 survives the delete of clone-1, so delete processes the shard and removes both copied blobs, which nothing refers to now
        deleteSnapshotAndAwaitCleanup(repoName, getRepositoryData(repoName), "clone-1", shardContainer);
        assertFalse(shardContainer.blobExists(OperationPurpose.SNAPSHOT_DATA, copiedBlob));
        assertFalse(shardContainer.blobExists(OperationPurpose.SNAPSHOT_DATA, orphan));
        assertRestoredDocCount(repoName, "snap-2", indexName, docCount);
    }

    /**
     * Deletes a snapshot and waits until its shard-level blobs are cleaned up. The delete responds before that cleanup runs, so wait for
     * the snapshot's own shard snapshot blob to go, then for the master to finish the rest of the cleanup.
     */
    private void deleteSnapshotAndAwaitCleanup(
        String repoName,
        RepositoryData repositoryData,
        String snapshotName,
        BlobContainer shardContainer
    ) throws Exception {
        final String shardSnapshotBlob = Strings.format(
            BlobStoreRepository.SNAPSHOT_NAME_FORMAT,
            snapshotId(repositoryData, snapshotName).getUUID()
        );
        assertTrue(shardContainer.blobExists(OperationPurpose.SNAPSHOT_METADATA, shardSnapshotBlob));
        assertAcked(startDeleteSnapshot(repoName, snapshotName).get());
        assertBusy(() -> assertFalse(shardContainer.blobExists(OperationPurpose.SNAPSHOT_METADATA, shardSnapshotBlob)));
        awaitMasterFinishRepoOperations();
    }

    private long indexDocsAndFlush(String indexName) {
        final int docs = between(1, 50);
        for (int i = 0; i < docs; i++) {
            prepareIndex(indexName).setSource("field", randomAlphaOfLength(10)).get();
        }
        flushAndRefresh(indexName);
        return docs;
    }

    private static SnapshotId snapshotId(RepositoryData repositoryData, String snapshotName) {
        return repositoryData.getSnapshotIds().stream().filter(id -> id.getName().equals(snapshotName)).findFirst().orElseThrow();
    }

    private void assertRestoredDocCount(String repoName, String snapshotName, String indexName, long expectedDocCount) {
        final String restoredIndex = "restored-" + snapshotName;
        final RestoreSnapshotResponse restoreResponse = clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, repoName, snapshotName)
            .setIndices(indexName)
            .setRenamePattern(indexName)
            .setRenameReplacement(restoredIndex)
            .setWaitForCompletion(true)
            .get();
        assertThat(restoreResponse.getRestoreInfo().failedShards(), equalTo(0));
        assertDocCount(restoredIndex, expectedDocCount);
        assertAcked(indicesAdmin().prepareDelete(restoredIndex));
    }
}
