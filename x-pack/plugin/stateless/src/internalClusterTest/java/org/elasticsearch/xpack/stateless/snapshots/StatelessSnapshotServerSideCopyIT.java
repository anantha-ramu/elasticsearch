/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots;

import org.apache.logging.log4j.Level;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.repositories.blobstore.ESBlobStoreRepositoryIntegTestCase;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPoolStats;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.elasticsearch.xpack.stateless.StatelessMockRepository;
import org.elasticsearch.xpack.stateless.StatelessMockRepositoryPlugin;
import org.elasticsearch.xpack.stateless.StatelessMockRepositoryStrategy;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;
import org.elasticsearch.xpack.stateless.objectstore.gc.ObjectStoreGCTask;
import org.elasticsearch.xpack.stateless.reshard.ReshardIndexRequest;
import org.elasticsearch.xpack.stateless.reshard.TransportReshardAction;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.xpack.stateless.reshard.SplitSourceService.RESHARD_SPLIT_DELETE_UNOWNED_GRACE_PERIOD;
import static org.elasticsearch.xpack.stateless.snapshots.StatelessSnapshotSettings.RELOCATION_DURING_SNAPSHOT_ENABLED_SETTING;
import static org.elasticsearch.xpack.stateless.snapshots.StatelessSnapshotSettings.SERVER_SIDE_COPY_ENABLED_SETTING;
import static org.elasticsearch.xpack.stateless.snapshots.StatelessSnapshotSettings.STATELESS_SNAPSHOT_ENABLED_SETTING;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.not;

/**
 * Shard snapshots that copy stateless commit blobs into the repository server-side, and the snapshot operations that read them: restore,
 * clone, delete, a failure part way through, a primary that moves during a snapshot, and a snapshot of a split target.
 */
public class StatelessSnapshotServerSideCopyIT extends AbstractStatelessPluginIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final var plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(StatelessMockRepositoryPlugin.class);
        plugins.add(InternalSettingsPlugin.class);
        return List.copyOf(plugins);
    }

    @Override
    protected boolean addMockFsRepository() {
        return false;
    }

    /**
     * Settings that turn copies on. The object store is randomly the mock one, whose containers are wrapped, or plain FS. Most tests
     * snapshot into an FS repository, whose containers are plain, so copies run with matching and with mismatched wrapping. The tests that
     * snapshot into the mock repository, to make copies fail or wait, have wrapped containers on that side too.
     */
    private static Settings.Builder copySettings() {
        return Settings.builder()
            .put(STATELESS_SNAPSHOT_ENABLED_SETTING.getKey(), randomFrom("read_from_object_store", "enabled"))
            .put(SERVER_SIDE_COPY_ENABLED_SETTING.getKey(), true)
            .put(
                ObjectStoreService.TYPE_SETTING.getKey(),
                randomFrom(ObjectStoreService.ObjectStoreType.FS, ObjectStoreService.ObjectStoreType.MOCK)
            );
    }

    public void testSnapshotAndRestoreCopyCommitBlobs() throws Exception {
        final Settings settings = copySettings().build();
        startMasterAndIndexNode(settings);
        startSearchNode(settings);
        final String indexName = randomIdentifier();
        createIndex(indexName, between(1, 3), 1);
        ensureGreen(indexName);
        final long docCount = indexDocsAndFlush(indexName, between(1, 3));

        final String repoName = randomIdentifier();
        createRepository(repoName, "fs");
        // at least one shard snapshot of the index logs a copy stats line that shows no uploaded file
        MockLog.assertThatLogger(
            () -> createSnapshot(repoName, "snap", List.of(indexName), List.of()),
            BlobStoreRepository.class,
            new MockLog.SeenEventExpectation(
                "copy stats",
                BlobStoreRepository.class.getCanonicalName(),
                Level.INFO,
                "["
                    + repoName
                    + "][snap/*]["
                    + indexName
                    + "][*] shard snapshot copy stats: new copies [*], copied bytes [*], total copy time [*ms], slowest copy [*ms],"
                    + " files in new copies [*], reused copies [0], files in reused copies [0], files reused by name, uploaded [0],"
                    + " bytes reused by name, uploaded [0], files reused by name, copied [0], bytes reused by name, copied [0],"
                    + " uploaded files [0], uploaded bytes [0], dead bytes [*], dead bytes per new copy [*]"
            )
        );
        assertAllDataFilesCopied(repoName, indexName, "snap");

        assertAcked(indicesAdmin().prepareDelete(indexName));
        restore(repoName, "snap", indexName, indexName);
        assertHitCount(prepareSearch(indexName), docCount);
        // the restored index keeps working
        indexDocs(indexName, between(1, 50));
    }

    public void testSnapshotMixesUploadedAndCopiedFiles() throws Exception {
        final Settings settings = copySettings().put(SERVER_SIDE_COPY_ENABLED_SETTING.getKey(), false).build();
        startMasterAndIndexNode(settings);
        startSearchNode(settings);
        final String indexName = randomIdentifier();
        // no merges, so that the first snapshot's segments are still there for the second snapshot to keep
        createIndex(indexName, indexSettings(1, 1).put(InternalSettingsPlugin.MERGE_ENABLED.getKey(), false).build());
        ensureGreen(indexName);
        long docCount = indexDocsAndFlush(indexName, between(1, 2));
        final String repoName = randomIdentifier();
        createRepository(repoName, "fs");
        createSnapshot(repoName, "uploaded", List.of(indexName), List.of());

        // the second snapshot keeps the uploaded files that did not change and copies the commit blobs of the new ones
        docCount += indexDocsAndFlush(indexName, 1);
        updateClusterSettings(Settings.builder().put(SERVER_SIDE_COPY_ENABLED_SETTING.getKey(), true));
        createSnapshot(repoName, "mixed", List.of(indexName), List.of());
        final List<FileInfo> dataFiles = dataFiles(shardSnapshot(repoName, indexName, "mixed", 0));
        assertTrue("no copied file in " + dataFiles, dataFiles.stream().anyMatch(FileInfo::isCopied));
        assertTrue("no uploaded file in " + dataFiles, dataFiles.stream().anyMatch(fileInfo -> fileInfo.isCopied() == false));

        assertAcked(indicesAdmin().prepareDelete(indexName));
        restore(repoName, "mixed", indexName, indexName);
        assertHitCount(prepareSearch(indexName), docCount);
    }

    public void testLaterSnapshotCopiesOnlyNewCommitBlobs() throws Exception {
        final Settings settings = copySettings().build();
        startMasterAndIndexNode(settings);
        startSearchNode(settings);
        final String indexName = randomIdentifier();
        createIndex(indexName, 1, 1);
        ensureGreen(indexName);
        long docCount = indexDocsAndFlush(indexName, between(1, 2));
        final String repoName = randomIdentifier();
        createRepository(repoName, "fs");
        createSnapshot(repoName, "first", List.of(indexName), List.of());
        final Map<String, FileInfo> firstFiles = byName(dataFiles(shardSnapshot(repoName, indexName, "first", 0)));
        final Set<String> firstCopies = copiedBlobs(repoName, indexName, 0);

        docCount += indexDocsAndFlush(indexName, 1);
        createSnapshot(repoName, "second", List.of(indexName), List.of());
        final List<FileInfo> secondFiles = dataFiles(shardSnapshot(repoName, indexName, "second", 0));
        final Map<String, String> copyBySource = new HashMap<>();
        for (FileInfo fileInfo : secondFiles) {
            assertTrue(fileInfo + " is not copied", fileInfo.isCopied());
            // a file the first snapshot already holds keeps its file info, and so its copy
            final FileInfo earlier = firstFiles.get(fileInfo.name());
            if (earlier != null) {
                assertTrue(earlier.isSame(fileInfo));
            }
            // each commit blob is copied once
            assertThat(copyBySource.computeIfAbsent(fileInfo.source(), ignored -> fileInfo.blob()), equalTo(fileInfo.blob()));
        }
        final Set<String> secondCopies = copiedBlobs(repoName, indexName, 0);
        assertTrue(secondCopies.containsAll(firstCopies));
        assertThat(secondCopies, equalTo(Set.copyOf(copyBySource.values())));

        assertAcked(indicesAdmin().prepareDelete(indexName));
        restore(repoName, "second", indexName, indexName);
        assertHitCount(prepareSearch(indexName), docCount);
    }

    public void testFailedCopyLeavesEarlierSnapshotRestorable() throws Exception {
        final Settings settings = copySettings().build();
        final String indexNode = startMasterAndIndexNode(settings);
        startSearchNode(settings);
        final String indexName = randomIdentifier();
        createIndex(indexName, 1, 1);
        ensureGreen(indexName);
        final long firstDocCount = indexDocsAndFlush(indexName, between(1, 2));

        // the snapshot repository is the mock one, so that copies into it can be made to fail
        final String repoName = randomIdentifier();
        createRepository(repoName, StatelessMockRepositoryPlugin.TYPE);
        final AtomicBoolean failCopies = new AtomicBoolean();
        snapshotRepositoryOnNode(repoName, indexNode).setStrategy(new StatelessMockRepositoryStrategy() {
            @Override
            public void blobContainerCopyBlob(
                CheckedRunnable<IOException> originalRunnable,
                OperationPurpose purpose,
                BlobContainer sourceBlobContainer,
                String sourceBlobName,
                String blobName,
                long blobSize
            ) throws IOException {
                if (failCopies.get() && blobName.startsWith(BlobStoreRepository.COPIED_DATA_BLOB_PREFIX)) {
                    throw new IOException("simulated copy failure");
                }
                originalRunnable.run();
            }
        });
        createSnapshot(repoName, "first", List.of(indexName), List.of());
        final Set<String> firstCopies = copiedBlobs(repoName, indexName, 0);
        assertThat(firstCopies, not(empty()));

        // the second snapshot needs new copies, which fail, so its shard snapshot fails
        indexDocsAndFlush(indexName, 1);
        failCopies.set(true);
        final CreateSnapshotResponse failed = clusterAdmin().prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, "second")
            .setIndices(indexName)
            .setWaitForCompletion(true)
            .setPartial(true)
            .get();
        failCopies.set(false);
        assertThat(failed.getSnapshotInfo().failedShards(), equalTo(1));

        // the failure deleted no copy the first snapshot refers to, and left no copy of its own behind
        assertThat(copiedBlobs(repoName, indexName, 0), equalTo(firstCopies));
        restore(repoName, "first", indexName, "restored");
        assertHitCount(prepareSearch("restored"), firstDocCount);
    }

    public void testCloneKeepsCopiesUntilDeleted() throws Exception {
        final Settings settings = copySettings().build();
        startMasterAndIndexNode(settings);
        startSearchNode(settings);
        final String indexName = randomIdentifier();
        createIndex(indexName, 1, 1);
        ensureGreen(indexName);
        final long firstDocCount = indexDocsAndFlush(indexName, between(1, 2));
        final String repoName = randomIdentifier();
        createRepository(repoName, "fs");
        createSnapshot(repoName, "first", List.of(indexName), List.of());
        final Set<String> firstCopies = copiedBlobs(repoName, indexName, 0);
        assertThat(firstCopies, not(empty()));

        // a clone refers to the same copies, so deleting the original keeps them, and the clone restores from them
        assertAcked(clusterAdmin().prepareCloneSnapshot(TEST_REQUEST_TIMEOUT, repoName, "first", "clone").setIndices(indexName).get());
        deleteSnapshotAndAwaitCleanup(repoName, indexName, "first");
        assertTrue(copiedBlobs(repoName, indexName, 0).containsAll(firstCopies));
        restore(repoName, "clone", indexName, "restored-clone");
        assertHitCount(prepareSearch("restored-clone"), firstDocCount);

        // Take a later snapshot that shares no file with the clone: more documents and a force merge replace every segment. Deleting the
        // clone then leaves that later snapshot, so the delete processes the shard and removes the old copies, which nothing refers to.
        final long laterDocCount = firstDocCount + indexDocsAndFlush(indexName, 1);
        assertThat(indicesAdmin().prepareForceMerge(indexName).setMaxNumSegments(1).setFlush(true).get().getFailedShards(), equalTo(0));
        createSnapshot(repoName, "later", List.of(indexName), List.of());
        deleteSnapshotAndAwaitCleanup(repoName, indexName, "clone");
        final Set<String> remaining = copiedBlobs(repoName, indexName, 0);
        assertTrue("old copies " + firstCopies + " are still in " + remaining, firstCopies.stream().noneMatch(remaining::contains));
        restore(repoName, "later", indexName, "restored-later");
        assertHitCount(prepareSearch("restored-later"), laterDocCount);
    }

    public void testRestoreAfterSourceBlobsAreGone() throws Exception {
        final Settings settings = copySettings().put(ObjectStoreGCTask.GC_INTERVAL_SETTING.getKey(), "1s").build();
        final String indexNode = startMasterAndIndexNode(settings);
        startSearchNode(settings);
        final String indexName = randomIdentifier();
        final int shards = between(1, 2);
        createIndex(indexName, shards, 1);
        ensureGreen(indexName);
        final long docCount = indexDocsAndFlush(indexName, between(1, 2));
        final String repoName = randomIdentifier();
        createRepository(repoName, "fs");
        createSnapshot(repoName, "snap", List.of(indexName), List.of());

        // Delete the index and wait until garbage collection removes its commit blobs from the object store. Restore then has nothing
        // to read but the copies, so a restore that read from the source paths would fail.
        final Index index = resolveIndex(indexName);
        assertAcked(indicesAdmin().prepareDelete(indexName));
        final ObjectStoreService objectStore = getObjectStoreService(indexNode);
        for (int shard = 0; shard < shards; shard++) {
            final BlobContainer shardContainer = objectStore.getProjectBlobContainer(new ShardId(index, shard));
            assertBusy(() -> assertFalse(hasBlobs(shardContainer)));
        }
        restore(repoName, "snap", indexName, indexName);
        assertHitCount(prepareSearch(indexName), docCount);
    }

    public void testPrimaryMovesWhileCopying() throws Exception {
        final Settings settings = copySettings().put(STATELESS_SNAPSHOT_ENABLED_SETTING.getKey(), "enabled")
            .put(RELOCATION_DURING_SNAPSHOT_ENABLED_SETTING.getKey(), true)
            .build();
        final String firstIndexNode = startMasterAndIndexNode(settings);
        startSearchNode(settings);
        final String indexName = randomIdentifier();
        createIndex(indexName, 1, 1);
        ensureGreen(indexName);
        final long docCount = indexDocsAndFlush(indexName, between(1, 2));

        // block the first copy into the snapshot repository until the primary has moved to another node
        final String repoName = randomIdentifier();
        createRepository(repoName, StatelessMockRepositoryPlugin.TYPE);
        final CountDownLatch copyStarted = new CountDownLatch(1);
        final CountDownLatch releaseCopy = new CountDownLatch(1);
        snapshotRepositoryOnNode(repoName, firstIndexNode).setStrategy(new StatelessMockRepositoryStrategy() {
            @Override
            public void blobContainerCopyBlob(
                CheckedRunnable<IOException> originalRunnable,
                OperationPurpose purpose,
                BlobContainer sourceBlobContainer,
                String sourceBlobName,
                String blobName,
                long blobSize
            ) throws IOException {
                if (blobName.startsWith(BlobStoreRepository.COPIED_DATA_BLOB_PREFIX)) {
                    copyStarted.countDown();
                    safeAwait(releaseCopy);
                }
                originalRunnable.run();
            }
        });
        final var snapshotFuture = clusterAdmin().prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, "snap")
            .setIndices(indexName)
            .setWaitForCompletion(true)
            .execute();
        safeAwait(copyStarted);

        final String secondIndexNode = startIndexNode(settings);
        final String secondIndexNodeId = internalCluster().clusterService(secondIndexNode).localNode().getId();
        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", firstIndexNode), indexName);
        awaitClusterState(state -> {
            final ShardRouting primary = state.routingTable().index(indexName).shard(0).primaryShard();
            return primary.started() && primary.currentNodeId().equals(secondIndexNodeId);
        });
        releaseCopy.countDown();

        final SnapshotInfo snapshotInfo = safeGet(snapshotFuture).getSnapshotInfo();
        assertThat(snapshotInfo.state(), equalTo(SnapshotState.SUCCESS));
        assertAllDataFilesCopied(repoName, indexName, "snap");
        restore(repoName, "snap", indexName, "restored");
        assertHitCount(prepareSearch("restored"), docCount);
    }

    public void testSnapshotOfSplitTargets() throws Exception {
        // the split completes only after the source deletes the documents it no longer owns, so do not wait for a grace period
        final Settings settings = copySettings().put(RESHARD_SPLIT_DELETE_UNOWNED_GRACE_PERIOD.getKey(), TimeValue.ZERO).build();
        startMasterAndIndexNode(settings);
        startSearchNode(settings);
        final String indexName = randomIdentifier();
        createIndex(indexName, between(1, 2), 1);
        ensureGreen(indexName);
        final long docCount = indexDocsAndFlush(indexName, between(1, 2));

        // split every shard; the targets start with copies of their source's commit blobs in their own folders
        final Index index = resolveIndex(indexName);
        client().execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName)).actionGet();
        awaitClusterState(state -> state.metadata().projectFor(index).index(indexName).getReshardingMetadata() == null);
        ensureGreen(indexName);

        final String repoName = randomIdentifier();
        createRepository(repoName, "fs");
        createSnapshot(repoName, "snap", List.of(indexName), List.of());
        assertAllDataFilesCopied(repoName, indexName, "snap");

        assertAcked(indicesAdmin().prepareDelete(indexName));
        restore(repoName, "snap", indexName, indexName);
        assertHitCount(prepareSearch(indexName), docCount);
    }

    private long indexDocsAndFlush(String indexName, int segments) {
        long docCount = 0;
        for (int segment = 0; segment < segments; segment++) {
            final int docs = between(10, 100);
            indexDocs(indexName, docs);
            flush(indexName);
            docCount += docs;
        }
        return docCount;
    }

    private void restore(String repoName, String snapshotName, String indexName, String restoredName) {
        final RestoreSnapshotResponse response = clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, repoName, snapshotName)
            .setIndices(indexName)
            .setRenamePattern(indexName)
            .setRenameReplacement(restoredName)
            .setWaitForCompletion(true)
            .get();
        assertThat(response.getRestoreInfo().failedShards(), equalTo(0));
        assertThat(response.getRestoreInfo().successfulShards(), greaterThan(0));
        ensureGreen(restoredName);
    }

    /**
     * Checks that in every shard of the snapshot, each data file is a byte range of a copied blob and no file was uploaded on its own.
     */
    private void assertAllDataFilesCopied(String repoName, String indexName, String snapshotName) throws IOException {
        final BlobStoreRepository repository = repository(repoName);
        final RepositoryData repositoryData = ESBlobStoreRepositoryIntegTestCase.getRepositoryData(repository);
        final IndexId indexId = repositoryData.resolveIndexId(indexName);
        for (int shard = 0; shard < getNumShards(indexName).numPrimaries; shard++) {
            final List<FileInfo> dataFiles = dataFiles(shardSnapshot(repoName, indexName, snapshotName, shard));
            final Set<String> referencedCopies = new HashSet<>();
            for (FileInfo fileInfo : dataFiles) {
                assertTrue(fileInfo + " is not copied", fileInfo.isCopied());
                referencedCopies.add(fileInfo.blob());
            }
            final Set<String> blobs = repository.shardContainer(indexId, shard).listBlobs(OperationPurpose.SNAPSHOT_METADATA).keySet();
            assertThat(blobs.stream().filter(name -> name.startsWith(BlobStoreRepository.UPLOADED_DATA_BLOB_PREFIX)).toList(), empty());
            assertThat(copiedBlobs(repoName, indexName, shard), equalTo(referencedCopies));
        }
    }

    /**
     * Deletes a snapshot and waits until its shard-level blobs are cleaned up. The delete API responds before those blobs are deleted. So
     * wait until the deleted snapshot's {@code snap-<uuid>.dat} blob in the shard folder is gone, which shows the cleanup has started, and
     * then until the master's snapshot thread pool is idle, which shows it has finished.
     */
    private void deleteSnapshotAndAwaitCleanup(String repoName, String indexName, String snapshotName) throws Exception {
        final BlobStoreRepository repository = repository(repoName);
        final RepositoryData repositoryData = ESBlobStoreRepositoryIntegTestCase.getRepositoryData(repository);
        final BlobContainer shardContainer = repository.shardContainer(repositoryData.resolveIndexId(indexName), 0);
        final String shardSnapshotBlob = Strings.format(
            BlobStoreRepository.SNAPSHOT_NAME_FORMAT,
            snapshotId(repositoryData, snapshotName).getUUID()
        );
        assertAcked(clusterAdmin().prepareDeleteSnapshot(TEST_REQUEST_TIMEOUT, repoName, snapshotName).get());
        assertBusy(() -> assertFalse(shardContainer.blobExists(OperationPurpose.SNAPSHOT_METADATA, shardSnapshotBlob)));
        final String masterName = internalCluster().getMasterName();
        assertBusy(() -> {
            for (ThreadPoolStats.Stats stats : internalCluster().getInstance(ThreadPool.class, masterName).stats()) {
                if (stats.name().equals(ThreadPool.Names.SNAPSHOT)) {
                    assertThat(stats.active(), equalTo(0));
                }
            }
        });
    }

    /**
     * The snapshot repository's own mock instance on a node. Its strategy sees the copies that node makes into the repository, unlike the
     * strategy of the object store's mock repository.
     */
    private static StatelessMockRepository snapshotRepositoryOnNode(String repoName, String nodeName) {
        return (StatelessMockRepository) internalCluster().getInstance(RepositoriesService.class, nodeName).repository(repoName);
    }

    private static BlobStoreRepository repository(String repoName) {
        return (BlobStoreRepository) internalCluster().getCurrentMasterNodeInstance(RepositoriesService.class).repository(repoName);
    }

    private static SnapshotId snapshotId(RepositoryData repositoryData, String snapshotName) {
        return repositoryData.getSnapshotIds().stream().filter(id -> id.getName().equals(snapshotName)).findFirst().orElseThrow();
    }

    private BlobStoreIndexShardSnapshot shardSnapshot(String repoName, String indexName, String snapshotName, int shard) {
        final BlobStoreRepository repository = repository(repoName);
        final RepositoryData repositoryData = ESBlobStoreRepositoryIntegTestCase.getRepositoryData(repository);
        return repository.loadShardSnapshot(
            repository.shardContainer(repositoryData.resolveIndexId(indexName), shard),
            snapshotId(repositoryData, snapshotName)
        );
    }

    private Set<String> copiedBlobs(String repoName, String indexName, int shard) throws IOException {
        final BlobStoreRepository repository = repository(repoName);
        final IndexId indexId = ESBlobStoreRepositoryIntegTestCase.getRepositoryData(repository).resolveIndexId(indexName);
        return repository.shardContainer(indexId, shard)
            .listBlobs(OperationPurpose.SNAPSHOT_METADATA)
            .keySet()
            .stream()
            .filter(name -> name.startsWith(BlobStoreRepository.COPIED_DATA_BLOB_PREFIX))
            .collect(Collectors.toSet());
    }

    private static List<FileInfo> dataFiles(BlobStoreIndexShardSnapshot shardSnapshot) {
        return shardSnapshot.indexFiles().stream().filter(fileInfo -> fileInfo.metadata().hashEqualsContents() == false).toList();
    }

    private static Map<String, FileInfo> byName(List<FileInfo> fileInfos) {
        return fileInfos.stream().collect(Collectors.toMap(FileInfo::name, fileInfo -> fileInfo));
    }

    private static boolean hasBlobs(BlobContainer container) throws IOException {
        try {
            if (container.listBlobs(OperationPurpose.INDICES).isEmpty() == false) {
                return true;
            }
            for (BlobContainer child : container.children(OperationPurpose.INDICES).values()) {
                if (hasBlobs(child)) {
                    return true;
                }
            }
            return false;
        } catch (NoSuchFileException e) {
            // garbage collection removed the folder itself
            return false;
        }
    }
}
