/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots;

import org.apache.logging.log4j.Level;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.cluster.SnapshotDeletionsInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshotsIntegritySuppressor;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.repositories.blobstore.BlobStoreTestUtil;
import org.elasticsearch.repositories.blobstore.ESBlobStoreRepositoryIntegTestCase;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.NodeShutdownTestUtils;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPoolStats;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.elasticsearch.xpack.stateless.StatelessMockRepository;
import org.elasticsearch.xpack.stateless.StatelessMockRepositoryPlugin;
import org.elasticsearch.xpack.stateless.StatelessMockRepositoryStrategy;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitService;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;
import org.elasticsearch.xpack.stateless.objectstore.gc.ObjectStoreGCTask;
import org.elasticsearch.xpack.stateless.reshard.ReshardIndexRequest;
import org.elasticsearch.xpack.stateless.reshard.TransportReshardAction;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.xpack.stateless.reshard.SplitSourceService.RESHARD_SPLIT_DELETE_UNOWNED_GRACE_PERIOD;
import static org.elasticsearch.xpack.stateless.snapshots.StatelessSnapshotSettings.RELOCATION_DURING_SNAPSHOT_ENABLED_SETTING;
import static org.elasticsearch.xpack.stateless.snapshots.StatelessSnapshotSettings.SERVER_SIDE_COPY_ENABLED_SETTING;
import static org.elasticsearch.xpack.stateless.snapshots.StatelessSnapshotSettings.STATELESS_SNAPSHOT_ENABLED_SETTING;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

/**
 * Shard snapshots that copy stateless commit blobs into the repository server-side, and the snapshot operations that read them: restore,
 * clone, delete, a failure part way through, a primary that moves during a snapshot, and a snapshot of a split target.
 * <p>
 * Terms used in the tests:
 * <ul>
 * <li>A commit blob is a blob in the object store that holds the files of one or more commits of a shard.</li>
 * <li>A copied blob, {@code c__<uuid>}, is a copy of a whole commit blob that a snapshot makes inside the repository.</li>
 * <li>An uploaded blob, {@code __<uuid>}, holds one file that a snapshot writes to the repository itself.</li>
 * <li>A file info is the repository's record of one file of a shard snapshot. For a copied file it names the copied blob and the byte
 * range of the file in it.</li>
 * <li>A shard snapshot blob, {@code snap-<uuid>.dat}, lists the file infos of one shard snapshot.</li>
 * <li>A shard generation, {@code index-<uuid>}, lists the file infos of all the snapshots of one shard.</li>
 * <li>The repository data is the root {@code index-N} blob. It lists the snapshots, the indices and the current shard generation of every
 * shard.</li>
 * <li>Finalization is the last step of a snapshot. After all shard snapshots end, the master writes the snapshot's metadata and the
 * repository data.</li>
 * <li>TLA+ is a language for describing a design so that a tool can check every order of its steps. Two tests replay sequences that a
 * TLA+ model of the copies found. The model is not part of this repository.</li>
 * </ul>
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
        assertAcked(clusterAdmin().prepareDeleteSnapshot(TEST_REQUEST_TIMEOUT, repoName, "first").get());
        assertTrue(copiedBlobs(repoName, indexName, 0).containsAll(firstCopies));
        restore(repoName, "clone", indexName, "restored-clone");
        assertHitCount(prepareSearch("restored-clone"), firstDocCount);

        // Take a later snapshot that shares no file with the clone: more documents and a force merge replace every segment. Deleting the
        // clone then leaves that later snapshot, so the delete processes the shard and removes the old copies, which nothing refers to.
        final long laterDocCount = firstDocCount + indexDocsAndFlush(indexName, 1);
        assertThat(indicesAdmin().prepareForceMerge(indexName).setMaxNumSegments(1).setFlush(true).get().getFailedShards(), equalTo(0));
        createSnapshot(repoName, "later", List.of(indexName), List.of());
        assertAcked(clusterAdmin().prepareDeleteSnapshot(TEST_REQUEST_TIMEOUT, repoName, "clone").get());
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

    /**
     * Takes snapshots into one repository with server-side copy on, then off, then on again. A busy index changes between the snapshots,
     * and an idle index does not. A snapshot reuses a file that an earlier snapshot already stored, whether that snapshot copied or
     * uploaded the file. A shard that has not changed since its last snapshot reuses the file list of that snapshot as it is, and copies
     * and uploads nothing.
     */
    public void testCopiesOnThenOffThenOn() throws Exception {
        final Settings settings = copySettings().build();
        startMasterAndIndexNode(settings);
        startSearchNode(settings);
        final String busy = randomIdentifier();
        final String idle = randomIdentifier();
        // no merges, so that the files of a snapshot are still there for the next snapshot to reuse
        final Settings indexSettings = indexSettings(1, 1).put(InternalSettingsPlugin.MERGE_ENABLED.getKey(), false).build();
        createIndex(busy, indexSettings);
        createIndex(idle, indexSettings);
        ensureGreen(busy, idle);
        final long busyDocsAtFirst = indexDocsAndFlush(busy, 1);
        final long idleDocs = indexDocsAndFlush(idle, 1);
        final String repoName = randomIdentifier();
        final Path location = randomRepoPath();
        createRepository(logger, repoName, "fs", Settings.builder().put("location", location), true);

        // Server-side copy is on. Every data file of both shards is copied.
        createSnapshot(repoName, "first", List.of(busy, idle), List.of());
        assertAllDataFilesCopied(repoName, busy, "first");
        assertAllDataFilesCopied(repoName, idle, "first");
        final Set<String> busyCopies = copiedBlobs(repoName, busy, 0);
        final Set<String> idleCopies = copiedBlobs(repoName, idle, 0);
        final Map<String, FileInfo> idleFiles = byName(dataFiles(shardSnapshot(repoName, idle, "first", 0)));

        // Server-side copy is off. The busy shard keeps the copied files of the first snapshot and uploads its new files. The idle shard
        // has not changed, so its file list is the same as in the first snapshot, and nothing is copied or uploaded for it.
        updateClusterSettings(Settings.builder().put(SERVER_SIDE_COPY_ENABLED_SETTING.getKey(), false));
        final long busyDocsAtSecond = busyDocsAtFirst + indexDocsAndFlush(busy, 1);
        createSnapshot(repoName, "second", List.of(busy, idle), List.of());
        final List<FileInfo> busySecondFiles = dataFiles(shardSnapshot(repoName, busy, "second", 0));
        assertTrue("no copied file in " + busySecondFiles, busySecondFiles.stream().anyMatch(FileInfo::isCopied));
        assertTrue("no uploaded file in " + busySecondFiles, busySecondFiles.stream().anyMatch(fileInfo -> fileInfo.isCopied() == false));
        assertThat(copiedBlobs(repoName, busy, 0), equalTo(busyCopies));
        final Set<String> busyUploads = uploadedBlobs(repoName, busy, 0);
        assertThat(busyUploads, not(empty()));
        assertThat(copiedBlobs(repoName, idle, 0), equalTo(idleCopies));
        assertThat(uploadedBlobs(repoName, idle, 0), empty());
        assertSameFiles(idleFiles, byName(dataFiles(shardSnapshot(repoName, idle, "second", 0))));

        // Server-side copy is on again. The busy shard copies its new files, and keeps the files that the repository already holds,
        // whether they were copied or uploaded. The idle shard still has the same file list.
        updateClusterSettings(Settings.builder().put(SERVER_SIDE_COPY_ENABLED_SETTING.getKey(), true));
        final long busyDocsAtThird = busyDocsAtSecond + indexDocsAndFlush(busy, 1);
        // The busy shard reuses the data files of the second snapshot by name, the copied ones and the uploaded ones. Its copy stats line
        // counts both groups.
        final List<FileInfo> busyReusedCopied = busySecondFiles.stream().filter(FileInfo::isCopied).toList();
        final List<FileInfo> busyReusedUploaded = busySecondFiles.stream().filter(fileInfo -> fileInfo.isCopied() == false).toList();
        MockLog.assertThatLogger(
            () -> createSnapshot(repoName, "third", List.of(busy, idle), List.of()),
            BlobStoreRepository.class,
            new MockLog.SeenEventExpectation(
                "files reused by name in the copy stats",
                BlobStoreRepository.class.getCanonicalName(),
                Level.INFO,
                "["
                    + repoName
                    + "][third/*]["
                    + busy
                    + "][*] shard snapshot copy stats: new copies [*], copied bytes [*], total copy time [*ms], slowest copy [*ms],"
                    + " files in new copies [*], reused copies [*], files in reused copies [*], files reused by name, uploaded ["
                    + busyReusedUploaded.size()
                    + "], bytes reused by name, uploaded ["
                    + busyReusedUploaded.stream().mapToLong(FileInfo::length).sum()
                    + "], files reused by name, copied ["
                    + busyReusedCopied.size()
                    + "], bytes reused by name, copied ["
                    + busyReusedCopied.stream().mapToLong(FileInfo::length).sum()
                    + "], uploaded files [*], uploaded bytes [*], dead bytes [*], dead bytes per new copy [*]"
            )
        );
        final Set<String> busyCopiesAtThird = copiedBlobs(repoName, busy, 0);
        assertTrue(busyCopiesAtThird.containsAll(busyCopies));
        assertThat(busyCopiesAtThird.size(), greaterThan(busyCopies.size()));
        assertThat(uploadedBlobs(repoName, busy, 0), equalTo(busyUploads));
        assertThat(copiedBlobs(repoName, idle, 0), equalTo(idleCopies));
        assertThat(uploadedBlobs(repoName, idle, 0), empty());
        assertSameFiles(idleFiles, byName(dataFiles(shardSnapshot(repoName, idle, "third", 0))));

        // every snapshot restores
        restore(repoName, "first", busy, "busy-first");
        assertHitCount(prepareSearch("busy-first"), busyDocsAtFirst);
        restore(repoName, "second", busy, "busy-second");
        assertHitCount(prepareSearch("busy-second"), busyDocsAtSecond);
        restore(repoName, "third", busy, "busy-third");
        assertHitCount(prepareSearch("busy-third"), busyDocsAtThird);
        for (String snapshotName : List.of("first", "second", "third")) {
            restore(repoName, snapshotName, idle, "idle-" + snapshotName);
            assertHitCount(prepareSearch("idle-" + snapshotName), idleDocs);
        }

        // After the first two snapshots are deleted, the third snapshot still restores, so the deletes kept every blob that the third
        // snapshot needs. When the third snapshot is deleted too, no copied or uploaded data blob is left in the repository.
        assertAcked(clusterAdmin().prepareDeleteSnapshot(TEST_REQUEST_TIMEOUT, repoName, "first", "second").get());
        restore(repoName, "third", busy, "busy-third-again");
        assertHitCount(prepareSearch("busy-third-again"), busyDocsAtThird);
        assertAcked(clusterAdmin().prepareDeleteSnapshot(TEST_REQUEST_TIMEOUT, repoName, "third").get());
        assertThat(blobsWithPrefix(location, BlobStoreRepository.COPIED_DATA_BLOB_PREFIX), empty());
        assertThat(blobsWithPrefix(location, BlobStoreRepository.UPLOADED_DATA_BLOB_PREFIX), empty());
    }

    /**
     * An index is deleted while a shard snapshot of the index is copying a commit blob. A copy call cannot be cancelled once it has
     * started, so the test lets the blocked copy finish. The shard snapshot was aborted, so its failure cleanup then removes the copy
     * that the shard snapshot made, and keeps the copies of the earlier snapshot. After the earlier snapshot is deleted, no snapshot in
     * the repository contains the index, and the whole folder of the index is removed.
     */
    public void testIndexDeletedWhileCopying() throws Exception {
        final Settings settings = copySettings().build();
        final String indexNode = startMasterAndIndexNode(settings);
        final String indexName = randomIdentifier();
        createIndex(indexName, 1, 0);
        ensureGreen(indexName);
        indexDocsAndFlush(indexName, between(1, 2));
        final String repoName = randomIdentifier();
        final Path location = randomRepoPath();
        // the snapshot repository uses the mock repository type, so that the copy can be blocked
        createRepository(logger, repoName, StatelessMockRepositoryPlugin.TYPE, Settings.builder().put("location", location), true);
        // The earlier snapshot adds the index to the repository data. That gives the index a folder in the repository, which the delete
        // at the end can remove.
        createSnapshot(repoName, "earlier", List.of(indexName), List.of());
        final Set<String> earlierCopies = copiedBlobs(repoName, indexName, 0);
        assertThat(earlierCopies, not(empty()));
        final IndexId indexId = ESBlobStoreRepositoryIntegTestCase.getRepositoryData(repository(repoName)).resolveIndexId(indexName);

        // The later snapshot needs a new copy, which is blocked.
        indexDocsAndFlush(indexName, 1);
        final var copyStarted = new CountDownLatch(1);
        final var releaseCopy = new CountDownLatch(1);
        final var copyReturned = new CountDownLatch(1);
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
                copyStarted.countDown();
                safeAwait(releaseCopy);
                originalRunnable.run();
                copyReturned.countDown();
            }
        });
        final var snapshotFuture = clusterAdmin().prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, "later")
            .setIndices(indexName)
            .setPartial(true)
            .setWaitForCompletion(true)
            .execute();
        safeAwait(copyStarted);

        // The index is deleted while the copy is blocked. The shard snapshot is aborted, and the master drops the index from the snapshot.
        assertAcked(indicesAdmin().prepareDelete(indexName));
        releaseCopy.countDown();
        safeAwait(copyReturned);
        safeGet(snapshotFuture);

        // The failure cleanup of the aborted shard snapshot removes the copy that the shard snapshot made, and keeps the copies of the
        // earlier snapshot.
        assertBusy(() -> assertThat(blobsWithPrefix(location, BlobStoreRepository.COPIED_DATA_BLOB_PREFIX), equalTo(earlierCopies)));

        // After the earlier snapshot is deleted, no snapshot in the repository contains the index, so the delete removes the whole folder
        // of the index.
        assertAcked(clusterAdmin().prepareDeleteSnapshot(TEST_REQUEST_TIMEOUT, repoName, "earlier").get());
        assertFalse(Files.exists(location.resolve("indices").resolve(indexId.getId())));
    }

    /**
     * A snapshot fails during finalization, after its shard snapshot is done. The shard snapshot has left its blobs in the shard's
     * folder, among them a copy of a commit blob. The repository data does not name the failed snapshot, so no snapshot in the repository
     * refers to that copy.
     * <p>
     * A later snapshot copies the same commit blob again, because the shard generation it starts from does not refer to the failed
     * snapshot's copy. Then the shard generation is lost. The next snapshot rebuilds it from the {@code snap-<uuid>.dat} blobs, among
     * them the blob of the failed snapshot. The rebuilt shard generation holds two copies of the same commit blob, and the next snapshot
     * has to pick one of the two without failing.
     * <p>
     * Every snapshot that is left can be restored, and every copied blob that is left is referred to by one of those snapshots. Java
     * assertions are on in tests, except the integrity assertions that the test turns off. So an assertion that wrongly rejects two
     * copies of the same commit blob would fail the snapshot.
     */
    public void testRebuildOfMissingShardGenerationKeepsCopies() throws Exception {
        final Settings settings = copySettings().build();
        final String node = startMasterAndIndexNode(settings);
        startSearchNode(settings);
        final String indexName = randomIdentifier();
        createIndex(indexName, 1, 1);
        ensureGreen(indexName);
        long docCount = indexDocsAndFlush(indexName, 1);
        final String repoName = randomIdentifier();
        final Path location = randomRepoPath();
        // the snapshot repository uses the mock repository type, so that the finalization of a snapshot can fail
        createRepository(logger, repoName, StatelessMockRepositoryPlugin.TYPE, Settings.builder().put("location", location), true);
        // The base snapshot adds the index to the repository data, so later snapshots of the index use the same folder in the repository.
        createSnapshot(repoName, "base", List.of(indexName), List.of());

        // The snapshot fails after its shard snapshot is done: every write of a meta-<uuid>.dat blob fails, and finalization starts with
        // those writes.
        docCount += indexDocsAndFlush(indexName, 1);
        final var failFinalization = new AtomicBoolean(true);
        snapshotRepositoryOnNode(repoName, node).setStrategy(new StatelessMockRepositoryStrategy() {
            @Override
            public void blobContainerWriteMetadataBlob(
                CheckedRunnable<IOException> original,
                OperationPurpose purpose,
                String blobName,
                boolean failIfAlreadyExists,
                boolean atomic,
                CheckedConsumer<OutputStream, IOException> writer
            ) throws IOException {
                if (failFinalization.get() && blobName.startsWith("meta-")) {
                    throw new IOException("simulated failure of the finalization of a snapshot");
                }
                original.run();
            }
        });
        expectThrows(
            Exception.class,
            () -> clusterAdmin().prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, "failed")
                .setIndices(indexName)
                .setWaitForCompletion(true)
                .get()
        );
        failFinalization.set(false);
        // the failed snapshot left the copy of the new commit blob behind, next to the copy of the base snapshot
        assertThat(blobsWithPrefix(location, BlobStoreRepository.COPIED_DATA_BLOB_PREFIX), hasSize(2));

        // This snapshot starts from the shard generation that the repository data names. That generation does not refer to the failed
        // snapshot's copy, so the snapshot copies the commit blob again.
        createSnapshot(repoName, "second", List.of(indexName), List.of());
        final long docsAtSecond = docCount;
        assertThat(blobsWithPrefix(location, BlobStoreRepository.COPIED_DATA_BLOB_PREFIX), hasSize(3));

        // lose the shard generation that the repository data names
        final BlobStoreRepository repository = repository(repoName);
        final RepositoryData repositoryData = ESBlobStoreRepositoryIntegTestCase.getRepositoryData(repository);
        final IndexId indexId = repositoryData.resolveIndexId(indexName);
        final String shardGeneration = repositoryData.shardGenerations().getShardGen(indexId, 0).getGenerationUUID();
        Files.delete(
            location.resolve("indices")
                .resolve(indexId.getId())
                .resolve("0")
                .resolve(BlobStoreRepository.SNAPSHOT_INDEX_PREFIX + shardGeneration)
        );

        // The next snapshot finds that the shard generation blob is missing. The repository treats that as damage, and in tests an
        // assertion fails the snapshot when it happens, so the test turns that assertion off. The repository then logs an error and
        // rebuilds the generation from the snap-<uuid>.dat blobs, among them the blob of the failed snapshot. This snapshot then needs a
        // new copy.
        docCount += indexDocsAndFlush(indexName, 1);
        try (
            var ignored = new BlobStoreIndexShardSnapshotsIntegritySuppressor();
            var mockLog = MockLog.capture(BlobStoreRepository.class)
        ) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "fallback message",
                    BlobStoreRepository.class.getCanonicalName(),
                    Level.ERROR,
                    "index ["
                        + indexName
                        + "/*] shard generation [*] in [*"
                        + repoName
                        + "][*] not found "
                        + "- falling back to reading all shard snapshots"
                )
            );
            // If an assertion fails inside the shard snapshot, the snapshot never finishes, and the wait below fails the test.
            final SnapshotInfo thirdSnapshot = safeGet(
                clusterAdmin().prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, "third")
                    .setIndices(indexName)
                    .setWaitForCompletion(true)
                    .execute()
            ).getSnapshotInfo();
            assertThat(thirdSnapshot.state(), equalTo(SnapshotState.SUCCESS));
            mockLog.assertAllExpectationsMatched();
        }
        final long docsAtThird = docCount;

        // Delete an older snapshot. The snapshots that are left restore, and no copy is left that none of them refers to.
        assertAcked(clusterAdmin().prepareDeleteSnapshot(TEST_REQUEST_TIMEOUT, repoName, "base").get());
        restore(repoName, "second", indexName, "restored-second");
        assertHitCount(prepareSearch("restored-second"), docsAtSecond);
        restore(repoName, "third", indexName, "restored-third");
        assertHitCount(prepareSearch("restored-third"), docsAtThird);
        final Set<String> referencedCopies = new HashSet<>();
        for (String snapshotName : List.of("second", "third")) {
            dataFiles(shardSnapshot(repoName, indexName, snapshotName, 0)).forEach(fileInfo -> referencedCopies.add(fileInfo.blob()));
        }
        assertThat(blobsWithPrefix(location, BlobStoreRepository.COPIED_DATA_BLOB_PREFIX), equalTo(referencedCopies));
    }

    /**
     * A TLA+ model of the design found this sequence of steps. The sequence loses a copy in a design that names each copy after its
     * commit blob:
     * <ol>
     * <li>The first snapshot copies a commit blob.</li>
     * <li>A delete removes the first snapshot. The delete has listed the shard's folder, written the new shard generation and the new
     * repository data, and is no longer in the cluster state. It still has to remove the blobs that it found, among them the copy.</li>
     * <li>The second snapshot starts from the new shard generation, which holds no copy of the commit blob, so it needs the commit blob
     * again. With a name derived from the commit blob, the second snapshot copies to the same name as the first copy.</li>
     * <li>The delete removes the blobs that it found, among them the second snapshot's copy.</li>
     * </ol>
     * Copy names are random, so the second snapshot's copy has a different name, and the delete does not remove it. The test takes a base
     * snapshot first. It keeps the index in the repository data after the first snapshot is deleted, so the second snapshot writes into
     * the same shard folder that the delete is cleaning.
     */
    public void testDeleteThatIsStillRemovingBlobsKeepsTheCopyOfALaterSnapshot() throws Exception {
        // a blocked delete holds a snapshot thread, so leave enough of them for the second snapshot
        final Settings settings = copySettings().put("thread_pool.snapshot.max", 10).build();
        final String node = startMasterAndIndexNode(settings);
        startSearchNode(settings);
        final String indexName = randomIdentifier();
        createIndex(indexName, 1, 1);
        ensureGreen(indexName);
        long docCount = indexDocsAndFlush(indexName, 1);
        final String repoName = randomIdentifier();
        final Path location = randomRepoPath();
        // the snapshot repository uses the mock repository type, so that the delete can be blocked
        createRepository(logger, repoName, StatelessMockRepositoryPlugin.TYPE, Settings.builder().put("location", location), true);
        createSnapshot(repoName, "base", List.of(indexName), List.of());
        final Set<String> baseCopies = blobsWithPrefix(location, BlobStoreRepository.COPIED_DATA_BLOB_PREFIX);

        // 1. The first snapshot copies the commit blob of its new files.
        docCount += indexDocsAndFlush(indexName, 1);
        createSnapshot(repoName, "first", List.of(indexName), List.of());
        final Set<String> firstCopies = difference(blobsWithPrefix(location, BlobStoreRepository.COPIED_DATA_BLOB_PREFIX), baseCopies);
        assertThat(firstCopies, not(empty()));

        // 2. The delete of the first snapshot is blocked just before it removes blobs. By then it has written the new repository data.
        // The master removes the delete from the cluster state without waiting for the blobs, so the test waits for that. A new snapshot
        // starts only when no delete is in the cluster state.
        final var deleteBlocked = new CountDownLatch(1);
        final var releaseDelete = new CountDownLatch(1);
        snapshotRepositoryOnNode(repoName, node).setStrategy(new StatelessMockRepositoryStrategy() {
            @Override
            public void blobStoreDeleteBlobsIgnoringIfNotExists(
                CheckedRunnable<IOException> originalRunnable,
                OperationPurpose purpose,
                Iterator<String> blobNames
            ) throws IOException {
                // block the removal of the shard's blobs, which include the copies
                if (blobNames.hasNext() && blobNamesHaveCopy(blobNames)) {
                    deleteBlocked.countDown();
                    safeAwait(releaseDelete);
                }
                originalRunnable.run();
            }
        });
        final var deleteFuture = clusterAdmin().prepareDeleteSnapshot(TEST_REQUEST_TIMEOUT, repoName, "first").execute();
        safeAwait(deleteBlocked);
        awaitClusterState(state -> SnapshotDeletionsInProgress.get(state).hasDeletionsInProgress() == false);

        // 3. The second snapshot needs the same commit blob again, and copies it to a new name.
        createSnapshot(repoName, "second", List.of(indexName), List.of());
        final Set<String> secondCopies = difference(
            blobsWithPrefix(location, BlobStoreRepository.COPIED_DATA_BLOB_PREFIX),
            union(baseCopies, firstCopies)
        );
        assertThat(secondCopies, not(empty()));

        // 4. The delete removes the blobs that it found, which are the copies of the first snapshot, and none of the second snapshot's.
        releaseDelete.countDown();
        assertAcked(safeGet(deleteFuture));
        assertThat(blobsWithPrefix(location, BlobStoreRepository.COPIED_DATA_BLOB_PREFIX), equalTo(union(baseCopies, secondCopies)));
        restore(repoName, "second", indexName, "restored-second");
        assertHitCount(prepareSearch("restored-second"), docCount);
    }

    /**
     * A TLA+ model of the design found this sequence of steps for a bad design, in which the failure cleanup of a snapshot deletes the
     * blob that each new file info points at:
     * <ol>
     * <li>The first snapshot needs file f1 and copies the commit blob that holds it. That commit blob also holds file f2.</li>
     * <li>The second snapshot needs f1, f2 and a file f3 that lives in another commit blob. It reuses the file info of f1. The first
     * snapshot does not list f2, so f2 is new to the second snapshot. A copy of the commit blob of f2 already exists, so the new file
     * info of f2 points at that copy. The second snapshot copies the commit blob of f3 under a new name.</li>
     * <li>The second snapshot fails. The bad cleanup deletes the blob of every new file info, so it deletes the copy that the first
     * snapshot refers to.</li>
     * </ol>
     * The real cleanup deletes only the copies that the failed snapshot made, so the copy of the first snapshot stays. To make f2 new to
     * the second snapshot, the test rewrites the first shard snapshot so that it does not list f2.
     */
    public void testFailedSnapshotCleanupKeepsTheCopyOfAnEarlierSnapshot() throws Exception {
        final Settings settings = copySettings()
            // one commit in each commit blob, so that each flush makes a commit blob of its own
            .put(StatelessCommitService.STATELESS_UPLOAD_MAX_AMOUNT_COMMITS.getKey(), 1)
            .build();
        final String node = startMasterAndIndexNode(settings);
        startSearchNode(settings);
        final String indexName = randomIdentifier();
        // no merges, so that the files of the first commit are still there for the second snapshot
        createIndex(indexName, indexSettings(1, 1).put(InternalSettingsPlugin.MERGE_ENABLED.getKey(), false).build());
        ensureGreen(indexName);
        long docCount = indexDocsAndFlush(indexName, 1);
        final String repoName = randomIdentifier();
        final Path location = randomRepoPath();
        // the snapshot repository uses the mock repository type, so that a copy can fail
        createRepository(logger, repoName, StatelessMockRepositoryPlugin.TYPE, Settings.builder().put("location", location), true);

        // 1. The first snapshot copies the commit blob of the first commit, which holds at least two data files.
        createSnapshot(repoName, "first", List.of(indexName), List.of());
        final Set<String> firstCopies = copiedBlobs(repoName, indexName, 0);
        assertThat(firstCopies, hasSize(1));
        final String firstCopy = firstCopies.iterator().next();
        final long firstCopyLength = Files.size(
            location.resolve("indices").resolve(indexId(repoName, indexName)).resolve("0").resolve(firstCopy)
        );
        final List<FileInfo> firstFiles = dataFiles(shardSnapshot(repoName, indexName, "first", 0));
        assertThat(firstFiles.size(), greaterThanOrEqualTo(2));
        // A real commit blob can hold the files of several commits, so a snapshot of an earlier commit can copy files that it does not
        // list. Here each commit blob holds one commit, so the test builds that state by rewriting the first shard snapshot to list only
        // one of the data files of its commit.
        final String hiddenFile = firstFiles.get(0).physicalName();
        final BlobStoreRepository repository = repository(repoName);
        final RepositoryData repositoryData = ESBlobStoreRepositoryIntegTestCase.getRepositoryData(repository);
        final IndexId indexId = repositoryData.resolveIndexId(indexName);
        BlobStoreTestUtil.rewriteShardSnapshotFiles(
            repository,
            indexId,
            0,
            repositoryData.shardGenerations().getShardGen(indexId, 0),
            snapshotId(repositoryData, "first"),
            fileInfo -> fileInfo.physicalName().equals(hiddenFile) ? null : fileInfo
        );

        // 2. The second snapshot has a new commit with a new commit blob. The copy of that commit blob fails, so the shard snapshot
        // fails.
        docCount += indexDocsAndFlush(indexName, 1);
        snapshotRepositoryOnNode(repoName, node).setStrategy(new StatelessMockRepositoryStrategy() {
            @Override
            public void blobContainerCopyBlob(
                CheckedRunnable<IOException> originalRunnable,
                OperationPurpose purpose,
                BlobContainer sourceBlobContainer,
                String sourceBlobName,
                String blobName,
                long blobSize
            ) throws IOException {
                throw new IOException("simulated copy failure");
            }
        });
        final CreateSnapshotResponse failed = clusterAdmin().prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, "second")
            .setIndices(indexName)
            .setPartial(true)
            .setWaitForCompletion(true)
            .get();
        assertThat(failed.getSnapshotInfo().failedShards(), equalTo(1));
        snapshotRepositoryOnNode(repoName, node).setStrategy(StatelessMockRepositoryStrategy.DEFAULT);

        // 3. The cleanup of the failed shard snapshot did not delete the copy that the first snapshot refers to, and left it as it was.
        assertThat(blobsWithPrefix(location, BlobStoreRepository.COPIED_DATA_BLOB_PREFIX), equalTo(firstCopies));
        assertThat(
            Files.size(location.resolve("indices").resolve(indexId.getId()).resolve("0").resolve(firstCopy)),
            equalTo(firstCopyLength)
        );

        // A later snapshot works, and reuses that copy for the file that the first snapshot did not list.
        createSnapshot(repoName, "third", List.of(indexName), List.of());
        final List<FileInfo> thirdFiles = dataFiles(shardSnapshot(repoName, indexName, "third", 0));
        assertTrue(
            "the hidden file does not use the copy of the first snapshot: " + thirdFiles,
            thirdFiles.stream().anyMatch(fileInfo -> fileInfo.physicalName().equals(hiddenFile) && firstCopy.equals(fileInfo.blob()))
        );
        restore(repoName, "third", indexName, "restored-third");
        assertHitCount(prepareSearch("restored-third"), docCount);
    }

    /**
     * A node that runs a shard snapshot gets a shutdown marker (type SIGTERM) while the snapshot copies a commit blob. The marker makes
     * the shard snapshot pause, so that the master can give it to another node. A copy call cannot be interrupted, so the pause can start
     * only after the call returns. The test holds the copy call before it makes the copy, marks the node, waits until the node has seen
     * the marker, and only then lets the call run. Each run of a shard snapshot on a node is an attempt. The first attempt pauses,
     * deletes the copy it made, and logs the copies it made and the time they took. The master then gives the shard snapshot to the
     * second index node, and the attempt there makes its own copies. At the end the snapshot can be restored, and no copy is left that
     * the snapshot does not refer to.
     */
    public void testShutdownMarkerPausesTheShardSnapshotAfterTheCopy() throws Exception {
        // copySettings() picks this setting at random. Only "enabled" lets a shard snapshot run on a node that does not host the primary.
        final Settings settings = copySettings().put(STATELESS_SNAPSHOT_ENABLED_SETTING.getKey(), "enabled").build();
        startMasterOnlyNode(settings);
        final String pausedNode = startIndexNode(settings);
        startSearchNode(settings);
        final String indexName = randomIdentifier();
        createIndex(indexName, 1, 1);
        ensureGreen(indexName);
        final long docCount = indexDocsAndFlush(indexName, between(1, 2));
        // A second index node. The shard snapshot moves to it when pausedNode pauses.
        startIndexNode(settings);
        ensureStableCluster(4);
        final String repoName = randomIdentifier();
        // the snapshot repository uses the mock repository type, so that the copy can be blocked
        createRepository(logger, repoName, StatelessMockRepositoryPlugin.TYPE, Settings.builder().put("location", randomRepoPath()), true);
        final var copyStarted = new CountDownLatch(1);
        final var releaseCopy = new CountDownLatch(1);
        snapshotRepositoryOnNode(repoName, pausedNode).setStrategy(blockCopies(copyStarted, releaseCopy));

        MockLog.assertThatLogger(() -> {
            final var snapshotFuture = clusterAdmin().prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, "snap")
                .setIndices(indexName)
                .setPartial(true)
                .setWaitForCompletion(true)
                .execute();
            safeAwait(copyStarted);
            // Wait until the paused node itself has seen that it must pause. The copy call is still held, so the pause cannot start until
            // the call runs and returns.
            NodeShutdownTestUtils.putShutdownMetadata(
                pausedNode,
                internalCluster().getCurrentMasterNodeInstance(ClusterService.class),
                EnumSet.of(SingleNodeShutdownMetadata.Type.SIGTERM)
            );
            awaitClusterState(pausedNode, state -> SnapshotsInProgress.get(state).isNodeIdForRemoval(getNodeId(pausedNode)));
            // The copy call runs and returns, the attempt pauses, and the second index node runs the shard snapshot.
            releaseCopy.countDown();
            assertThat(safeGet(snapshotFuture).getSnapshotInfo().state(), equalTo(SnapshotState.SUCCESS));
        },
            BlobStoreRepository.class,
            new MockLog.SeenEventExpectation(
                "copy stats of the paused attempt",
                BlobStoreRepository.class.getCanonicalName(),
                Level.INFO,
                "["
                    + repoName
                    + "][snap/*]["
                    + indexName
                    + "][*] shard snapshot copy stats: attempt [paused], copies made [*], copied bytes [*], total copy time [*ms]"
            )
        );

        // Only the copies that the snapshot refers to are left, so the paused attempt has deleted its copy.
        assertAllDataFilesCopied(repoName, indexName, "snap");
        restore(repoName, "snap", indexName, "restored");
        assertHitCount(prepareSearch("restored"), docCount);
    }

    /**
     * A node makes a copy for a shard snapshot and then crashes. No file info refers to the copy, and the node never deletes it. The next
     * delete of a snapshot that has this shard lists the shard folder and removes every copy that no remaining snapshot refers to, so it
     * removes this copy too. The test cannot crash a node at exactly this point, so it fakes the crash. It lets the copy finish, then
     * makes the copy call fail and every blob delete on that node fail. The shard snapshot fails and its cleanup cannot remove the copy.
     * A crash leaves the same unreferenced copy behind.
     */
    public void testCopyOfACrashedNodeIsRemovedByTheNextDelete() throws Exception {
        final Settings settings = copySettings().build();
        startMasterOnlyNode(settings);
        final String indexNode = startIndexNode(settings);
        startSearchNode(settings);
        final String indexName = randomIdentifier();
        createIndex(indexName, 1, 1);
        ensureGreen(indexName);
        final long docCountOfKept = indexDocsAndFlush(indexName, 1);
        final String repoName = randomIdentifier();
        final Path location = randomRepoPath();
        // the snapshot repository uses the mock repository type, so that the node can be made to fail
        createRepository(logger, repoName, StatelessMockRepositoryPlugin.TYPE, Settings.builder().put("location", location), true);
        createSnapshot(repoName, "kept", List.of(indexName), List.of());
        final Set<String> keptCopies = blobsWithPrefix(location, BlobStoreRepository.COPIED_DATA_BLOB_PREFIX);
        assertThat(keptCopies, not(empty()));
        indexDocsAndFlush(indexName, 1);
        createSnapshot(repoName, "deleted", List.of(indexName), List.of());
        final Set<String> deletedCopies = difference(blobsWithPrefix(location, BlobStoreRepository.COPIED_DATA_BLOB_PREFIX), keptCopies);
        assertThat(deletedCopies, not(empty()));

        // The third snapshot makes a copy, and then its node "crashes". The copy call fails, and so does every blob delete on that node,
        // including the cleanup of the failed shard snapshot.
        indexDocsAndFlush(indexName, 1);
        final var copyDone = new CountDownLatch(1);
        final var crash = new CountDownLatch(1);
        final var crashed = new AtomicBoolean();
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
                originalRunnable.run();
                copyDone.countDown();
                safeAwait(crash);
                throw new IOException("the node crashed");
            }

            @Override
            public void blobStoreDeleteBlobsIgnoringIfNotExists(
                CheckedRunnable<IOException> originalRunnable,
                OperationPurpose purpose,
                Iterator<String> blobNames
            ) throws IOException {
                if (crashed.get()) {
                    throw new IOException("the node crashed");
                }
                originalRunnable.run();
            }
        });
        final var crashedSnapshotFuture = clusterAdmin().prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, "crashed")
            .setIndices(indexName)
            .setPartial(true)
            .setWaitForCompletion(true)
            .execute();
        safeAwait(copyDone);
        crashed.set(true);
        crash.countDown();
        assertThat(safeGet(crashedSnapshotFuture).getSnapshotInfo().state(), equalTo(SnapshotState.PARTIAL));
        // An orphan is a copy that no file info refers to. The crashed snapshot left one.
        final Set<String> orphans = difference(
            blobsWithPrefix(location, BlobStoreRepository.COPIED_DATA_BLOB_PREFIX),
            union(keptCopies, deletedCopies)
        );
        assertThat(orphans, not(empty()));

        // The delete of the second snapshot removes its own copies and the orphan, and it keeps the copies of the first snapshot.
        assertAcked(safeGet(clusterAdmin().prepareDeleteSnapshot(TEST_REQUEST_TIMEOUT, repoName, "deleted").execute()));
        assertThat(blobsWithPrefix(location, BlobStoreRepository.COPIED_DATA_BLOB_PREFIX), equalTo(keptCopies));
        restore(repoName, "kept", indexName, "restored");
        assertHitCount(prepareSearch("restored"), docCountOfKept);
    }

    /**
     * The master changes while a delete is about to write the new repository data. Until that write, the delete has only added blobs,
     * such as new shard generations, and the cluster state still lists the delete as in progress. The new master runs the delete again
     * and finishes it. Then a new snapshot runs under the new master and makes copies of its own. After that the test lets the blocked
     * write of the old master continue. That late write must not remove the copies of the new snapshot. The test checks that these copies
     * are still there and that each remaining snapshot can be restored.
     */
    public void testMasterFailoverDuringDelete() throws Exception {
        final Settings settings = copySettings().build();
        startMasterOnlyNode(settings);
        startMasterOnlyNode(settings);
        startIndexNode(settings);
        startSearchNode(settings);
        ensureStableCluster(4);
        final String indexName = randomIdentifier();
        createIndex(indexName, 1, 1);
        ensureGreen(indexName);
        final long docCountOfBase = indexDocsAndFlush(indexName, 1);
        final String repoName = randomIdentifier();
        final Path location = randomRepoPath();
        // the snapshot repository uses the mock repository type, so that the delete can be blocked
        createRepository(logger, repoName, StatelessMockRepositoryPlugin.TYPE, Settings.builder().put("location", location), true);
        createSnapshot(repoName, "base", List.of(indexName), List.of());
        final Set<String> baseCopies = blobsWithPrefix(location, BlobStoreRepository.COPIED_DATA_BLOB_PREFIX);
        assertThat(baseCopies, not(empty()));
        final long docCountOfDeleted = docCountOfBase + indexDocsAndFlush(indexName, 1);
        createSnapshot(repoName, "deleted", List.of(indexName), List.of());
        assertThat(difference(blobsWithPrefix(location, BlobStoreRepository.COPIED_DATA_BLOB_PREFIX), baseCopies), not(empty()));

        // The delete of the second snapshot is blocked on the master, right before it writes the new repository data.
        final String oldMaster = internalCluster().getMasterName();
        final var writeBlocked = new CountDownLatch(1);
        final var releaseWrite = new CountDownLatch(1);
        snapshotRepositoryOnNode(repoName, oldMaster).setStrategy(new StatelessMockRepositoryStrategy() {
            @Override
            public void blobContainerWriteMetadataBlob(
                CheckedRunnable<IOException> original,
                OperationPurpose purpose,
                String blobName,
                boolean failIfAlreadyExists,
                boolean atomic,
                CheckedConsumer<OutputStream, IOException> writer
            ) throws IOException {
                // The repository data is the root blob index-N, where N is a number. A shard generation is index-<uuid>, so the pattern
                // skips it.
                if (blobName.matches("index-\\d+")) {
                    writeBlocked.countDown();
                    safeAwait(releaseWrite);
                }
                original.run();
            }
        });
        // The test does not use the result of this request. The master changes while it runs, so it can succeed or fail.
        clusterAdmin().prepareDeleteSnapshot(TEST_REQUEST_TIMEOUT, repoName, "deleted").execute();
        safeAwait(writeBlocked);

        // Mark the old master for shutdown. In stateless, a node with a shutdown marker may not win an election, so the old master hands
        // over to the other master, which then runs the delete again.
        final var masterClusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        safeAwait(
            (ActionListener<Void> listener) -> NodeShutdownTestUtils.putShutdownMetadata(
                masterClusterService,
                SingleNodeShutdownMetadata.builder()
                    .setType(SingleNodeShutdownMetadata.Type.SIGTERM)
                    .setStartedAtMillis(masterClusterService.threadPool().absoluteTimeInMillis())
                    .setReason("test")
                    .setGracePeriod(TimeValue.timeValueSeconds(60)),
                oldMaster,
                listener
            )
        );
        awaitMasterNode();
        assertThat(internalCluster().getMasterName(), not(equalTo(oldMaster)));
        awaitClusterState(state -> SnapshotDeletionsInProgress.get(state).hasDeletionsInProgress() == false);
        assertBusy(() -> assertThat(blobsWithPrefix(location, BlobStoreRepository.COPIED_DATA_BLOB_PREFIX), equalTo(baseCopies)));

        // A snapshot that runs under the new master makes copies.
        final long docCountOfAfter = docCountOfDeleted + indexDocsAndFlush(indexName, 1);
        createSnapshot(repoName, "after", List.of(indexName), List.of());
        final Set<String> afterCopies = difference(blobsWithPrefix(location, BlobStoreRepository.COPIED_DATA_BLOB_PREFIX), baseCopies);
        assertThat(afterCopies, not(empty()));

        // Let the blocked write of the old master continue. The cluster state names a newer generation of the repository data, which the
        // new master wrote, and the old master can no longer change the cluster state. So the delete of the old master fails before it
        // removes any blob.
        releaseWrite.countDown();
        assertSnapshotThreadPoolIdle(oldMaster);
        assertThat(blobsWithPrefix(location, BlobStoreRepository.COPIED_DATA_BLOB_PREFIX), equalTo(union(baseCopies, afterCopies)));
        restore(repoName, "base", indexName, "restored-base");
        assertHitCount(prepareSearch("restored-base"), docCountOfBase);
        restore(repoName, "after", indexName, "restored-after");
        assertHitCount(prepareSearch("restored-after"), docCountOfAfter);
    }

    /**
     * Returns a strategy that holds each copy of a commit blob. When the copy call starts, it counts down {@code copyStarted}. It then
     * waits for {@code releaseCopy} before it makes the copy.
     */
    private static StatelessMockRepositoryStrategy blockCopies(CountDownLatch copyStarted, CountDownLatch releaseCopy) {
        return new StatelessMockRepositoryStrategy() {
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
        };
    }

    /**
     * Waits until the node's snapshot thread pool has no task running and none queued. The tests use it to wait for snapshot work, such
     * as the blob deletes after a snapshot delete.
     */
    private static void assertSnapshotThreadPoolIdle(String nodeName) throws Exception {
        assertBusy(() -> {
            for (ThreadPoolStats.Stats stats : internalCluster().getInstance(ThreadPool.class, nodeName).stats()) {
                if (stats.name().equals(ThreadPool.Names.SNAPSHOT)) {
                    assertThat(stats.active(), equalTo(0));
                    assertThat(stats.queue(), equalTo(0));
                }
            }
        });
    }

    /**
     * Whether the blob names that a delete removes include a copied blob. A delete names each blob of a shard by its path from the
     * repository root, such as {@code indices/<index id>/<shard>/<blob>}. The failure cleanup of a shard snapshot uses plain blob names
     * with no folder, so the check for {@code /c__} does not match it. The method reads names up to the first copied blob. That is safe
     * in the hook, because the mock repository gives the hook its own copy of the names.
     */
    private static boolean blobNamesHaveCopy(Iterator<String> blobNames) {
        while (blobNames.hasNext()) {
            if (blobNames.next().contains("/" + BlobStoreRepository.COPIED_DATA_BLOB_PREFIX)) {
                return true;
            }
        }
        return false;
    }

    private static Set<String> difference(Set<String> all, Set<String> removed) {
        return all.stream().filter(name -> removed.contains(name) == false).collect(Collectors.toSet());
    }

    private static Set<String> union(Set<String> first, Set<String> second) {
        return Stream.concat(first.stream(), second.stream()).collect(Collectors.toSet());
    }

    private static String indexId(String repoName, String indexName) {
        return ESBlobStoreRepositoryIntegTestCase.getRepositoryData(repository(repoName)).resolveIndexId(indexName).getId();
    }

    private static void assertSameFiles(Map<String, FileInfo> expected, Map<String, FileInfo> actual) {
        assertThat(actual.keySet(), equalTo(expected.keySet()));
        for (Map.Entry<String, FileInfo> entry : expected.entrySet()) {
            assertTrue(
                entry.getValue() + " is not the same as " + actual.get(entry.getKey()),
                entry.getValue().isSame(actual.get(entry.getKey()))
            );
        }
    }

    private Set<String> uploadedBlobs(String repoName, String indexName, int shard) throws IOException {
        final BlobStoreRepository repository = repository(repoName);
        final IndexId indexId = ESBlobStoreRepositoryIntegTestCase.getRepositoryData(repository).resolveIndexId(indexName);
        return repository.shardContainer(indexId, shard)
            .listBlobs(OperationPurpose.SNAPSHOT_METADATA)
            .keySet()
            .stream()
            .filter(name -> name.startsWith(BlobStoreRepository.UPLOADED_DATA_BLOB_PREFIX))
            .collect(Collectors.toSet());
    }

    /**
     * The names of the blobs that start with the given prefix, anywhere in the directory of a repository that stores its blobs as files,
     * which is an {@code fs} repository or the mock repository. This reads the files directly, so it does not depend on the repository
     * code that the test checks.
     */
    private static Set<String> blobsWithPrefix(Path location, String prefix) throws IOException {
        try (Stream<Path> paths = Files.walk(location)) {
            return paths.filter(Files::isRegularFile)
                .map(path -> path.getFileName().toString())
                .filter(name -> name.startsWith(prefix))
                .collect(Collectors.toSet());
        }
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
