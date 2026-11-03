/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots;

import org.apache.lucene.tests.util.TestUtil;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.support.BlobMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshots;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.repositories.blobstore.ESBlobStoreRepositoryIntegTestCase;
import org.elasticsearch.snapshots.SnapshotCopiedBlobFeatures;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.xpack.stateless.snapshots.StatelessSnapshotSettings.SERVER_SIDE_COPY_ENABLED_SETTING;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.emptyOrNullString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;

/**
 * Checks that a node can restore, clone and delete snapshots that hold copied files, and that the copy setting and the cluster feature
 * for copied files are registered.
 * <p>
 * The terms are:
 * <ul>
 * <li>A commit blob is one blob in the object store. It holds the files of one or more Lucene commits.</li>
 * <li>A copied blob is a copy of a whole commit blob. The object store makes the copy itself, so the node does not read or upload any
 * file data.</li>
 * <li>A copied file is a file that is stored as a byte range of a copied blob. The shard's snapshot metadata records it with the name of
 * the copied blob and the offset where the file starts in the copied blob.</li>
 * <li>A data file is a file whose bytes are not kept in the shard's snapshot metadata. The bytes are in blobs of their own, or in a copied
 * blob. Small files such as {@code segments_N} and {@code .si} are not data files, because the metadata keeps their whole content. They
 * are never copied, so the test ignores them.</li>
 * </ul>
 * The code that writes copied blobs is not part of this test. The test reads a repository that was written earlier by the code that
 * copies. The repository is kept as the test resource {@code copied-blob-snapshots.zip}. The test fails if a later change to the snapshot
 * metadata format, or to the code that reads these snapshots, can no longer read this repository.
 * <p>
 * The repository holds one index, {@code copied-index}, with one shard, and three snapshots of that index:
 * <ul>
 * <li>{@code uploaded} has 20 documents. It was taken with {@link StatelessSnapshotSettings#SERVER_SIDE_COPY_ENABLED_SETTING} off, so
 * every data file was uploaded as a blob of its own.</li>
 * <li>{@code mixed} has 30 documents. It was taken with the setting on. It refers to the two uploaded blobs of {@code uploaded} instead
 * of storing those files again, and its new data files are in a copied blob.</li>
 * <li>{@code merged} has 35 documents. It was taken with the setting on, after a force merge replaced every segment. It shares no file
 * with the other two snapshots, and all its data files are copied.</li>
 * </ul>
 * To make the resource again, use a build that has the code that copies commit blobs into snapshots, and then:
 * <ol>
 * <li>Start a stateless cluster with {@code stateless.snapshot.enabled} set to {@code enabled}. Create {@code copied-index} with one shard
 * and one replica, and register an {@code fs} repository.</li>
 * <li>Index 20 documents, flush, and take the snapshot {@code uploaded}.</li>
 * <li>Set {@code stateless.snapshot.server_side_copy.enabled} to {@code true}. Index 10 more documents, flush, and take the snapshot
 * {@code mixed}.</li>
 * <li>Index 5 more documents, force merge to one segment, flush, and take the snapshot {@code merged}.</li>
 * <li>Zip the contents of the repository's directory, so that {@code index.latest} and {@code indices/} are at the top of the zip.
 * Replace {@code src/internalClusterTest/resources/org/elasticsearch/xpack/stateless/snapshots/copied-blob-snapshots.zip} with it.</li>
 * </ol>
 */
public class StatelessSnapshotCopiedBlobReadersIT extends AbstractStatelessPluginIntegTestCase {

    private static final String REPOSITORY_ZIP = "copied-blob-snapshots.zip";
    private static final String REPOSITORY = "copied-blob-snapshots";
    private static final String INDEX = "copied-index";
    private static final String UPLOADED = "uploaded";
    private static final String MIXED = "mixed";
    private static final String MERGED = "merged";
    private static final String CLONE = "clone";

    @Override
    protected boolean addMockFsRepository() {
        return false;
    }

    /**
     * Reads the shard's snapshot metadata of each snapshot, checks that every copied file lies inside a copied blob, and restores every
     * snapshot.
     */
    public void testParseAndRestoreSnapshotsThatHoldCopiedFiles() throws Exception {
        startNodes();
        registerRepository();

        // "uploaded" was taken with the copy setting off, so it has no copied file.
        final List<FileInfo> uploadedFiles = dataFiles(UPLOADED);
        assertThat(uploadedFiles, not(empty()));
        assertThat(copied(uploadedFiles), empty());

        // "mixed" has copied files. For its other data files, it refers to the uploaded blobs that "uploaded" also refers to.
        final List<FileInfo> mixedFiles = dataFiles(MIXED);
        assertThat(copied(mixedFiles), not(empty()));
        assertThat(uploaded(mixedFiles), not(empty()));

        // "merged" has only copied files.
        final List<FileInfo> mergedFiles = dataFiles(MERGED);
        assertThat(mergedFiles, not(empty()));
        assertThat(copied(mergedFiles), hasSize(mergedFiles.size()));

        // Every copied file is a byte range that lies inside a copied blob in the shard's folder.
        final Map<String, BlobMetadata> blobs = shardContainer().listBlobs(OperationPurpose.SNAPSHOT_METADATA);
        for (FileInfo fileInfo : Stream.concat(copied(mixedFiles).stream(), copied(mergedFiles).stream()).toList()) {
            assertThat(fileInfo.blob(), startsWith(BlobStoreRepository.COPIED_DATA_BLOB_PREFIX));
            assertThat(fileInfo.source(), not(emptyOrNullString()));
            final BlobMetadata copiedBlob = blobs.get(fileInfo.blob());
            assertThat("copied blob of " + fileInfo, copiedBlob, notNullValue());
            assertThat(fileInfo.offset() + fileInfo.length(), lessThanOrEqualTo(copiedBlob.length()));
        }

        // The shard generation is the index-<uuid> blob in the shard's folder. It lists the files of all three snapshots, and it refers to
        // exactly the copied blobs that are in the folder.
        final BlobStoreRepository repository = repository();
        final RepositoryData repositoryData = ESBlobStoreRepositoryIntegTestCase.getRepositoryData(repository);
        final IndexId indexId = repositoryData.resolveIndexId(INDEX);
        final BlobStoreIndexShardSnapshots shardGeneration = repository.getBlobStoreIndexShardSnapshots(
            indexId,
            0,
            repositoryData.shardGenerations().getShardGen(indexId, 0)
        );
        assertThat(shardGeneration.snapshots(), hasSize(3));
        assertThat(shardGeneration.copiedBlobNames(), equalTo(blobNamesWithPrefix(blobs, BlobStoreRepository.COPIED_DATA_BLOB_PREFIX)));

        restoreAndCountDocuments(UPLOADED, 20);
        restoreAndCountDocuments(MIXED, 30);
        restoreAndCountDocuments(MERGED, 35);
    }

    /**
     * Clones "mixed" as "clone", then deletes all snapshots in three steps. After each step, a copied or uploaded data blob must still be
     * there if a remaining snapshot refers to it, and must be gone if no remaining snapshot does.
     */
    public void testCloneAndDeleteSnapshotsThatHoldCopiedFiles() throws Exception {
        startNodes();
        final Path location = registerRepository();

        final List<FileInfo> mixedFiles = dataFiles(MIXED);
        final Set<String> mixedCopies = copiedBlobs(mixedFiles);
        final Set<String> mergedCopies = copiedBlobs(dataFiles(MERGED));
        assertThat(mixedCopies, not(empty()));
        assertThat(mergedCopies, not(empty()));
        // "mixed" and "merged" have no copied blob in common. So the test can tell the copied blobs that must go from the copied blobs that
        // must stay.
        assertThat(union(mixedCopies, mergedCopies), hasSize(mixedCopies.size() + mergedCopies.size()));
        final Set<String> uploadedBlobs = blobsWithPrefix(location, BlobStoreRepository.UPLOADED_DATA_BLOB_PREFIX);
        assertThat(uploadedBlobs, not(empty()));

        // The clone keeps the file entries of "mixed", so it refers to the same copied and uploaded blobs.
        assertAcked(clusterAdmin().prepareCloneSnapshot(TEST_REQUEST_TIMEOUT, REPOSITORY, MIXED, CLONE).setIndices(INDEX).get());
        final Map<String, FileInfo> mixedFilesByName = mixedFiles.stream().collect(Collectors.toMap(FileInfo::name, fileInfo -> fileInfo));
        final List<FileInfo> cloneFiles = dataFiles(CLONE);
        assertThat(cloneFiles, hasSize(mixedFiles.size()));
        for (FileInfo cloneFile : cloneFiles) {
            final FileInfo original = mixedFilesByName.get(cloneFile.name());
            assertThat("file of " + MIXED + " for " + cloneFile, original, notNullValue());
            assertTrue(cloneFile + " is not the same as " + original, cloneFile.isSame(original));
        }

        // Delete step 1. Only "merged" and "clone" are left. "clone" refers to every blob of "uploaded" and "mixed", so no data blob is
        // removed.
        deleteSnapshots(UPLOADED, MIXED);
        assertThat(blobsWithPrefix(location, BlobStoreRepository.COPIED_DATA_BLOB_PREFIX), equalTo(union(mixedCopies, mergedCopies)));
        assertThat(blobsWithPrefix(location, BlobStoreRepository.UPLOADED_DATA_BLOB_PREFIX), equalTo(uploadedBlobs));
        restoreAndCountDocuments(CLONE, 30);

        // Delete step 2. Only "merged" is left. No snapshot refers to the blobs of "uploaded" and "mixed" now, so they are removed.
        // "merged" keeps its copied blobs.
        deleteSnapshots(CLONE);
        assertThat(blobsWithPrefix(location, BlobStoreRepository.COPIED_DATA_BLOB_PREFIX), equalTo(mergedCopies));
        assertThat(blobsWithPrefix(location, BlobStoreRepository.UPLOADED_DATA_BLOB_PREFIX), empty());
        restoreAndCountDocuments(MERGED, 35);

        // Delete step 3. No snapshot is left, so no data blob is left.
        deleteSnapshots(MERGED);
        assertThat(blobsWithPrefix(location, BlobStoreRepository.COPIED_DATA_BLOB_PREFIX), empty());
        assertThat(blobsWithPrefix(location, BlobStoreRepository.UPLOADED_DATA_BLOB_PREFIX), empty());
    }

    /**
     * Checks that every node accepts {@link StatelessSnapshotSettings#SERVER_SIDE_COPY_ENABLED_SETTING}, and that the cluster has
     * {@link SnapshotCopiedBlobFeatures#COPIED_BLOB_RANGES}. Each node publishes that feature to say that it can read copied files.
     */
    public void testSettingIsRegisteredAndNodesHaveTheReadersFeature() {
        // Starting the nodes with the setting in their configuration shows that the setting is registered, because a node refuses to start
        // with a setting that it does not know.
        final Settings settings = Settings.builder().put(SERVER_SIDE_COPY_ENABLED_SETTING.getKey(), true).build();
        startMasterAndIndexNode(settings);
        startSearchNode(settings);
        // Updating the setting and then removing it shows the same for settings updates, because an update that contains an unknown setting
        // is rejected.
        updateClusterSettings(Settings.builder().put(SERVER_SIDE_COPY_ENABLED_SETTING.getKey(), false));
        updateClusterSettings(Settings.builder().putNull(SERVER_SIDE_COPY_ENABLED_SETTING.getKey()));

        final FeatureService featureService = internalCluster().getCurrentMasterNodeInstance(FeatureService.class);
        assertTrue(featureService.clusterHasFeature(clusterService().state(), SnapshotCopiedBlobFeatures.COPIED_BLOB_RANGES));
        // The feature must not be marked as assumed. Otherwise a build that always assumes features, such as serverless, would count nodes
        // that cannot read copied files as having the feature. The Javadoc of COPIED_BLOB_RANGES gives the full reason.
        assertFalse(SnapshotCopiedBlobFeatures.COPIED_BLOB_RANGES.assumedAfterNextCompatibilityBoundary());
    }

    /**
     * Starts an index node and a search node. The restored indices have a replica, which lives on the search node.
     */
    private void startNodes() {
        startMasterAndIndexNode();
        startSearchNode();
    }

    /**
     * Unzips the repository into a new directory under {@code path.repo}, where an {@code fs} repository must be, and registers that
     * directory as an {@code fs} repository. Each call makes its own copy, because the tests clone and delete snapshots and so change the
     * repository.
     *
     * @return the directory of the repository
     */
    private Path registerRepository() throws IOException {
        final Path location = randomRepoPath();
        try (InputStream zip = StatelessSnapshotCopiedBlobReadersIT.class.getResourceAsStream(REPOSITORY_ZIP)) {
            assertThat("test resource " + REPOSITORY_ZIP, zip, notNullValue());
            TestUtil.unzip(zip, location);
        }
        createRepository(logger, REPOSITORY, "fs", Settings.builder().put("location", location), true);
        return location;
    }

    private void restoreAndCountDocuments(String snapshotName, long expectedDocuments) {
        final String restoredIndex = "restored-" + snapshotName;
        final RestoreSnapshotResponse response = clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, REPOSITORY, snapshotName)
            .setIndices(INDEX)
            .setRenamePattern(INDEX)
            .setRenameReplacement(restoredIndex)
            .setWaitForCompletion(true)
            .get();
        assertThat(response.getRestoreInfo().failedShards(), equalTo(0));
        ensureGreen(restoredIndex);
        assertHitCount(prepareSearch(restoredIndex), expectedDocuments);
    }

    /**
     * Deletes snapshots. The delete responds only after the repository has removed the blobs that no remaining snapshot refers to.
     */
    private void deleteSnapshots(String... snapshotNames) {
        assertAcked(clusterAdmin().prepareDeleteSnapshot(TEST_REQUEST_TIMEOUT, REPOSITORY, snapshotNames).get());
    }

    private BlobStoreRepository repository() {
        return (BlobStoreRepository) internalCluster().getCurrentMasterNodeInstance(RepositoriesService.class).repository(REPOSITORY);
    }

    private BlobContainer shardContainer() {
        final BlobStoreRepository repository = repository();
        return repository.shardContainer(ESBlobStoreRepositoryIntegTestCase.getRepositoryData(repository).resolveIndexId(INDEX), 0);
    }

    private static SnapshotId snapshotId(RepositoryData repositoryData, String snapshotName) {
        return repositoryData.getSnapshotIds().stream().filter(id -> id.getName().equals(snapshotName)).findFirst().orElseThrow();
    }

    /**
     * The data files of a snapshot, as the shard's snapshot metadata records them.
     */
    private List<FileInfo> dataFiles(String snapshotName) {
        final BlobStoreRepository repository = repository();
        final RepositoryData repositoryData = ESBlobStoreRepositoryIntegTestCase.getRepositoryData(repository);
        final BlobStoreIndexShardSnapshot shardSnapshot = repository.loadShardSnapshot(
            repository.shardContainer(repositoryData.resolveIndexId(INDEX), 0),
            snapshotId(repositoryData, snapshotName)
        );
        return shardSnapshot.indexFiles().stream().filter(fileInfo -> fileInfo.metadata().hashEqualsContents() == false).toList();
    }

    private static List<FileInfo> copied(List<FileInfo> fileInfos) {
        return fileInfos.stream().filter(FileInfo::isCopied).toList();
    }

    private static List<FileInfo> uploaded(List<FileInfo> fileInfos) {
        return fileInfos.stream().filter(fileInfo -> fileInfo.isCopied() == false).toList();
    }

    private static Set<String> copiedBlobs(List<FileInfo> fileInfos) {
        return copied(fileInfos).stream().map(FileInfo::blob).collect(Collectors.toSet());
    }

    private static Set<String> union(Set<String> first, Set<String> second) {
        return Stream.concat(first.stream(), second.stream()).collect(Collectors.toSet());
    }

    private static Set<String> blobNamesWithPrefix(Map<String, BlobMetadata> blobs, String prefix) {
        return blobs.keySet().stream().filter(name -> name.startsWith(prefix)).collect(Collectors.toSet());
    }

    /**
     * The names of all blobs under the repository's directory that start with the given prefix. The method lists the directory with plain
     * file operations instead of asking the repository, because the tests check what the repository's delete removes, and the check must
     * not depend on that code.
     */
    private static Set<String> blobsWithPrefix(Path location, String prefix) throws IOException {
        try (Stream<Path> paths = Files.walk(location)) {
            return paths.filter(Files::isRegularFile)
                .map(path -> path.getFileName().toString())
                .filter(name -> name.startsWith(prefix))
                .collect(Collectors.toSet());
        }
    }
}
