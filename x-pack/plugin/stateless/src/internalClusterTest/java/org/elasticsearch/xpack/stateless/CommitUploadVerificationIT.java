/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.support.FilterBlobContainer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesMetrics;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.SnapshotMetrics;
import org.elasticsearch.snapshots.mockstore.BlobStoreWrapper;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommit;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.CRC32C;
import java.util.zip.Checksum;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Covers the guarantee that a batched compound commit is not treated as durable until the bytes that landed in the object store have
 * been read back and matched against the bytes the shard produced.
 * <p>
 * The corruption injected here is the one seen in production: one block of the blob is delivered twice and the block after it is
 * dropped, so the blob keeps its exact length and every length or byte-count check on the way down still passes. Nothing reads a commit
 * blob again until a shard recovers from it, and the translog covering the same operations is released as soon as the upload reports
 * success, so without the read-back those operations are left with no durable copy anywhere.
 */
public class CommitUploadVerificationIT extends AbstractStatelessPluginIntegTestCase {

    private static final String INDEX_NAME = "verified-commits";
    private static final int DOC_COUNT = 100;

    /**
     * Number of batched compound commit writes left to corrupt. Armed by a test immediately before the flush it means to damage, so that
     * unrelated writes — other indices, earlier generations — are left alone.
     */
    private static final AtomicInteger corruptionsRemaining = new AtomicInteger(0);

    /**
     * Restricts corruption to one index, identified by its UUID appearing in the blob container path.
     */
    private static final AtomicReference<String> corruptionIndexUuid = new AtomicReference<>();

    /**
     * Name of the blob the corruption was applied to, so a test can assert on that blob rather than on whichever one happened to be
     * written first.
     */
    private static final AtomicReference<String> corruptedBlobName = new AtomicReference<>();

    private static final Map<String, BlobWrites> blobWrites = new ConcurrentHashMap<>();

    /**
     * What happened to one commit blob across every attempt to write it: how many attempts there were, the checksum of the bytes the
     * shard produced on the last attempt, and the checksum of the bytes that actually landed.
     */
    private record BlobWrites(int attempts, long sourceChecksum, long storedChecksum) {

        BlobWrites next(long sourceChecksum, long storedChecksum) {
            return new BlobWrites(attempts + 1, sourceChecksum, storedChecksum);
        }
    }

    @Override
    protected boolean addMockFsRepository() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), CorruptingRepositoryPlugin.class);
    }

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK)
            // The corruption is injected into the single-stream upload path, the one that carries a whole blob through one InputStream.
            // The multipart path splits the same bytes across ranges and is corrupted the same way in production, but reproducing that
            // needs a repository supporting concurrent multipart uploads.
            .put(ObjectStoreService.OBJECT_STORE_CONCURRENT_MULTIPART_UPLOADS.getKey(), false);
    }

    @Before
    public void resetCorruptionState() {
        corruptionsRemaining.set(0);
        corruptionIndexUuid.set(null);
        corruptedBlobName.set(null);
        blobWrites.clear();
    }

    public void testUploadIsRetriedUntilTheStoredCommitMatches() throws Exception {
        startMasterOnlyNode();
        final String indexNode = startIndexNode();
        ensureStableCluster(2);
        setVerifyCommitUploads(true);

        armCorruption(createIndexAndIndexDocs(), 1);
        flush(INDEX_NAME);

        final String blobName = awaitCorruptionApplied();
        // The verification failure fails the upload, which the retryable upload task runs again, overwriting the damaged blob.
        assertBusy(() -> {
            final BlobWrites writes = blobWrites.get(blobName);
            assertThat("the corrupted upload should have been retried", writes.attempts(), greaterThanOrEqualTo(2));
            assertThat(
                "the commit finally stored should match the one the shard produced",
                writes.storedChecksum(),
                equalTo(writes.sourceChecksum())
            );
        });

        // The point of holding the translog until the commit verifies: the shard can still be rebuilt from the object store.
        internalCluster().restartNode(indexNode);
        ensureGreen(INDEX_NAME);
        final long recoveredDocs = indicesAdmin().prepareStats(INDEX_NAME).setDocs(true).get().getPrimaries().getDocs().getCount();
        assertThat("every document should survive the recovery", recoveredDocs, equalTo((long) DOC_COUNT));
    }

    public void testCorruptUploadIsAcceptedWithoutVerification() throws Exception {
        startMasterOnlyNode();
        startIndexNode();
        ensureStableCluster(2);
        setVerifyCommitUploads(false);

        armCorruption(createIndexAndIndexDocs(), 1);
        flush(INDEX_NAME);

        final BlobWrites writes = blobWrites.get(awaitCorruptionApplied());
        assertThat("the corrupt upload should have been accepted on the first attempt", writes.attempts(), equalTo(1));
        assertThat(
            "the stored commit should differ from the one the shard produced, with nothing having noticed",
            writes.storedChecksum(),
            not(equalTo(writes.sourceChecksum()))
        );
    }

    private String createIndexAndIndexDocs() {
        createIndex(INDEX_NAME, indexSettings(1, 0).build());
        ensureGreen(INDEX_NAME);
        for (int i = 0; i < DOC_COUNT; i++) {
            client().index(new IndexRequest(INDEX_NAME).id("doc-" + i).source("field", "value-" + i)).actionGet();
        }
        return resolveIndex(INDEX_NAME).getUUID();
    }

    private void setVerifyCommitUploads(boolean enabled) {
        updateClusterSettings(Settings.builder().put(ObjectStoreService.OBJECT_STORE_VERIFY_COMMIT_UPLOADS.getKey(), enabled));
    }

    private static void armCorruption(String indexUuid, int writes) {
        corruptionIndexUuid.set(indexUuid);
        corruptionsRemaining.set(writes);
    }

    /**
     * Waits for the armed corruption to have been applied to a commit blob and returns that blob's name.
     */
    private String awaitCorruptionApplied() throws Exception {
        assertBusy(() -> assertThat("corruption was never applied to a commit blob", corruptedBlobName.get(), notNullValue()));
        return corruptedBlobName.get();
    }

    public static class CorruptingRepositoryPlugin extends MockRepository.Plugin {

        public static final String TYPE = ObjectStoreService.ObjectStoreType.MOCK.toString().toLowerCase(Locale.ROOT);

        @Override
        public Map<String, Repository.Factory> getRepositories(
            Environment env,
            NamedXContentRegistry namedXContentRegistry,
            ClusterService clusterService,
            BigArrays bigArrays,
            RecoverySettings recoverySettings,
            RepositoriesMetrics repositoriesMetrics,
            SnapshotMetrics snapshotMetrics
        ) {
            return Collections.singletonMap(
                TYPE,
                (projectId, metadata) -> new CorruptingMockRepository(
                    projectId,
                    metadata,
                    env,
                    namedXContentRegistry,
                    clusterService,
                    bigArrays,
                    recoverySettings,
                    snapshotMetrics
                )
            );
        }
    }

    public static class CorruptingMockRepository extends MockRepository {

        public CorruptingMockRepository(
            ProjectId projectId,
            RepositoryMetadata metadata,
            Environment environment,
            NamedXContentRegistry namedXContentRegistry,
            ClusterService clusterService,
            BigArrays bigArrays,
            RecoverySettings recoverySettings,
            SnapshotMetrics snapshotMetrics
        ) {
            super(projectId, metadata, environment, namedXContentRegistry, clusterService, bigArrays, recoverySettings, snapshotMetrics);
        }

        @Override
        protected BlobStore createBlobStore() throws Exception {
            return new CorruptingBlobStore(super.createBlobStore());
        }

        private static class CorruptingBlobStore extends BlobStoreWrapper {

            CorruptingBlobStore(BlobStore delegate) {
                super(delegate);
            }

            @Override
            public BlobContainer blobContainer(BlobPath path) {
                return new CorruptingBlobContainer(super.blobContainer(path));
            }
        }

        private static class CorruptingBlobContainer extends FilterBlobContainer {

            CorruptingBlobContainer(BlobContainer delegate) {
                super(delegate);
            }

            @Override
            protected BlobContainer wrapChild(BlobContainer child) {
                return new CorruptingBlobContainer(child);
            }

            @Override
            public void writeBlobAtomic(
                OperationPurpose purpose,
                String blobName,
                InputStream inputStream,
                long blobSize,
                boolean failIfAlreadyExists
            ) throws IOException {
                if (isCommitBlobOfIndexUnderTest(blobName) == false) {
                    super.writeBlobAtomic(purpose, blobName, inputStream, blobSize, failIfAlreadyExists);
                    return;
                }

                final byte[] source = inputStream.readAllBytes();
                final byte[] stored;
                if (corruptionsRemaining.getAndUpdate(remaining -> Math.max(0, remaining - 1)) > 0) {
                    stored = duplicateFirstBlock(source);
                    corruptedBlobName.compareAndSet(null, blobName);
                } else {
                    stored = source;
                }

                blobWrites.compute(
                    blobName,
                    (name, previous) -> previous == null
                        ? new BlobWrites(1, checksum(source), checksum(stored))
                        : previous.next(checksum(source), checksum(stored))
                );

                super.writeBlobAtomic(purpose, blobName, new ByteArrayInputStream(stored), blobSize, failIfAlreadyExists);
            }

            private boolean isCommitBlobOfIndexUnderTest(String blobName) {
                final String indexUuid = corruptionIndexUuid.get();
                return indexUuid != null
                    && StatelessCompoundCommit.startsWithBlobPrefix(blobName)
                    && path().buildAsString().contains(indexUuid);
            }
        }
    }

    /**
     * Rewrites the second block of {@code source} with a copy of the first, which is what a delivery that repeats one buffer and drops
     * the next leaves behind: the same number of bytes, one block duplicated, one block gone. The first block carries the compound
     * commit header, so copying it over the second always changes the content.
     */
    private static byte[] duplicateFirstBlock(byte[] source) {
        final int blockSize = Math.min(512, source.length / 2);
        if (blockSize == 0) {
            throw new IllegalStateException("commit blob of [" + source.length + "] bytes is too small to corrupt");
        }
        final byte[] corrupted = Arrays.copyOf(source, source.length);
        System.arraycopy(source, 0, corrupted, blockSize, blockSize);
        if (Arrays.equals(source, corrupted)) {
            throw new IllegalStateException("corrupting the commit blob did not change it");
        }
        return corrupted;
    }

    private static long checksum(byte[] bytes) {
        final Checksum checksum = new CRC32C();
        checksum.update(bytes, 0, bytes.length);
        return checksum.getValue();
    }
}
