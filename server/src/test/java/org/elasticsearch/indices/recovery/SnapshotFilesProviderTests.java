/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.indices.recovery;

import org.apache.lucene.util.Version;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.repositories.blobstore.TestUtils;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests how a node that recovers from a snapshot reads the files that the snapshot lists. A file that was uploaded is in blobs of its
 * own, and a file that was copied is a byte range of a copied blob that other files share.
 */
public class SnapshotFilesProviderTests extends ESTestCase {

    private static final String LUCENE_VERSION = Version.LATEST.toString();

    private final IndexId indexId = new IndexId(randomAlphaOfLength(10), UUIDs.randomBase64UUID());
    private final ShardId shardId = new ShardId(indexId.getName(), indexId.getId(), 0);
    private BlobStoreRepository repository;
    private SnapshotFilesProvider snapshotFilesProvider;

    @Before
    public void createRepositoryAndProvider() {
        final Repository repo = TestUtils.createRepository(ProjectId.DEFAULT, createTempDir(), xContentRegistry());
        repository = (BlobStoreRepository) repo;
        // The provider only looks the repository up by name. A mock stands in for the repositories service because a real one needs a
        // cluster, which the test does not need.
        final RepositoriesService repositoriesService = mock(RepositoriesService.class);
        when(repositoriesService.repository(repository.getMetadata().name())).thenReturn(repository);
        snapshotFilesProvider = new SnapshotFilesProvider(repositoriesService);
    }

    @After
    public void closeRepository() {
        repository.stop();
        repository.close();
    }

    /**
     * A copied blob holds several files back to back with other bytes between them. Each file is read from its offset, for its own
     * length, whether it starts at the beginning of the blob, in the middle, or runs to the very end.
     */
    public void testReadsACopiedFileAsARangeOfItsBlob() throws IOException {
        final String copiedBlob = BlobStoreRepository.UPLOADED_DATA_BLOB_PREFIX + UUIDs.randomBase64UUID();
        final int files = between(2, 5);
        final List<byte[]> contents = new ArrayList<>();
        final List<FileInfo> fileInfos = new ArrayList<>();
        try (BytesStreamOutput blob = new BytesStreamOutput()) {
            for (int i = 0; i < files; i++) {
                // the first file starts at offset 0 about half of the time, the others come after bytes that belong to no file
                if (i > 0 || randomBoolean()) {
                    blob.writeBytes(randomByteArrayOfLength(between(1, 200)));
                }
                final byte[] content = randomByteArrayOfLength(between(1, 1000));
                contents.add(content);
                fileInfos.add(new FileInfo(copiedBlob, metadata("_" + i + ".cfs", content.length), blob.position()));
                blob.writeBytes(content);
            }
            // bytes after the last file, or none, so that the last file can end where the blob ends
            if (randomBoolean()) {
                blob.writeBytes(randomByteArrayOfLength(between(1, 200)));
            }
            shardContainer().writeBlob(OperationPurpose.SNAPSHOT_DATA, copiedBlob, blob.bytes(), true);
        }

        for (int i = 0; i < files; i++) {
            assertThat(readFile(fileInfos.get(i)), equalTo(contents.get(i)));
        }
    }

    /**
     * A file that was uploaded is read from its blob, or from its parts when it is large, as before.
     */
    public void testReadsAnUploadedFileFromItsParts() throws IOException {
        final int partSize = between(1, 50);
        final byte[] content = randomByteArrayOfLength(between(partSize * 2 + 1, partSize * 5));
        final String name = BlobStoreRepository.UPLOADED_DATA_BLOB_PREFIX + UUIDs.randomBase64UUID();
        final FileInfo uploaded = new FileInfo(name, metadata("_0.cfs", content.length), ByteSizeValue.ofBytes(partSize));
        assertThat(uploaded.numberOfParts(), equalTo((content.length + partSize - 1) / partSize));
        for (int part = 0; part < uploaded.numberOfParts(); part++) {
            final int from = part * partSize;
            final int to = Math.min(content.length, from + partSize);
            shardContainer().writeBlob(
                OperationPurpose.SNAPSHOT_DATA,
                uploaded.partName(part),
                new BytesArray(content, from, to - from),
                true
            );
        }
        assertThat(readFile(uploaded), equalTo(content));
    }

    private byte[] readFile(FileInfo fileInfo) throws IOException {
        final AtomicLong throttledNanos = new AtomicLong();
        try (
            InputStream in = snapshotFilesProvider.getInputStreamForSnapshotFile(
                repository.getMetadata().name(),
                indexId,
                shardId,
                fileInfo,
                throttledNanos::addAndGet
            )
        ) {
            return in.readAllBytes();
        }
    }

    private BlobContainer shardContainer() {
        return repository.shardContainer(indexId, shardId.id());
    }

    private static StoreFileMetadata metadata(String physicalName, int length) {
        return new StoreFileMetadata(physicalName, length, randomAlphaOfLength(8), LUCENE_VERSION);
    }
}
