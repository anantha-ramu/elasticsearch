/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.DeleteResult;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.OptionalBytesReference;
import org.elasticsearch.common.blobstore.support.BlobMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.CheckedRunnable;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.BooleanSupplier;

/**
 * This is a strategy class for {@link StatelessMockRepository}.
 * <p>
 *
 * This class can be extended to add custom behavior to a repository for testing purposes. New methods should be added to this interface,
 * and called in a corresponding {@link StatelessMockRepository} method, as needed for new testing.
 * <p>
 *
 * Most of the methods receive the corresponding BlobStore/BlobContainer method as a lambda parameter (Supplier/Consumer) so that strategy
 * implementations can control before and after actions, as well as whether to call the real logic at all or instead return an artificially
 * constructed result.
 */
public class StatelessMockRepositoryStrategy {

    public static final StatelessMockRepositoryStrategy DEFAULT = new StatelessMockRepositoryStrategy();

    /**
     * Called in {@link BlobContainer#deleteBlobsIgnoringIfNotExists(OperationPurpose, Iterator)}.
     */
    public void blobStoreDeleteBlobsIgnoringIfNotExists(
        CheckedRunnable<IOException> originalRunnable,
        OperationPurpose purpose,
        Iterator<String> blobNames
    ) throws IOException {
        originalRunnable.run();
    }

    /**
     * Called in {@link BlobContainer#children(OperationPurpose)}.
     */
    public Map<String, BlobContainer> blobContainerChildren(
        CheckedSupplier<Map<String, BlobContainer>, IOException> originalSupplier,
        OperationPurpose purpose
    ) throws IOException {
        return originalSupplier.get();
    }

    /**
     * Called in {@link BlobContainer#delete(OperationPurpose)}.
     */
    public DeleteResult blobContainerDelete(CheckedSupplier<DeleteResult, IOException> originalSupplier, OperationPurpose purpose)
        throws IOException {
        return originalSupplier.get();
    }

    /**
     * Called in {@link BlobContainer#readBlob(OperationPurpose, String)}.
     */
    public InputStream blobContainerReadBlob(
        CheckedSupplier<InputStream, IOException> originalSupplier,
        OperationPurpose purpose,
        String blobName
    ) throws IOException {
        return originalSupplier.get();
    }

    /**
     * Called in {@link BlobContainer#readBlob(OperationPurpose, String, long, long)}.
     */
    public InputStream blobContainerReadBlob(
        CheckedSupplier<InputStream, IOException> originalSupplier,
        OperationPurpose purpose,
        String blobName,
        long position,
        long length
    ) throws IOException {
        return originalSupplier.get();
    }

    /**
     * Called in {@link BlobContainer#writeBlob(OperationPurpose, String, InputStream, long, boolean)}.
     */
    public void blobContainerWriteBlob(
        CheckedRunnable<IOException> originalRunnable,
        OperationPurpose purpose,
        String blobName,
        InputStream inputStream,
        long blobSize,
        boolean failIfAlreadyExists
    ) throws IOException {
        originalRunnable.run();
    }

    /**
     * Called in {@link BlobContainer#writeBlob(OperationPurpose, String, BytesReference, boolean)}.
     */
    public void blobContainerWriteBlob(
        CheckedRunnable<IOException> originalRunnable,
        OperationPurpose purpose,
        String blobName,
        BytesReference bytes,
        boolean failIfAlreadyExists
    ) throws IOException {
        originalRunnable.run();
    }

    /**
     * Called in {@link BlobContainer#supportsConcurrentMultipartUploads()} .
     */
    public boolean supportsConcurrentMultipartUploads(BooleanSupplier originalRunnable) {
        return originalRunnable.getAsBoolean();
    }

    /**
     * Called in {@link BlobContainer#writeBlobAtomic(OperationPurpose, String, long,
     * BlobContainer.BlobMultiPartInputStreamProvider, boolean, Executor)}.
     */
    public void blobContainerWriteBlobAtomic(
        CheckedRunnable<IOException> originalRunnable,
        OperationPurpose purpose,
        String blobName,
        long blobSize,
        BlobContainer.BlobMultiPartInputStreamProvider provider,
        boolean failIfAlreadyExists,
        Executor executor
    ) throws IOException {
        originalRunnable.run();
    }

    /**
     * Called in {@link BlobContainer#writeBlobAtomic(OperationPurpose, String, InputStream, long, boolean)}.
     */
    public void blobContainerWriteBlobAtomic(
        CheckedRunnable<IOException> originalRunnable,
        OperationPurpose purpose,
        String blobName,
        InputStream inputStream,
        long blobSize,
        boolean failIfAlreadyExists
    ) throws IOException {
        originalRunnable.run();
    }

    /**
     * Called in {@link BlobContainer#copyBlob(OperationPurpose, BlobContainer, String, String, long, java.util.concurrent.Executor)}.
     */
    public void blobContainerCopyBlob(
        CheckedRunnable<IOException> originalRunnable,
        OperationPurpose purpose,
        BlobContainer sourceBlobContainer,
        String sourceBlobName,
        String blobName,
        long neededEnd
    ) throws IOException {
        originalRunnable.run();
    }

    /**
     * Whether a copy knows the length of its source blob. The default is {@code true}, and a copy then checks that the source reaches to
     * the needed end and returns the length, as the file system does. A copy can be unable to know the length. An S3 CopyObject request
     * is one, and so is an Azure copy inside one storage account: they copy whatever the source holds, without a check of its length,
     * and return no length. With {@code false}, the copy of the mock repository behaves like that.
     */
    public boolean blobContainerCopyBlobKnowsLength() {
        return true;
    }

    /**
     * Called in {@link BlobContainer#writeMetadataBlob(OperationPurpose, String, boolean, boolean, CheckedConsumer)}.
     */
    public void blobContainerWriteMetadataBlob(
        CheckedRunnable<IOException> original,
        OperationPurpose purpose,
        String blobName,
        boolean failIfAlreadyExists,
        boolean atomic,
        CheckedConsumer<OutputStream, IOException> writer
    ) throws IOException {
        original.run();
    }

    /**
     * Called in {@link BlobContainer#listBlobs(OperationPurpose)}.
     */
    public Map<String, BlobMetadata> blobContainerListBlobs(
        CheckedSupplier<Map<String, BlobMetadata>, IOException> originalSupplier,
        OperationPurpose purpose
    ) throws IOException {
        return originalSupplier.get();
    }

    /**
     * Called in {@link BlobContainer#listBlobsByPrefix(OperationPurpose, String)}.
     */
    public Map<String, BlobMetadata> blobContainerListBlobsByPrefix(
        CheckedSupplier<Map<String, BlobMetadata>, IOException> originalSupplier,
        OperationPurpose purpose,
        String blobNamePrefix
    ) throws IOException {
        return originalSupplier.get();
    }

    public void blobContainerCompareAndSetRegister(
        Runnable originalRunnable,
        OperationPurpose purpose,
        String key,
        BytesReference expected,
        BytesReference updated,
        ActionListener<Boolean> listener
    ) {
        originalRunnable.run();
    }

    public void blobContainerGetRegister(
        Runnable originalRunnable,
        OperationPurpose purpose,
        String key,
        ActionListener<OptionalBytesReference> listener
    ) {
        originalRunnable.run();
    }
}
