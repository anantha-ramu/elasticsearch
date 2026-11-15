/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.s3;

import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.metrics.MetricCollection;
import software.amazon.awssdk.metrics.MetricPublisher;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.SdkPartType;
import software.amazon.awssdk.services.s3.model.StorageClass;
import software.amazon.awssdk.services.s3.model.UploadPartCopyRequest;
import software.amazon.awssdk.services.s3.model.UploadPartCopyResponse;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStoreException;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.mockito.ArgumentCaptor;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.repositories.blobstore.BlobStoreTestUtil.randomPurpose;
import static org.elasticsearch.repositories.s3.S3BlobContainer.ConditionalOperation;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class S3BlobStoreContainerTests extends ESTestCase {

    public void testExecuteSingleUploadBlobSizeTooLarge() {
        final long blobSize = ByteSizeUnit.GB.toBytes(randomIntBetween(6, 10));
        final S3BlobStore blobStore = mock(S3BlobStore.class);
        final S3BlobContainer blobContainer = new S3BlobContainer(mock(BlobPath.class), blobStore);

        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> blobContainer.executeSingleUpload(
                randomPurpose(),
                blobStore,
                randomAlphaOfLengthBetween(1, 10),
                null,
                blobSize,
                randomCondition()
            )
        );
        assertEquals("Upload request size [" + blobSize + "] can't be larger than 5gb", e.getMessage());
    }

    public void testExecuteSingleUploadBlobSizeLargerThanBufferSize() {
        final S3BlobStore blobStore = mock(S3BlobStore.class);
        when(blobStore.bufferSizeInBytes()).thenReturn(ByteSizeUnit.MB.toBytes(1));

        final S3BlobContainer blobContainer = new S3BlobContainer(mock(BlobPath.class), blobStore);
        final String blobName = randomAlphaOfLengthBetween(1, 10);

        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> blobContainer.executeSingleUpload(
                randomPurpose(),
                blobStore,
                blobName,
                new ByteArrayInputStream(new byte[0]),
                ByteSizeUnit.MB.toBytes(2),
                randomCondition()
            )
        );
        assertEquals("Upload request size [2097152] can't be larger than buffer size", e.getMessage());
    }

    public void testExecuteSingleUpload() throws IOException {
        final String bucketName = randomAlphaOfLengthBetween(1, 10);
        final String blobName = randomAlphaOfLengthBetween(1, 10);

        final BlobPath blobPath = BlobPath.EMPTY;
        if (randomBoolean()) {
            IntStream.of(randomIntBetween(1, 5)).forEach(value -> blobPath.add("path_" + value));
        }

        final int bufferSize = randomIntBetween(1024, 2048);
        final int blobSize = randomIntBetween(0, bufferSize);

        final S3BlobStore blobStore = mock(S3BlobStore.class);
        when(blobStore.bucket()).thenReturn(bucketName);
        when(blobStore.bufferSizeInBytes()).thenReturn((long) bufferSize);
        when(blobStore.supportsConditionalWrites()).thenReturn(true);

        final S3BlobContainer blobContainer = new S3BlobContainer(blobPath, blobStore);

        final boolean serverSideEncryption = randomBoolean();
        when(blobStore.serverSideEncryption()).thenReturn(serverSideEncryption);

        final StorageClass storageClass = randomFrom(StorageClass.values());
        when(blobStore.resolveStorageClass(any(OperationPurpose.class))).thenReturn(storageClass);

        final ObjectCannedACL cannedAccessControlList = randomBoolean() ? randomFrom(ObjectCannedACL.values()) : null;
        if (cannedAccessControlList != null) {
            when(blobStore.getCannedACL()).thenReturn(cannedAccessControlList);
        }

        final ConditionalOperation conditionalOperation = randomCondition();

        final S3Client client = configureMockClient(blobStore);

        final ArgumentCaptor<PutObjectRequest> requestCaptor = ArgumentCaptor.forClass(PutObjectRequest.class);
        final ArgumentCaptor<RequestBody> bodyCaptor = ArgumentCaptor.forClass(RequestBody.class);

        when(client.putObject(requestCaptor.capture(), bodyCaptor.capture())).thenReturn(PutObjectResponse.builder().build());

        final ByteArrayInputStream inputStream = new ByteArrayInputStream(new byte[blobSize]);
        blobContainer.executeSingleUpload(randomPurpose(), blobStore, blobName, inputStream, blobSize, conditionalOperation);

        final PutObjectRequest request = requestCaptor.getValue();
        assertEquals(bucketName, request.bucket());
        assertEquals(blobPath.buildAsString() + blobName, request.key());

        assertEquals(Long.valueOf(blobSize), request.contentLength());
        assertEquals(storageClass, request.storageClass());
        assertEquals(cannedAccessControlList, request.acl());
        if (serverSideEncryption) {
            assertEquals(
                PutObjectRequest.builder().serverSideEncryption("AES256").build().sseCustomerAlgorithm(),
                request.sseCustomerAlgorithm()
            );
        }

        switch (conditionalOperation) {
            case ConditionalOperation.IfMatch ifMatch -> {
                assertEquals(ifMatch.etag(), request.ifMatch());
                assertNull(request.ifNoneMatch());
            }
            case ConditionalOperation.IfNoneMatch ignored -> {
                assertNull(request.ifMatch());
                assertEquals("*", request.ifNoneMatch());
            }
            case ConditionalOperation.None ignored -> {
                assertNull(request.ifMatch());
                assertNull(request.ifNoneMatch());
            }
        }

        final RequestBody requestBody = bodyCaptor.getValue();
        try (var contentStream = requestBody.contentStreamProvider().newStream()) {
            assertEquals(inputStream.available(), blobSize);
            // checking that reading from contentStream also reads from inputStream
            final int toSkip = between(0, blobSize);
            contentStream.skipNBytes(toSkip);
            assertEquals(inputStream.available(), blobSize - toSkip);
        }
    }

    public void testExecuteMultipartUploadBlobSizeTooLarge() {
        final long blobSize = ByteSizeUnit.TB.toBytes(randomIntBetween(6, 10));
        final S3BlobStore blobStore = mock(S3BlobStore.class);
        final S3BlobContainer blobContainer = new S3BlobContainer(mock(BlobPath.class), blobStore);

        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> blobContainer.executeMultipartUpload(
                randomPurpose(),
                blobStore,
                randomAlphaOfLengthBetween(1, 10),
                null,
                blobSize,
                randomCondition()
            )
        );
        assertEquals("Multipart upload request size [" + blobSize + "] can't be larger than 5tb", e.getMessage());
    }

    public void testExecuteMultipartUploadBlobSizeTooSmall() {
        final long blobSize = ByteSizeUnit.MB.toBytes(randomIntBetween(1, 4));
        final S3BlobStore blobStore = mock(S3BlobStore.class);
        final S3BlobContainer blobContainer = new S3BlobContainer(mock(BlobPath.class), blobStore);

        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> blobContainer.executeMultipartUpload(
                randomPurpose(),
                blobStore,
                randomAlphaOfLengthBetween(1, 10),
                null,
                blobSize,
                randomCondition()
            )
        );
        assertEquals("Multipart upload request size [" + blobSize + "] can't be smaller than 5mb", e.getMessage());
    }

    public void testExecuteMultipartUpload() throws IOException {
        testExecuteMultipart(false);
    }

    public void testExecuteMultipartCopy() throws IOException {
        testExecuteMultipart(true);
    }

    void testExecuteMultipart(boolean doCopy) throws IOException {
        final String bucketName = randomAlphaOfLengthBetween(1, 10);
        final String blobName = randomAlphaOfLengthBetween(1, 10);
        final String sourceBucketName = randomAlphaOfLengthBetween(1, 10);
        final String sourceBlobName = randomAlphaOfLengthBetween(1, 10);

        final BlobPath blobPath = BlobPath.EMPTY;
        if (randomBoolean()) {
            IntStream.of(randomIntBetween(1, 5)).forEach(value -> blobPath.add("path_" + value));
        }
        final var sourceBlobPath = BlobPath.EMPTY;
        if (randomBoolean()) {
            IntStream.of(randomIntBetween(1, 5)).forEach(value -> sourceBlobPath.add("path_" + value));
        }

        final long blobSize = ByteSizeUnit.GB.toBytes(randomIntBetween(1, 128));
        final long bufferSize = ByteSizeUnit.MB.toBytes(randomIntBetween(5, 1024));
        final long maxCopySizeBeforeMultipart = ByteSizeUnit.MB.toBytes(randomIntBetween(5, 1024));

        final S3BlobStore blobStore = mock(S3BlobStore.class);
        when(blobStore.bucket()).thenReturn(bucketName);
        when(blobStore.bufferSizeInBytes()).thenReturn(bufferSize);
        when(blobStore.maxCopySizeBeforeMultipart()).thenReturn(maxCopySizeBeforeMultipart);
        when(blobStore.supportsConditionalWrites()).thenReturn(true);

        final S3BlobStore sourceBlobStore = mock(S3BlobStore.class);
        when(sourceBlobStore.bucket()).thenReturn(sourceBucketName);

        final boolean serverSideEncryption = randomBoolean();
        when(blobStore.serverSideEncryption()).thenReturn(serverSideEncryption);

        final StorageClass storageClass = randomFrom(StorageClass.values());
        when(blobStore.resolveStorageClass(any(OperationPurpose.class))).thenReturn(storageClass);

        final ObjectCannedACL cannedAccessControlList = randomBoolean() ? randomFrom(ObjectCannedACL.values()) : null;
        if (cannedAccessControlList != null) {
            when(blobStore.getCannedACL()).thenReturn(cannedAccessControlList);
        }

        final ConditionalOperation conditionalOperation = doCopy ? ConditionalOperation.NONE : randomCondition();

        final S3Client client = configureMockClient(blobStore);

        final var uploadId = randomIdentifier();
        final ArgumentCaptor<CreateMultipartUploadRequest> createMultipartUploadRequestCaptor = ArgumentCaptor.forClass(
            CreateMultipartUploadRequest.class
        );
        when(client.createMultipartUpload(createMultipartUploadRequestCaptor.capture())).thenReturn(
            CreateMultipartUploadResponse.builder().uploadId(uploadId).build()
        );

        final ArgumentCaptor<UploadPartRequest> uploadPartRequestCaptor = ArgumentCaptor.forClass(UploadPartRequest.class);
        final ArgumentCaptor<UploadPartCopyRequest> uploadPartCopyRequestCaptor = ArgumentCaptor.forClass(UploadPartCopyRequest.class);
        final ArgumentCaptor<RequestBody> uploadPartBodyCaptor = ArgumentCaptor.forClass(RequestBody.class);

        final List<String> expectedEtags = new ArrayList<>();
        final long partSize = Math.min(doCopy ? maxCopySizeBeforeMultipart : bufferSize, blobSize);
        long totalBytes = 0;
        do {
            expectedEtags.add(randomAlphaOfLength(50));
            totalBytes += partSize;
        } while (totalBytes < blobSize);

        if (doCopy) {
            when(client.uploadPartCopy(uploadPartCopyRequestCaptor.capture())).thenAnswer(invocationOnMock -> {
                final UploadPartCopyRequest request = invocationOnMock.getArgument(0);
                return UploadPartCopyResponse.builder().copyPartResult(b -> b.eTag(expectedEtags.get(request.partNumber() - 1))).build();
            });
        } else {
            when(client.uploadPart(uploadPartRequestCaptor.capture(), uploadPartBodyCaptor.capture())).thenAnswer(invocationOnMock -> {
                final UploadPartRequest request = invocationOnMock.getArgument(0);
                return UploadPartResponse.builder().eTag(expectedEtags.get(request.partNumber() - 1)).build();
            });
        }

        final ArgumentCaptor<CompleteMultipartUploadRequest> completeMultipartUploadRequestCaptor = ArgumentCaptor.forClass(
            CompleteMultipartUploadRequest.class
        );
        // S3 answers the completion of a multipart copy with the ETag of the new blob, and a copy fails without it
        when(client.completeMultipartUpload(completeMultipartUploadRequestCaptor.capture())).thenReturn(
            doCopy
                ? CompleteMultipartUploadResponse.builder().eTag(randomAlphaOfLength(20)).build()
                : CompleteMultipartUploadResponse.builder().build()
        );

        final ByteArrayInputStream inputStream = new ByteArrayInputStream(new byte[0]);
        final S3BlobContainer blobContainer = new S3BlobContainer(blobPath, blobStore);
        final S3BlobContainer sourceContainer = new S3BlobContainer(sourceBlobPath, sourceBlobStore);

        if (doCopy) {
            blobContainer.executeMultipartCopy(randomPurpose(), sourceContainer, sourceBlobName, blobName, blobSize, null);
        } else {
            blobContainer.executeMultipartUpload(randomPurpose(), blobStore, blobName, inputStream, blobSize, conditionalOperation);
        }

        final CreateMultipartUploadRequest initRequest = createMultipartUploadRequestCaptor.getValue();
        assertEquals(bucketName, initRequest.bucket());
        assertEquals(blobPath.buildAsString() + blobName, initRequest.key());
        assertEquals(storageClass, initRequest.storageClass());
        assertEquals(cannedAccessControlList, initRequest.acl());
        if (serverSideEncryption) {
            assertEquals(
                PutObjectRequest.builder().serverSideEncryption("AES256").build().sseCustomerAlgorithm(),
                initRequest.sseCustomerAlgorithm()
            );
        }

        final Tuple<Long, Long> numberOfParts = S3BlobContainer.numberOfMultiparts(blobSize, partSize);

        if (doCopy) {
            final var copyRequests = uploadPartCopyRequestCaptor.getAllValues();
            assertEquals(numberOfParts.v1().intValue(), copyRequests.size());
            for (int i = 0; i < copyRequests.size(); i++) {
                final var uploadPartCopyRequest = copyRequests.get(i);
                final long startOffset = i * partSize;
                final long endOffset = Math.min(startOffset + partSize - 1, blobSize - 1);

                assertEquals(sourceBucketName, uploadPartCopyRequest.sourceBucket());
                assertEquals(sourceBlobPath.buildAsString() + sourceBlobName, uploadPartCopyRequest.sourceKey());
                assertEquals(bucketName, uploadPartCopyRequest.destinationBucket());
                assertEquals(blobPath.buildAsString() + blobName, uploadPartCopyRequest.destinationKey());
                assertEquals(uploadId, uploadPartCopyRequest.uploadId());
                assertEquals(Integer.valueOf(i + 1), uploadPartCopyRequest.partNumber());
                assertEquals("bytes=" + startOffset + "-" + endOffset, uploadPartCopyRequest.copySourceRange());
            }
        } else {
            final List<UploadPartRequest> uploadPartRequests = uploadPartRequestCaptor.getAllValues();
            assertEquals(numberOfParts.v1().intValue(), uploadPartRequests.size());

            final List<RequestBody> uploadPartBodies = uploadPartBodyCaptor.getAllValues();
            assertEquals(numberOfParts.v1().intValue(), uploadPartBodies.size());

            for (int i = 0; i < uploadPartRequests.size(); i++) {
                final UploadPartRequest uploadRequest = uploadPartRequests.get(i);

                assertEquals(bucketName, uploadRequest.bucket());
                assertEquals(blobPath.buildAsString() + blobName, uploadRequest.key());
                assertEquals(uploadId, uploadRequest.uploadId());
                assertEquals(i + 1, uploadRequest.partNumber().intValue());

                assertEquals(
                    uploadRequest.sdkPartType() + " at " + i + " of " + uploadPartRequests.size(),
                    uploadRequest.sdkPartType() == SdkPartType.LAST,
                    i == uploadPartRequests.size() - 1
                );

                assertEquals(
                    "part " + i,
                    uploadRequest.sdkPartType() == SdkPartType.LAST ? Optional.of(numberOfParts.v2()) : Optional.of(bufferSize),
                    uploadPartBodies.get(i).optionalContentLength()
                );
            }
        }

        final CompleteMultipartUploadRequest compRequest = completeMultipartUploadRequestCaptor.getValue();
        assertEquals(bucketName, compRequest.bucket());
        assertEquals(blobPath.buildAsString() + blobName, compRequest.key());
        assertEquals(uploadId, compRequest.uploadId());

        switch (conditionalOperation) {
            case ConditionalOperation.IfMatch ifMatch -> {
                assertEquals(ifMatch.etag(), compRequest.ifMatch());
                assertNull(compRequest.ifNoneMatch());
            }
            case ConditionalOperation.IfNoneMatch ignored -> {
                assertNull(compRequest.ifMatch());
                assertEquals("*", compRequest.ifNoneMatch());
            }
            case ConditionalOperation.None ignored -> {
                assertNull(compRequest.ifMatch());
                assertNull(compRequest.ifNoneMatch());
            }
        }

        final List<String> actualETags = compRequest.multipartUpload()
            .parts()
            .stream()
            .map(CompletedPart::eTag)
            .collect(Collectors.toList());
        assertEquals(expectedEtags, actualETags);

        closeMockClient(blobStore);
    }

    public void testExecuteMultipartUploadAborted() {
        final String bucketName = randomAlphaOfLengthBetween(1, 10);
        final String blobName = randomAlphaOfLengthBetween(1, 10);

        final long blobSize = ByteSizeUnit.MB.toBytes(765);
        final long bufferSize = ByteSizeUnit.MB.toBytes(150);

        final S3BlobStore blobStore = mock(S3BlobStore.class);
        when(blobStore.bucket()).thenReturn(bucketName);
        when(blobStore.bufferSizeInBytes()).thenReturn(bufferSize);
        when(blobStore.resolveStorageClass(any(OperationPurpose.class))).thenReturn(randomFrom(StorageClass.values()));

        final S3Client client = mock(S3Client.class);
        final SdkHttpClient httpClient = mock(SdkHttpClient.class);
        final AmazonS3Reference clientReference = new AmazonS3Reference(client, httpClient);
        when(blobStore.clientReference()).then(invocation -> {
            clientReference.mustIncRef();
            return clientReference;
        });
        when(blobStore.getMetricPublisher(any(), any())).thenReturn(new MetricPublisher() {
            @Override
            public void publish(MetricCollection metricCollection) {}

            @Override
            public void close() {}
        });

        final String uploadId = randomAlphaOfLength(25);

        final int stage = randomInt(2);
        final List<AwsServiceException> exceptions = Arrays.asList(
            S3Exception.builder().message("Expected initialization request to fail").build(),
            S3Exception.builder().message("Expected upload part request to fail").build(),
            S3Exception.builder().message("Expected completion request to fail").build()
        );

        if (stage == 0) {
            // Fail the initialization request
            when(client.createMultipartUpload(any(CreateMultipartUploadRequest.class))).thenThrow(exceptions.get(stage));

        } else if (stage == 1) {
            final CreateMultipartUploadResponse.Builder initResult = CreateMultipartUploadResponse.builder();
            initResult.uploadId(uploadId);
            when(client.createMultipartUpload(any(CreateMultipartUploadRequest.class))).thenReturn(initResult.build());

            // Fail the upload part request
            when(client.uploadPart(any(UploadPartRequest.class), any(RequestBody.class))).thenThrow(exceptions.get(stage));

        } else {
            final CreateMultipartUploadResponse.Builder initResult = CreateMultipartUploadResponse.builder();
            initResult.uploadId(uploadId);
            when(client.createMultipartUpload(any(CreateMultipartUploadRequest.class))).thenReturn(initResult.build());

            when(client.uploadPart(any(UploadPartRequest.class), any(RequestBody.class))).thenAnswer(invocationOnMock -> {
                final UploadPartRequest request = (UploadPartRequest) invocationOnMock.getArguments()[0];
                final UploadPartResponse.Builder response = UploadPartResponse.builder();
                response.eTag(randomAlphaOfLength(20));
                return response.build();
            });

            // Fail the completion request
            when(client.completeMultipartUpload(any(CompleteMultipartUploadRequest.class))).thenThrow(exceptions.get(stage));
        }

        final ArgumentCaptor<AbortMultipartUploadRequest> argumentCaptor = ArgumentCaptor.forClass(AbortMultipartUploadRequest.class);
        when(client.abortMultipartUpload(argumentCaptor.capture())).thenReturn(AbortMultipartUploadResponse.builder().build());

        final IOException e = expectThrows(IOException.class, () -> {
            final S3BlobContainer blobContainer = new S3BlobContainer(BlobPath.EMPTY, blobStore);
            blobContainer.executeMultipartUpload(
                randomPurpose(),
                blobStore,
                blobName,
                new ByteArrayInputStream(new byte[0]),
                blobSize,
                randomCondition()
            );
        });

        assertEquals("Unable to upload object [" + blobName + "] using multipart upload", e.getMessage());
        assertThat(e.getCause(), instanceOf(S3Exception.class));
        assertEquals(exceptions.get(stage).getMessage(), e.getCause().getMessage());

        if (stage == 0) {
            verify(client, times(1)).createMultipartUpload(any(CreateMultipartUploadRequest.class));
            verify(client, times(0)).uploadPart(any(UploadPartRequest.class), any(RequestBody.class));
            verify(client, times(0)).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
            verify(client, times(0)).abortMultipartUpload(any(AbortMultipartUploadRequest.class));

        } else {
            verify(client, times(1)).createMultipartUpload(any(CreateMultipartUploadRequest.class));

            if (stage == 1) {
                verify(client, times(1)).uploadPart(any(UploadPartRequest.class), any(RequestBody.class));
                verify(client, times(0)).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
            } else {
                verify(client, times(6)).uploadPart(any(UploadPartRequest.class), any(RequestBody.class));
                verify(client, times(1)).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
            }

            verify(client, times(1)).abortMultipartUpload(any(AbortMultipartUploadRequest.class));

            final AbortMultipartUploadRequest abortRequest = argumentCaptor.getValue();
            assertEquals(bucketName, abortRequest.bucket());
            assertEquals(blobName, abortRequest.key());
            assertEquals(uploadId, abortRequest.uploadId());
        }

        closeMockClient(blobStore);
    }

    public void testConcurrentWriteBlobAtomicAborted() {
        final String bucketName = randomAlphaOfLengthBetween(1, 10);
        final String blobName = randomAlphaOfLengthBetween(1, 10);
        final long blobSize = ByteSizeUnit.MB.toBytes(765);
        final long bufferSize = ByteSizeUnit.MB.toBytes(150);

        final S3BlobStore blobStore = mock(S3BlobStore.class);
        when(blobStore.bucket()).thenReturn(bucketName);
        when(blobStore.bufferSizeInBytes()).thenReturn(bufferSize);
        when(blobStore.resolveStorageClass(any(OperationPurpose.class))).thenReturn(randomFrom(StorageClass.values()));
        when(blobStore.serverSideEncryption()).thenReturn(false);
        when(blobStore.supportsConditionalWrites()).thenReturn(false);

        final S3Client client = configureMockClient(blobStore);
        final String uploadId = randomAlphaOfLength(25);
        final AwsServiceException s3Exception = S3Exception.builder().message("Expected failure").build();

        // stages: 0=upload part throws S3Exception, 1=complete throws S3Exception, 2=provider throws IOException
        final int stage = randomInt(2);
        final IOException providerException = (stage == 2) ? new IOException("provider failure") : null;

        when(client.createMultipartUpload(any(CreateMultipartUploadRequest.class))).thenReturn(
            CreateMultipartUploadResponse.builder().uploadId(uploadId).build()
        );

        if (stage == 0) {
            when(client.uploadPart(any(UploadPartRequest.class), any(RequestBody.class))).thenThrow(s3Exception);
        } else if (stage == 1) {
            when(client.uploadPart(any(UploadPartRequest.class), any(RequestBody.class))).thenAnswer(
                inv -> UploadPartResponse.builder().eTag(randomAlphaOfLength(20)).build()
            );
            when(client.completeMultipartUpload(any(CompleteMultipartUploadRequest.class))).thenThrow(s3Exception);
        }

        final ArgumentCaptor<AbortMultipartUploadRequest> abortCaptor = ArgumentCaptor.forClass(AbortMultipartUploadRequest.class);
        when(client.abortMultipartUpload(abortCaptor.capture())).thenReturn(AbortMultipartUploadResponse.builder().build());

        final BlobContainer.BlobMultiPartInputStreamProvider provider = (stage == 2)
            ? (offset, length) -> { throw providerException; }
            : (offset, length) -> new ByteArrayInputStream(new byte[0]);

        final IOException e = expectThrows(
            IOException.class,
            () -> new S3BlobContainer(BlobPath.EMPTY, blobStore).writeBlobAtomic(
                randomPurpose(),
                blobName,
                blobSize,
                provider,
                randomBoolean(),
                Runnable::run
            )
        );

        if (stage == 2) {
            assertSame(providerException, e);
        } else {
            assertEquals("Unable to upload object [" + blobName + "] using multipart upload", e.getMessage());
            assertThat(e.getCause(), instanceOf(AwsServiceException.class));
        }

        verify(client, times(1)).createMultipartUpload(any(CreateMultipartUploadRequest.class));
        verify(client, times(1)).abortMultipartUpload(any(AbortMultipartUploadRequest.class));

        final AbortMultipartUploadRequest abortRequest = abortCaptor.getValue();
        assertEquals(bucketName, abortRequest.bucket());
        assertEquals(blobName, abortRequest.key());
        assertEquals(uploadId, abortRequest.uploadId());

        closeMockClient(blobStore);
    }

    public void testWriteMetadataBlobPropagatesErrorWhenAbortFails() {
        new PropagatesErrorWhenAbortFailsTestCase() {
            @Override
            protected void doMultipartUpload() throws IOException {
                container.writeMetadataBlob(
                    OperationPurpose.SNAPSHOT_METADATA,
                    blobName,
                    false,
                    false,
                    out -> out.write(randomByteArrayOfLength(Math.toIntExact(bufferSize)))
                );
            }
        }.run();
    }

    public void testExecuteMultipartUploadPropagatesErrorWhenAbortFails() {
        new PropagatesErrorWhenAbortFailsTestCase() {
            @Override
            protected void doMultipartUpload() throws IOException {
                container.executeMultipartUpload(
                    randomPurpose(),
                    blobStore,
                    blobName,
                    new ByteArrayInputStream(new byte[0]),
                    blobSize,
                    randomCondition()
                );
            }
        }.run();
    }

    public void testWriteBlobAtomicPropagatesErrorWhenAbortFails() {
        new PropagatesErrorWhenAbortFailsTestCase() {
            @Override
            protected void doMultipartUpload() throws IOException {
                container.writeBlobAtomic(
                    randomPurpose(),
                    blobName,
                    blobSize,
                    (offset, length) -> new ByteArrayInputStream(new byte[0]),
                    randomBoolean(),
                    Runnable::run
                );
            }
        }.run();
    }

    /**
     * Verifies that a fatal {@link Error} from the MPU upload path is not masked when best-effort MPU abort fails.
     */
    private abstract class PropagatesErrorWhenAbortFailsTestCase {

        protected final long bufferSize = S3Repository.MIN_PART_SIZE_USING_MULTIPART.getBytes();
        protected final long blobSize = bufferSize + 1;

        protected final String bucketName = randomIdentifier("bucket-");
        protected final String blobName = randomIdentifier("blob-");
        protected final String uploadId = randomIdentifier("upload-");
        protected final Error simulatedError = new Error("simulated error");

        protected final S3BlobStore blobStore = mock(S3BlobStore.class);
        protected final S3BlobContainer container = new S3BlobContainer(BlobPath.EMPTY, blobStore);
        protected S3Client client;

        protected abstract void doMultipartUpload() throws IOException;

        final void run() {
            when(blobStore.bucket()).thenReturn(bucketName);
            when(blobStore.bufferSizeInBytes()).thenReturn(bufferSize);
            when(blobStore.bigArrays()).thenReturn(
                new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService())
            );
            when(blobStore.resolveStorageClass(any(OperationPurpose.class))).thenReturn(randomFrom(StorageClass.values()));
            when(blobStore.serverSideEncryption()).thenReturn(false);
            when(blobStore.supportsConditionalWrites()).thenReturn(false);

            client = configureMockClient(blobStore);
            when(client.createMultipartUpload(any(CreateMultipartUploadRequest.class))).thenReturn(
                CreateMultipartUploadResponse.builder().uploadId(uploadId).build()
            );
            when(client.abortMultipartUpload(any(AbortMultipartUploadRequest.class))).thenThrow(
                S3Exception.builder().message("abort failed").build()
            );
            when(client.uploadPart(any(UploadPartRequest.class), any(RequestBody.class))).thenThrow(simulatedError);

            final Error thrown = expectThrows(Error.class, this::doMultipartUpload);
            assertSame(simulatedError, thrown);
            assertEquals(0, simulatedError.getSuppressed().length);
            verify(client, times(1)).createMultipartUpload(any(CreateMultipartUploadRequest.class));
            verify(client, atLeastOnce()).uploadPart(any(UploadPartRequest.class), any(RequestBody.class));
            verify(client, never()).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
            verify(client, times(1)).abortMultipartUpload(any(AbortMultipartUploadRequest.class));
            closeMockClient(blobStore);
        }
    }

    public void testConcurrentWriteBlobAtomicSingleThread() throws Exception {
        testConcurrentWriteBlobAtomic(true);
    }

    public void testConcurrentWriteBlobAtomicMultipleThreads() throws Exception {
        testConcurrentWriteBlobAtomic(false);
    }

    public void testConcurrentWriteBlobAtomic(boolean singleThread) throws Exception {
        final String bucketName = randomAlphaOfLengthBetween(1, 10);
        final String blobName = randomAlphaOfLengthBetween(1, 10);
        final int nbParts = randomIntBetween(2, 5);
        final long minBufferSize = S3Repository.MIN_PART_SIZE_USING_MULTIPART.getBytes();
        final long bufferSize = randomLongBetween(minBufferSize, minBufferSize * 2);
        // nbParts = ceil(blobSize / bufferSize)
        final long blobSize = randomLongBetween((nbParts - 1) * bufferSize + 1, nbParts * bufferSize);
        assert nbParts == (blobSize + bufferSize - 1) / bufferSize;

        final S3BlobStore blobStore = mock(S3BlobStore.class);
        when(blobStore.bucket()).thenReturn(bucketName);
        when(blobStore.bufferSizeInBytes()).thenReturn(bufferSize);
        when(blobStore.resolveStorageClass(any(OperationPurpose.class))).thenReturn(randomFrom(StorageClass.values()));
        when(blobStore.serverSideEncryption()).thenReturn(false);
        when(blobStore.supportsConditionalWrites()).thenReturn(false);

        final S3Client client = configureMockClient(blobStore);
        when(client.createMultipartUpload(any(CreateMultipartUploadRequest.class))).thenReturn(
            CreateMultipartUploadResponse.builder().uploadId(randomAlphaOfLength(25)).build()
        );

        final int numThreads = singleThread ? 1 : nbParts;
        // Barrier requires all nbParts threads to arrive before proceeding
        final CyclicBarrier barrier = new CyclicBarrier(numThreads);
        when(client.uploadPart(any(UploadPartRequest.class), any(RequestBody.class))).thenAnswer(inv -> {
            safeAwait(barrier);
            return UploadPartResponse.builder().eTag("test-etag").build();
        });
        when(client.completeMultipartUpload(any(CompleteMultipartUploadRequest.class))).thenReturn(
            CompleteMultipartUploadResponse.builder().build()
        );

        final ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
        try {
            executorService.submit(() -> {
                new S3BlobContainer(BlobPath.EMPTY, blobStore).writeBlobAtomic(
                    randomPurpose(),
                    blobName,
                    blobSize,
                    (offset, length) -> new ByteArrayInputStream(new byte[0]),
                    randomBoolean(),
                    executorService
                );
                return null;
            }).get();
        } finally {
            ESTestCase.terminate(executorService);
        }

        verify(client, times(1)).createMultipartUpload(any(CreateMultipartUploadRequest.class));
        verify(client, times(nbParts)).uploadPart(any(UploadPartRequest.class), any(RequestBody.class));
        verify(client, times(1)).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));

        closeMockClient(blobStore);
    }

    public void testCopy() throws Exception {
        final var sourceBucketName = randomAlphaOfLengthBetween(1, 10);
        final var sourceBlobName = randomAlphaOfLengthBetween(1, 10);
        final var blobName = randomAlphaOfLengthBetween(1, 10);

        final StorageClass storageClass = randomFrom(StorageClass.values());
        final ObjectCannedACL cannedAccessControlList = randomBoolean() ? randomFrom(ObjectCannedACL.values()) : null;

        final var blobStore = mock(S3BlobStore.class);
        when(blobStore.bucket()).thenReturn(sourceBucketName);
        when(blobStore.resolveStorageClass(any(OperationPurpose.class))).thenReturn(storageClass);
        if (cannedAccessControlList != null) {
            when(blobStore.getCannedACL()).thenReturn(cannedAccessControlList);
        }
        when(blobStore.maxCopySizeBeforeMultipart()).thenReturn(S3Repository.MIN_PART_SIZE_USING_MULTIPART.getBytes());

        final var sourceBlobPath = BlobPath.EMPTY.add(randomAlphaOfLengthBetween(1, 10));
        final var sourceBlobContainer = new S3BlobContainer(sourceBlobPath, blobStore);

        final var destinationBlobPath = BlobPath.EMPTY.add(randomAlphaOfLengthBetween(1, 10));
        final var destinationBlobContainer = new S3BlobContainer(destinationBlobPath, blobStore);

        final var client = configureMockClient(blobStore);

        final ArgumentCaptor<CopyObjectRequest> captor = ArgumentCaptor.forClass(CopyObjectRequest.class);
        when(client.copyObject(captor.capture())).thenReturn(copyObjectResponse());

        final var length = destinationBlobContainer.copyBlob(
            randomPurpose(),
            sourceBlobContainer,
            sourceBlobName,
            blobName,
            randomLongBetween(1, 10_000),
            null
        );
        // one CopyObject request copies the whole blob, and neither the request nor the answer has a length
        assertThat(length, equalTo(OptionalLong.empty()));

        final CopyObjectRequest request = captor.getValue();
        assertEquals(sourceBucketName, request.sourceBucket());
        assertEquals(sourceBlobPath.buildAsString() + sourceBlobName, request.sourceKey());
        assertEquals(sourceBucketName, request.destinationBucket());
        assertEquals(destinationBlobPath.buildAsString() + blobName, request.destinationKey());
        assertEquals(storageClass, request.storageClass());
        assertEquals(cannedAccessControlList, request.acl());

        closeMockClient(blobStore);
    }

    public void testConcurrentMultipartCopyAborted() {
        final String bucketName = randomAlphaOfLengthBetween(1, 10);
        final String sourceBlobName = randomAlphaOfLengthBetween(1, 10);
        final String blobName = randomAlphaOfLengthBetween(1, 10);
        final long blobSize = ByteSizeUnit.MB.toBytes(765);
        final long copyPartSize = ByteSizeUnit.MB.toBytes(150);

        final S3BlobStore blobStore = mock(S3BlobStore.class);
        when(blobStore.bucket()).thenReturn(bucketName);
        when(blobStore.maxCopySizeBeforeMultipart()).thenReturn(copyPartSize);
        when(blobStore.serverSideEncryption()).thenReturn(false);
        when(blobStore.supportsConditionalWrites()).thenReturn(false);
        when(blobStore.resolveStorageClass(any(OperationPurpose.class))).thenReturn(randomFrom(StorageClass.values()));

        final S3BlobStore sourceBlobStore = mock(S3BlobStore.class);
        when(sourceBlobStore.bucket()).thenReturn(randomAlphaOfLengthBetween(1, 10));

        final S3Client client = configureMockClient(blobStore);
        final String uploadId = randomAlphaOfLength(25);
        final AwsServiceException s3Exception = S3Exception.builder().message("Expected failure").build();

        // stages: 0=uploadPartCopy throws S3Exception, 1=complete throws S3Exception
        final int stage = randomInt(1);

        when(client.createMultipartUpload(any(CreateMultipartUploadRequest.class))).thenReturn(
            CreateMultipartUploadResponse.builder().uploadId(uploadId).build()
        );

        if (stage == 0) {
            when(client.uploadPartCopy(any(UploadPartCopyRequest.class))).thenThrow(s3Exception);
        } else {
            when(client.uploadPartCopy(any(UploadPartCopyRequest.class))).thenAnswer(
                inv -> UploadPartCopyResponse.builder().copyPartResult(b -> b.eTag(randomAlphaOfLength(20))).build()
            );
            when(client.completeMultipartUpload(any(CompleteMultipartUploadRequest.class))).thenThrow(s3Exception);
        }

        final ArgumentCaptor<AbortMultipartUploadRequest> abortCaptor = ArgumentCaptor.forClass(AbortMultipartUploadRequest.class);
        when(client.abortMultipartUpload(abortCaptor.capture())).thenReturn(AbortMultipartUploadResponse.builder().build());

        final S3BlobContainer blobContainer = new S3BlobContainer(BlobPath.EMPTY, blobStore);
        final S3BlobContainer sourceContainer = new S3BlobContainer(BlobPath.EMPTY, sourceBlobStore);

        final IOException e = expectThrows(
            IOException.class,
            () -> blobContainer.executeMultipartCopy(randomPurpose(), sourceContainer, sourceBlobName, blobName, blobSize, Runnable::run)
        );

        assertEquals("Unable to copy object [" + blobName + "] using multipart upload", e.getMessage());
        assertThat(e.getCause(), instanceOf(AwsServiceException.class));

        verify(client, times(1)).createMultipartUpload(any(CreateMultipartUploadRequest.class));
        verify(client, times(1)).abortMultipartUpload(any(AbortMultipartUploadRequest.class));

        final AbortMultipartUploadRequest abortRequest = abortCaptor.getValue();
        assertEquals(bucketName, abortRequest.bucket());
        assertEquals(blobName, abortRequest.key());
        assertEquals(uploadId, abortRequest.uploadId());

        closeMockClient(blobStore);
    }

    public void testConcurrentMultipartCopySingleThread() throws Exception {
        testConcurrentMultipartCopy(true);
    }

    public void testConcurrentMultipartCopyMultipleThreads() throws Exception {
        testConcurrentMultipartCopy(false);
    }

    void testConcurrentMultipartCopy(boolean singleThread) throws Exception {
        final String bucketName = randomAlphaOfLengthBetween(1, 10);
        final String sourceBucketName = randomAlphaOfLengthBetween(1, 10);
        final String sourceBlobName = randomAlphaOfLengthBetween(1, 10);
        final String blobName = randomAlphaOfLengthBetween(1, 10);
        final int nbParts = randomIntBetween(2, 5);
        final long minPartSize = S3Repository.MIN_PART_SIZE_USING_MULTIPART.getBytes();
        final long copyPartSize = randomLongBetween(minPartSize, minPartSize * 2);
        // nbParts = ceil(blobSize / copyPartSize)
        final long blobSize = randomLongBetween((nbParts - 1) * copyPartSize + 1, nbParts * copyPartSize);
        assert nbParts == (blobSize + copyPartSize - 1) / copyPartSize;

        final S3BlobStore blobStore = mock(S3BlobStore.class);
        when(blobStore.bucket()).thenReturn(bucketName);
        when(blobStore.maxCopySizeBeforeMultipart()).thenReturn(copyPartSize);
        when(blobStore.serverSideEncryption()).thenReturn(false);
        when(blobStore.supportsConditionalWrites()).thenReturn(false);
        when(blobStore.resolveStorageClass(any(OperationPurpose.class))).thenReturn(randomFrom(StorageClass.values()));

        final S3BlobStore sourceBlobStore = mock(S3BlobStore.class);
        when(sourceBlobStore.bucket()).thenReturn(sourceBucketName);

        final S3Client client = configureMockClient(blobStore);
        when(client.createMultipartUpload(any(CreateMultipartUploadRequest.class))).thenReturn(
            CreateMultipartUploadResponse.builder().uploadId(randomAlphaOfLength(25)).build()
        );

        final int numThreads = singleThread ? 1 : nbParts;
        final CyclicBarrier barrier = new CyclicBarrier(numThreads);
        when(client.uploadPartCopy(any(UploadPartCopyRequest.class))).thenAnswer(inv -> {
            safeAwait(barrier);
            return UploadPartCopyResponse.builder().copyPartResult(b -> b.eTag("test-etag")).build();
        });
        when(client.completeMultipartUpload(any(CompleteMultipartUploadRequest.class))).thenReturn(
            CompleteMultipartUploadResponse.builder().eTag("test-etag").build()
        );

        final S3BlobContainer blobContainer = new S3BlobContainer(BlobPath.EMPTY, blobStore);
        final S3BlobContainer sourceContainer = new S3BlobContainer(BlobPath.EMPTY, sourceBlobStore);

        final ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
        try {
            executorService.submit(() -> {
                blobContainer.executeMultipartCopy(randomPurpose(), sourceContainer, sourceBlobName, blobName, blobSize, executorService);
                return null;
            }).get();
        } finally {
            ESTestCase.terminate(executorService);
        }

        verify(client, times(1)).createMultipartUpload(any(CreateMultipartUploadRequest.class));
        verify(client, times(nbParts)).uploadPartCopy(any(UploadPartCopyRequest.class));
        verify(client, times(1)).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));

        closeMockClient(blobStore);
    }

    /**
     * S3 answers a copy that worked with the ETag of the new blob.
     */
    private static CopyObjectResponse copyObjectResponse() {
        return CopyObjectResponse.builder().copyObjectResult(result -> result.eTag(randomAlphaOfLength(20))).build();
    }

    /**
     * The stores and clients that the tests of a copy with a HEAD request use: a destination store with the part size {@code copyPartSize},
     * and a source store of its own. Each has a client of its own, so a test can tell which of them a request went through. Closing the
     * stores checks that every client reference that a copy took has been released, also when a test fails.
     */
    private record CopyStores(S3BlobStore destination, S3Client destinationClient, S3BlobStore source, S3Client sourceClient)
        implements
            AutoCloseable {
        @Override
        public void close() {
            closeMockClient(destination);
            closeMockClient(source);
        }
    }

    private static CopyStores copyStores(long copyPartSize, String destinationBucket, String sourceBucket) {
        final S3BlobStore blobStore = mock(S3BlobStore.class);
        when(blobStore.bucket()).thenReturn(destinationBucket);
        when(blobStore.maxCopySizeBeforeMultipart()).thenReturn(copyPartSize);
        when(blobStore.serverSideEncryption()).thenReturn(false);
        when(blobStore.supportsConditionalWrites()).thenReturn(false);
        when(blobStore.resolveStorageClass(any(OperationPurpose.class))).thenReturn(randomFrom(StorageClass.values()));
        final S3BlobStore sourceBlobStore = mock(S3BlobStore.class);
        when(sourceBlobStore.bucket()).thenReturn(sourceBucket);
        return new CopyStores(blobStore, configureMockClient(blobStore), sourceBlobStore, configureMockClient(sourceBlobStore));
    }

    private static void answerMultipartCopy(S3Client client, String eTagOfComplete) {
        when(client.createMultipartUpload(any(CreateMultipartUploadRequest.class))).thenReturn(
            CreateMultipartUploadResponse.builder().uploadId(randomAlphaOfLength(25)).build()
        );
        when(client.uploadPartCopy(any(UploadPartCopyRequest.class))).thenAnswer(
            inv -> UploadPartCopyResponse.builder().copyPartResult(b -> b.eTag(randomAlphaOfLength(20))).build()
        );
        when(client.completeMultipartUpload(any(CompleteMultipartUploadRequest.class))).thenReturn(
            eTagOfComplete == null
                ? CompleteMultipartUploadResponse.builder().build()
                : CompleteMultipartUploadResponse.builder().eTag(eTagOfComplete).build()
        );
    }

    private static HeadObjectResponse headAnswer(long length) {
        return HeadObjectResponse.builder().contentLength(length).build();
    }

    private static S3Exception s3Error(RestStatus status, @Nullable String errorCode) {
        final var builder = S3Exception.builder().statusCode(status.getStatus()).message("simulated " + status);
        if (errorCode != null) {
            builder.awsErrorDetails(AwsErrorDetails.builder().errorCode(errorCode).build());
        }
        // the builder gives the type AwsServiceException, and the object is an S3Exception, as the client throws it
        return (S3Exception) builder.build();
    }

    private IOException copyFails(CopyStores stores, OperationPurpose purpose, long neededEnd) {
        final var destination = new S3BlobContainer(BlobPath.EMPTY, stores.destination());
        final var source = new S3BlobContainer(BlobPath.EMPTY, stores.source());
        return expectThrows(
            IOException.class,
            () -> destination.copyBlob(
                purpose,
                source,
                randomAlphaOfLengthBetween(1, 10),
                randomAlphaOfLengthBetween(1, 10),
                neededEnd,
                null
            )
        );
    }

    /**
     * A copy of a blob that the caller needs more of than the copy limit goes in parts, and each part copies a byte range of the source.
     * The last range ends where the source ends, so the copy needs the exact length of the source. It asks for it with one HEAD request,
     * which goes through the client and the stats of the source store, and it copies the whole source, also if the caller needs only the
     * first bytes of it. The copy returns the length from the HEAD request.
     */
    public void testCopyAboveTheLimitAsksTheSourceStoreForItsLengthOnce() throws IOException {
        final long minPartSize = S3Repository.MIN_PART_SIZE_USING_MULTIPART.getBytes();
        final long copyPartSize = randomLongBetween(minPartSize, minPartSize * 2);
        final int nbParts = randomIntBetween(2, 5);
        final long sourceLength = randomLongBetween((nbParts - 1) * copyPartSize + 1, nbParts * copyPartSize);
        // the caller needs at least a part, so the copy goes in parts
        final long neededEnd = randomLongBetween(copyPartSize + 1, sourceLength);
        final String sourceBucket = randomAlphaOfLengthBetween(1, 10);
        final String sourceBlobName = randomAlphaOfLengthBetween(1, 10);
        final String blobName = randomAlphaOfLengthBetween(1, 10);
        final OperationPurpose purpose = randomPurpose();
        try (var stores = copyStores(copyPartSize, randomAlphaOfLengthBetween(1, 10), sourceBucket)) {
            final var sourcePath = BlobPath.EMPTY.add(randomAlphaOfLengthBetween(1, 10));
            final ArgumentCaptor<HeadObjectRequest> headCaptor = ArgumentCaptor.forClass(HeadObjectRequest.class);
            when(stores.sourceClient().headObject(headCaptor.capture())).thenReturn(headAnswer(sourceLength));
            answerMultipartCopy(stores.destinationClient(), randomAlphaOfLength(20));
            final ArgumentCaptor<UploadPartCopyRequest> partCaptor = ArgumentCaptor.forClass(UploadPartCopyRequest.class);
            when(stores.destinationClient().uploadPartCopy(partCaptor.capture())).thenAnswer(
                inv -> UploadPartCopyResponse.builder().copyPartResult(b -> b.eTag(randomAlphaOfLength(20))).build()
            );

            final var destination = new S3BlobContainer(BlobPath.EMPTY, stores.destination());
            final var source = new S3BlobContainer(sourcePath, stores.source());
            final var length = destination.copyBlob(purpose, source, sourceBlobName, blobName, neededEnd, null);

            assertThat(length, equalTo(OptionalLong.of(sourceLength)));
            // one HEAD request, for the source blob, through the client of the source store and no other
            verify(stores.sourceClient(), times(1)).headObject(any(HeadObjectRequest.class));
            verify(stores.destinationClient(), never()).headObject(any(HeadObjectRequest.class));
            assertThat(headCaptor.getValue().bucket(), equalTo(sourceBucket));
            assertThat(headCaptor.getValue().key(), equalTo(sourcePath.buildAsString() + sourceBlobName));
            // The request is set up for the stats of the source store, as a HEAD request of the purpose of the copy. The destination
            // store does not set up any HEAD request.
            verify(stores.source(), times(1)).getMetricPublisher(S3BlobStore.Operation.HEAD_OBJECT, purpose);
            verify(stores.source(), times(1)).addPurposeQueryParameter(eq(purpose), any());
            verify(stores.destination(), never()).getMetricPublisher(eq(S3BlobStore.Operation.HEAD_OBJECT), any());
            // one multipart copy, in parts of the copy limit, and the last part ends at the last byte of the source
            verify(stores.destinationClient(), never()).copyObject(any(CopyObjectRequest.class));
            verify(stores.destinationClient(), times(1)).createMultipartUpload(any(CreateMultipartUploadRequest.class));
            verify(stores.destinationClient(), times(1)).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
            final var ranges = partCaptor.getAllValues().stream().map(UploadPartCopyRequest::copySourceRange).toList();
            assertThat(ranges, hasSize(nbParts));
            for (int part = 0; part < nbParts - 1; part++) {
                assertThat(ranges.get(part), equalTo("bytes=" + (part * copyPartSize) + "-" + ((part + 1) * copyPartSize - 1)));
            }
            assertThat(ranges.get(nbParts - 1), equalTo("bytes=" + ((nbParts - 1) * copyPartSize) + "-" + (sourceLength - 1)));
        }
    }

    /**
     * A length from the HEAD request that is below the end that the caller needs cannot be used: the parts would copy a prefix of the
     * blob, and S3 would not say that the copy is short. The copy fails before it starts the multipart upload.
     */
    public void testCopyAboveTheLimitFailsIfTheLengthIsBelowTheNeededEnd() {
        final long minPartSize = S3Repository.MIN_PART_SIZE_USING_MULTIPART.getBytes();
        final long copyPartSize = randomLongBetween(minPartSize, minPartSize * 2);
        final long neededEnd = randomLongBetween(copyPartSize + 2, copyPartSize * 3);
        final long sourceLength = randomLongBetween(1, neededEnd - 1);
        try (var stores = copyStores(copyPartSize, randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10))) {
            when(stores.sourceClient().headObject(any(HeadObjectRequest.class))).thenReturn(headAnswer(sourceLength));
            answerMultipartCopy(stores.destinationClient(), randomAlphaOfLength(20));

            final IOException e = copyFails(stores, randomPurpose(), neededEnd);

            assertThat(
                e.getMessage(),
                containsString("the source is [" + sourceLength + "] bytes long, but the caller needs [" + neededEnd + "]")
            );
            verify(stores.sourceClient(), times(1)).headObject(any(HeadObjectRequest.class));
            verify(stores.destinationClient(), never()).createMultipartUpload(any(CreateMultipartUploadRequest.class));
            verify(stores.destinationClient(), never()).uploadPartCopy(any(UploadPartCopyRequest.class));
            verify(stores.destinationClient(), never()).copyObject(any(CopyObjectRequest.class));
        }
    }

    /**
     * A source that is not in the store fails the HEAD request with 404, which is a {@link NoSuchFileException}, before the multipart
     * upload starts.
     */
    public void testCopyAboveTheLimitOfAMissingSourceFailsWithNoSuchFile() {
        final long copyPartSize = S3Repository.MIN_PART_SIZE_USING_MULTIPART.getBytes();
        try (var stores = copyStores(copyPartSize, randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10))) {
            when(stores.sourceClient().headObject(any(HeadObjectRequest.class))).thenThrow(s3Error(RestStatus.NOT_FOUND, null));

            final var destination = new S3BlobContainer(BlobPath.EMPTY, stores.destination());
            final var source = new S3BlobContainer(BlobPath.EMPTY, stores.source());
            expectThrows(
                NoSuchFileException.class,
                () -> destination.copyBlob(
                    randomPurpose(),
                    source,
                    randomAlphaOfLengthBetween(1, 10),
                    randomAlphaOfLengthBetween(1, 10),
                    copyPartSize + 1,
                    null
                )
            );
            verify(stores.destinationClient(), never()).createMultipartUpload(any(CreateMultipartUploadRequest.class));
        }
    }

    /**
     * The HEAD request can fail in other ways than a missing blob. The copy then fails with an {@link IOException} that has the error of
     * the store as its cause, and the multipart upload does not start.
     */
    public void testCopyAboveTheLimitFailsIfTheHeadRequestIsRefused() {
        final long copyPartSize = S3Repository.MIN_PART_SIZE_USING_MULTIPART.getBytes();
        try (var stores = copyStores(copyPartSize, randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10))) {
            final S3Exception refusal = randomBoolean()
                ? s3Error(RestStatus.FORBIDDEN, "AccessDenied")
                : s3Error(RestStatus.INTERNAL_SERVER_ERROR, "InternalError");
            when(stores.sourceClient().headObject(any(HeadObjectRequest.class))).thenThrow(refusal);

            final IOException e = copyFails(stores, randomPurpose(), copyPartSize + 1);

            assertThat(e, not(instanceOf(NoSuchFileException.class)));
            assertThat(e.getMessage(), containsString("Unable to copy object"));
            assertThat(e.getCause(), sameInstance(refusal));
            verify(stores.destinationClient(), never()).createMultipartUpload(any(CreateMultipartUploadRequest.class));
        }
    }

    /**
     * An answer to the HEAD request without a content length gives no length for the parts, so the copy fails and the multipart upload
     * does not start.
     */
    public void testCopyAboveTheLimitFailsIfTheHeadAnswerHasNoLength() {
        final long copyPartSize = S3Repository.MIN_PART_SIZE_USING_MULTIPART.getBytes();
        try (var stores = copyStores(copyPartSize, randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10))) {
            when(stores.sourceClient().headObject(any(HeadObjectRequest.class))).thenReturn(HeadObjectResponse.builder().build());

            final IOException e = copyFails(stores, randomPurpose(), copyPartSize + 1);

            assertThat(e.getMessage(), containsString("has no content length"));
            verify(stores.destinationClient(), never()).createMultipartUpload(any(CreateMultipartUploadRequest.class));
        }
    }

    /**
     * S3 copies a source of up to 5 GB with one CopyObject request, and refuses a larger source with a 400 error, which the AWS
     * documentation gives the code InvalidRequest. The copy limit cannot be set above 5 GB, but the caller can need less than the limit
     * of a source that is longer than 5 GB. So the request can be refused for a size that the limit does not show. The copy then asks
     * for the length with a HEAD request, and goes in parts because the source is longer than the limit. The refusal is only what S3
     * documents, and the test cannot tell what a real S3 or an S3 compatible store answers, so it uses a mock.
     */
    public void testCopyThatS3RefusesAsTooLargeGoesInParts() throws IOException {
        assertRefusedCopyGoesInParts(s3Error(RestStatus.BAD_REQUEST, "InvalidRequest"));
    }

    /**
     * An S3 compatible store whose limit for one request is lower than 5 GB can refuse a copy with a 400 error that has another code,
     * or none. The copy does not look at the code. If the source is longer than the limit, it goes in parts, as for InvalidRequest.
     */
    public void testCopyRefusedWithAnotherCodeGoesInPartsIfTheSourceIsOverTheLimit() throws IOException {
        assertRefusedCopyGoesInParts(
            s3Error(RestStatus.BAD_REQUEST, randomFrom("InvalidArgument", "EntityTooLarge", "StoreSpecificCode", null))
        );
    }

    private void assertRefusedCopyGoesInParts(S3Exception refusal) throws IOException {
        final long minPartSize = S3Repository.MIN_PART_SIZE_USING_MULTIPART.getBytes();
        final long copyPartSize = randomLongBetween(minPartSize, minPartSize * 2);
        final long sourceLength = randomLongBetween(copyPartSize + 1, copyPartSize * 3);
        // the caller needs no more than the limit, so the copy starts with one CopyObject request
        final long neededEnd = randomLongBetween(1, copyPartSize);
        try (var stores = copyStores(copyPartSize, randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10))) {
            when(stores.destinationClient().copyObject(any(CopyObjectRequest.class))).thenThrow(refusal);
            when(stores.sourceClient().headObject(any(HeadObjectRequest.class))).thenReturn(headAnswer(sourceLength));
            answerMultipartCopy(stores.destinationClient(), randomAlphaOfLength(20));

            final var destination = new S3BlobContainer(BlobPath.EMPTY, stores.destination());
            final var source = new S3BlobContainer(BlobPath.EMPTY, stores.source());
            final var length = destination.copyBlob(
                randomPurpose(),
                source,
                randomAlphaOfLengthBetween(1, 10),
                randomAlphaOfLengthBetween(1, 10),
                neededEnd,
                null
            );

            assertThat(length, equalTo(OptionalLong.of(sourceLength)));
            verify(stores.destinationClient(), times(1)).copyObject(any(CopyObjectRequest.class));
            verify(stores.sourceClient(), times(1)).headObject(any(HeadObjectRequest.class));
            verify(stores.destinationClient(), times(1)).createMultipartUpload(any(CreateMultipartUploadRequest.class));
            verify(stores.destinationClient(), times(1)).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
            verify(stores.destinationClient(), times((int) ((sourceLength + copyPartSize - 1) / copyPartSize))).uploadPartCopy(
                any(UploadPartCopyRequest.class)
            );
        }
    }

    /**
     * A 400 error does not say that the source is too large. If the source is within the copy limit, the size is not the reason, and
     * the limit is at most what S3 copies with one request. Then the copy fails with the error of the refused request, as an
     * {@link IOException} that has it as its cause. The source can be shorter than the 5 MB of a part: a multipart copy of it would
     * fail with an {@link IllegalArgumentException} that hides the real error, and the copy must not start one.
     */
    public void testCopyRefusedWithA400ForASourceWithinTheLimitFailsWithTheOriginalError() {
        final long minPartSize = S3Repository.MIN_PART_SIZE_USING_MULTIPART.getBytes();
        final long copyPartSize = randomLongBetween(minPartSize, minPartSize * 2);
        final long sourceLength = randomBoolean() ? randomLongBetween(1, 300_000) : randomLongBetween(1, copyPartSize);
        final S3Exception refusal = s3Error(RestStatus.BAD_REQUEST, randomFrom("InvalidRequest", "InvalidArgument", null));
        try (var stores = copyStores(copyPartSize, randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10))) {
            when(stores.destinationClient().copyObject(any(CopyObjectRequest.class))).thenThrow(refusal);
            when(stores.sourceClient().headObject(any(HeadObjectRequest.class))).thenReturn(headAnswer(sourceLength));

            final IOException e = copyFails(stores, randomPurpose(), randomLongBetween(1, copyPartSize));

            assertThat(e.getMessage(), containsString("Unable to copy object"));
            assertThat(e.getCause(), sameInstance(refusal));
            verify(stores.sourceClient(), times(1)).headObject(any(HeadObjectRequest.class));
            verify(stores.destinationClient(), never()).createMultipartUpload(any(CreateMultipartUploadRequest.class));
        }
    }

    /**
     * If the HEAD request that follows a refused copy fails too, the refusal is the error of the copy, and the failed HEAD request is
     * attached to it.
     */
    public void testCopyRefusedWithA400AndAFailedHeadRequestFailsWithTheRefusal() {
        final long copyPartSize = S3Repository.MIN_PART_SIZE_USING_MULTIPART.getBytes();
        final S3Exception refusal = s3Error(RestStatus.BAD_REQUEST, "InvalidRequest");
        final S3Exception headFailure = s3Error(RestStatus.INTERNAL_SERVER_ERROR, "InternalError");
        try (var stores = copyStores(copyPartSize, randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10))) {
            when(stores.destinationClient().copyObject(any(CopyObjectRequest.class))).thenThrow(refusal);
            when(stores.sourceClient().headObject(any(HeadObjectRequest.class))).thenThrow(headFailure);

            final IOException e = copyFails(stores, randomPurpose(), 1L);

            assertThat(e.getCause(), sameInstance(refusal));
            assertThat(refusal.getSuppressed().length, equalTo(1));
            assertThat(refusal.getSuppressed()[0], sameInstance(headFailure));
            verify(stores.destinationClient(), never()).createMultipartUpload(any(CreateMultipartUploadRequest.class));
        }
    }

    /**
     * The refused CopyObject request is what sends the copy in parts. If the copy in parts fails too, the refusal is attached to that
     * failure as a suppressed exception, so that a reader of the failure sees why the copy went in parts.
     */
    public void testCopyInPartsThatFailsAfterARefusalKeepsTheRefusalAsSuppressed() {
        final long minPartSize = S3Repository.MIN_PART_SIZE_USING_MULTIPART.getBytes();
        final long copyPartSize = randomLongBetween(minPartSize, minPartSize * 2);
        final long sourceLength = randomLongBetween(copyPartSize + 1, copyPartSize * 3);
        final S3Exception refusal = s3Error(RestStatus.BAD_REQUEST, randomFrom("InvalidRequest", "InvalidArgument", null));
        try (var stores = copyStores(copyPartSize, randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10))) {
            when(stores.destinationClient().copyObject(any(CopyObjectRequest.class))).thenThrow(refusal);
            when(stores.sourceClient().headObject(any(HeadObjectRequest.class))).thenReturn(headAnswer(sourceLength));
            final OperationPurpose purpose;
            final String expectedMessage;
            if (randomBoolean()) {
                // the store cannot start the multipart upload
                purpose = randomPurpose();
                when(stores.destinationClient().createMultipartUpload(any(CreateMultipartUploadRequest.class))).thenThrow(
                    s3Error(RestStatus.INTERNAL_SERVER_ERROR, "InternalError")
                );
                expectedMessage = "using multipart upload";
            } else {
                // the answer that completes a snapshot copy has no ETag
                purpose = OperationPurpose.SNAPSHOT_DATA;
                answerMultipartCopy(stores.destinationClient(), null);
                when(stores.destinationClient().abortMultipartUpload(any(AbortMultipartUploadRequest.class))).thenReturn(
                    AbortMultipartUploadResponse.builder().build()
                );
                expectedMessage = "has no ETag";
            }

            final IOException e = copyFails(stores, purpose, randomLongBetween(1, copyPartSize));

            assertThat(e.getMessage(), containsString(expectedMessage));
            assertThat(e.getSuppressed().length, equalTo(1));
            assertThat(e.getSuppressed()[0], sameInstance(refusal));
        }
    }

    /**
     * Only a 400 error starts the length request and the copy in parts. Any other error of a CopyObject request fails the copy, without
     * a HEAD request and without a multipart upload.
     */
    public void testOtherCopyObjectErrorsDoNotGoInParts() {
        try (
            var stores = copyStores(
                S3Repository.MIN_PART_SIZE_USING_MULTIPART.getBytes(),
                randomAlphaOfLengthBetween(1, 10),
                randomAlphaOfLengthBetween(1, 10)
            )
        ) {
            final S3Exception error = randomFrom(
                s3Error(RestStatus.FORBIDDEN, "AccessDenied"),
                s3Error(RestStatus.INTERNAL_SERVER_ERROR, "InternalError"),
                s3Error(RestStatus.SERVICE_UNAVAILABLE, "SlowDown"),
                s3Error(RestStatus.CONFLICT, null)
            );
            when(stores.destinationClient().copyObject(any(CopyObjectRequest.class))).thenThrow(error);

            final IOException e = copyFails(stores, randomPurpose(), 1L);

            assertThat(e.getMessage(), containsString("Unable to copy object"));
            assertThat(e.getCause(), sameInstance(error));
            verify(stores.sourceClient(), never()).headObject(any(HeadObjectRequest.class));
            verify(stores.destinationClient(), never()).createMultipartUpload(any(CreateMultipartUploadRequest.class));
        }
    }

    /**
     * The client fails a copy that S3 answers with 200 and an error document. An answer to a CopyObject request without an ETag passes
     * that check, and it does not show that S3 made the copy. A snapshot copy has no other sign, so it fails.
     */
    public void testSnapshotCopyObjectAnswerWithoutAnEtagFails() {
        try (
            var stores = copyStores(
                S3Repository.MIN_PART_SIZE_USING_MULTIPART.getBytes(),
                randomAlphaOfLengthBetween(1, 10),
                randomAlphaOfLengthBetween(1, 10)
            )
        ) {
            when(stores.destinationClient().copyObject(any(CopyObjectRequest.class))).thenReturn(copyObjectAnswerWithoutAnEtag());

            final IOException e = copyFails(stores, OperationPurpose.SNAPSHOT_DATA, 1L);

            assertThat(e.getMessage(), containsString("has no ETag"));
        }
    }

    /**
     * Same as above for the answer that completes a multipart copy. A multipart upload that is not a copy needs no ETag in the answer.
     */
    public void testSnapshotMultipartCopyCompletedWithoutAnEtagFails() {
        final long minPartSize = S3Repository.MIN_PART_SIZE_USING_MULTIPART.getBytes();
        final long sourceLength = randomLongBetween(minPartSize + 1, minPartSize * 2);
        try (var stores = copyStores(minPartSize, randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10))) {
            when(stores.sourceClient().headObject(any(HeadObjectRequest.class))).thenReturn(headAnswer(sourceLength));
            answerMultipartCopy(stores.destinationClient(), randomBoolean() ? null : "");
            when(stores.destinationClient().abortMultipartUpload(any(AbortMultipartUploadRequest.class))).thenReturn(
                AbortMultipartUploadResponse.builder().build()
            );

            final IOException e = copyFails(stores, OperationPurpose.SNAPSHOT_DATA, sourceLength);

            assertThat(e.getMessage(), containsString("has no ETag"));
            // the multipart upload is aborted: the copy is not known to be done
            verify(stores.destinationClient(), times(1)).abortMultipartUpload(any(AbortMultipartUploadRequest.class));
        }
    }

    /**
     * Only a snapshot copy insists on the ETag. The copies of resharding, which are retried without end, and of the repository analysis
     * keep accepting an answer without an ETag, as they did before, so a store that never sends one does not block them.
     */
    public void testCopyOfAnotherPurposeThanSnapshotDataNeedsNoEtag() throws IOException {
        final OperationPurpose purpose = randomValueOtherThan(OperationPurpose.SNAPSHOT_DATA, () -> randomFrom(OperationPurpose.values()));
        final long minPartSize = S3Repository.MIN_PART_SIZE_USING_MULTIPART.getBytes();
        try (var stores = copyStores(minPartSize, randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10))) {
            final var destination = new S3BlobContainer(BlobPath.EMPTY, stores.destination());
            final var source = new S3BlobContainer(BlobPath.EMPTY, stores.source());

            // one CopyObject request, answered without an ETag
            when(stores.destinationClient().copyObject(any(CopyObjectRequest.class))).thenReturn(copyObjectAnswerWithoutAnEtag());
            assertThat(
                destination.copyBlob(purpose, source, randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10), 1L, null),
                equalTo(OptionalLong.empty())
            );

            // a multipart copy, whose completion is answered without an ETag
            final long sourceLength = randomLongBetween(minPartSize + 1, minPartSize * 2);
            when(stores.sourceClient().headObject(any(HeadObjectRequest.class))).thenReturn(headAnswer(sourceLength));
            answerMultipartCopy(stores.destinationClient(), randomBoolean() ? null : "");
            assertThat(
                destination.copyBlob(
                    purpose,
                    source,
                    randomAlphaOfLengthBetween(1, 10),
                    randomAlphaOfLengthBetween(1, 10),
                    sourceLength,
                    null
                ),
                equalTo(OptionalLong.of(sourceLength))
            );
            verify(stores.destinationClient(), times(1)).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
            verify(stores.destinationClient(), never()).abortMultipartUpload(any(AbortMultipartUploadRequest.class));
        }
    }

    private static CopyObjectResponse copyObjectAnswerWithoutAnEtag() {
        return randomFrom(
            CopyObjectResponse.builder().build(),
            CopyObjectResponse.builder().copyObjectResult(result -> {}).build(),
            CopyObjectResponse.builder().copyObjectResult(result -> result.eTag("")).build()
        );
    }

    /**
     * A source that goes away while the parts are copied fails an UploadPartCopy request with 404. The {@link NoSuchFileException} names
     * the source, which is what is missing, and not the new blob. The multipart upload is aborted.
     */
    public void testMultipartCopyOfASourceThatIsGoneNamesTheSource() {
        final long minPartSize = S3Repository.MIN_PART_SIZE_USING_MULTIPART.getBytes();
        final long sourceLength = randomLongBetween(minPartSize + 1, minPartSize * 2);
        final String sourceBlobName = randomAlphaOfLengthBetween(1, 10);
        final var sourcePath = BlobPath.EMPTY.add(randomAlphaOfLengthBetween(1, 10));
        final String blobName = randomAlphaOfLengthBetween(11, 20);
        try (var stores = copyStores(minPartSize, randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10))) {
            when(stores.sourceClient().headObject(any(HeadObjectRequest.class))).thenReturn(headAnswer(sourceLength));
            answerMultipartCopy(stores.destinationClient(), randomAlphaOfLength(20));
            when(stores.destinationClient().uploadPartCopy(any(UploadPartCopyRequest.class))).thenThrow(
                s3Error(RestStatus.NOT_FOUND, "NoSuchKey")
            );
            when(stores.destinationClient().abortMultipartUpload(any(AbortMultipartUploadRequest.class))).thenReturn(
                AbortMultipartUploadResponse.builder().build()
            );

            final var destination = new S3BlobContainer(BlobPath.EMPTY, stores.destination());
            final var source = new S3BlobContainer(sourcePath, stores.source());
            final var e = expectThrows(
                NoSuchFileException.class,
                () -> destination.copyBlob(randomPurpose(), source, sourceBlobName, blobName, sourceLength, null)
            );

            assertThat(e.getMessage(), containsString("Copy source [" + sourcePath.buildAsString() + sourceBlobName + "] not found"));
            assertThat(e.getMessage(), not(containsString(blobName)));
            verify(stores.destinationClient(), times(1)).abortMultipartUpload(any(AbortMultipartUploadRequest.class));
        }
    }

    private static S3Client configureMockClient(S3BlobStore blobStore) {
        final S3Client client = mock(S3Client.class);
        final SdkHttpClient httpClient = mock(SdkHttpClient.class);
        try (AmazonS3Reference clientReference = new AmazonS3Reference(client, httpClient)) {
            clientReference.mustIncRef(); // held by the mock, ultimately released in closeMockClient
            when(blobStore.clientReference()).then(invocation -> {
                clientReference.mustIncRef();
                return clientReference;
            });
            when(blobStore.getMetricPublisher(any(), any())).thenReturn(new MetricPublisher() {
                @Override
                public void publish(MetricCollection metricCollection) {}

                @Override
                public void close() {}
            });
        }
        return client;
    }

    private ConditionalOperation randomCondition() {
        return switch (between(0, 2)) {
            case 0 -> ConditionalOperation.NONE;
            case 1 -> ConditionalOperation.IF_NONE_MATCH;
            default -> ConditionalOperation.ifMatch(randomAlphanumericOfLength(128));
        };
    }

    private static void closeMockClient(S3BlobStore blobStore) {
        final var finalClientReference = blobStore.clientReference();
        assertFalse(finalClientReference.decRef());
        assertTrue(finalClientReference.decRef());
        assertFalse(finalClientReference.hasReferences());
    }

    public void testNumberOfMultipartsWithZeroPartSize() {
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> S3BlobContainer.numberOfMultiparts(randomNonNegativeLong(), 0L)
        );
        assertEquals("Part size must be greater than zero", e.getMessage());
    }

    public void testNumberOfMultiparts() {
        final ByteSizeUnit unit = randomFrom(ByteSizeUnit.BYTES, ByteSizeUnit.KB, ByteSizeUnit.MB, ByteSizeUnit.GB);
        final long size = unit.toBytes(randomIntBetween(2, 1000));
        final int factor = randomIntBetween(2, 10);

        // Fits in 1 empty part
        assertNumberOfMultiparts(1, 0L, 0L, size);

        // Fits in 1 part exactly
        assertNumberOfMultiparts(1, size, size, size);
        assertNumberOfMultiparts(1, size, size, size * factor);

        // Fits in N parts exactly
        assertNumberOfMultiparts(factor, size, size * factor, size);

        // Fits in N parts plus a bit more
        final long remaining = randomIntBetween(1, (size > Integer.MAX_VALUE) ? Integer.MAX_VALUE : (int) size - 1);
        assertNumberOfMultiparts(factor + 1, remaining, (size * factor) + remaining, size);
    }

    public void testInitCannedACL() {
        String[] aclList = new String[] {
            "private",
            "public-read",
            "public-read-write",
            "authenticated-read",
            "bucket-owner-read",
            "bucket-owner-full-control" };

        // empty acl
        assertThat(S3BlobStore.initCannedACL(null), equalTo(ObjectCannedACL.PRIVATE));
        assertThat(S3BlobStore.initCannedACL(""), equalTo(ObjectCannedACL.PRIVATE));

        // it should init cannedACL correctly
        for (String aclString : aclList) {
            ObjectCannedACL acl = S3BlobStore.initCannedACL(aclString);
            assertThat(acl.toString(), equalTo(aclString));
        }

        // it should accept all aws cannedACLs
        for (ObjectCannedACL awsList : ObjectCannedACL.values()) {
            ObjectCannedACL acl = S3BlobStore.initCannedACL(awsList.toString());
            assertThat(acl, equalTo(awsList));
        }
    }

    public void testInvalidCannedACL() {
        BlobStoreException ex = expectThrows(BlobStoreException.class, () -> S3BlobStore.initCannedACL("test_invalid"));
        assertThat(ex.getMessage(), equalTo("cannedACL is not valid: [test_invalid]"));
    }

    public void testInitStorageClassForAllowList() {
        // it should default to `standard`
        assertThat(S3BlobStore.initStorageClass(null, true), equalTo(StorageClass.STANDARD));
        assertThat(S3BlobStore.initStorageClass("", true), equalTo(StorageClass.STANDARD));

        for (final String name : EnumSet.allOf(StorageClass.class)
            .stream()
            .filter(S3BlobStore.ALLOWED_STORAGE_CLASSES::contains)
            .map(Enum::name)
            .toList()) {
            assertThat(S3BlobStore.initStorageClass(name, true), equalTo(StorageClass.valueOf(name)));
        }
    }

    public void testCaseInsensitiveStorageClass() {
        assertThat(S3BlobStore.initStorageClass("sTandaRd", true), equalTo(StorageClass.STANDARD));
        assertThat(S3BlobStore.initStorageClass("sTandaRd_Ia", true), equalTo(StorageClass.STANDARD_IA));
        assertThat(S3BlobStore.initStorageClass("oNeZoNe_iA", true), equalTo(StorageClass.ONEZONE_IA));
        assertThat(S3BlobStore.initStorageClass("reduCED_redundancy", true), equalTo(StorageClass.REDUCED_REDUNDANCY));
        assertThat(S3BlobStore.initStorageClass("intelLigeNt_tieriNG", true), equalTo(StorageClass.INTELLIGENT_TIERING));
        assertThat(S3BlobStore.initStorageClass("oUtPoSts", true), equalTo(StorageClass.OUTPOSTS));
        assertThat(S3BlobStore.initStorageClass("glaCiEr_iR", true), equalTo(StorageClass.GLACIER_IR));
        assertThat(S3BlobStore.initStorageClass("SnOw", true), equalTo(StorageClass.SNOW));
    }

    public void testInvalidStorageClass() {
        BlobStoreException ex = expectThrows(BlobStoreException.class, () -> S3BlobStore.initStorageClass("whatever", false));
        assertThat(ex.getMessage(), equalTo("`whatever` is not a known S3 Storage Class."));
    }

    public void testDisallowedKnownStorageClassesInAllowList() {
        for (final String name : EnumSet.allOf(StorageClass.class)
            .stream()
            .filter(
                storageClass -> StorageClass.UNKNOWN_TO_SDK_VERSION.equals(storageClass) == false
                    && S3BlobStore.ALLOWED_STORAGE_CLASSES.contains(storageClass) == false
            )
            .map(Enum::name)
            .toList()) {
            BlobStoreException ex = expectThrows(BlobStoreException.class, () -> S3BlobStore.initStorageClass(name, true));
            assertThat(ex.getMessage(), equalTo("`" + name + "` is not an allowed S3 Storage Class."));
        }
    }

    public void testRejectGlacierStorageClassForNonAllowList() {
        BlobStoreException ex = expectThrows(BlobStoreException.class, () -> S3BlobStore.initStorageClass("glacier", false));
        assertThat(ex.getMessage(), equalTo("Glacier storage class is not supported"));
    }

    private static void assertNumberOfMultiparts(final int expectedParts, final long expectedRemaining, long totalSize, long partSize) {
        final Tuple<Long, Long> result = S3BlobContainer.numberOfMultiparts(totalSize, partSize);

        assertEquals("Expected number of parts [" + expectedParts + "] but got [" + result.v1() + "]", expectedParts, (long) result.v1());
        assertEquals("Expected remaining [" + expectedRemaining + "] but got [" + result.v2() + "]", expectedRemaining, (long) result.v2());
    }
}
