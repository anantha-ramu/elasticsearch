/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.azure;

import fixture.azure.AzureHttpFixture;
import fixture.azure.MockAzureBlobStore;

import com.azure.core.exception.HttpResponseException;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.AccessTier;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.BlobRange;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.sas.BlobSasPermission;
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues;
import com.azure.storage.blob.specialized.BlockBlobClient;
import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

import org.apache.lucene.tests.util.TimeUnits;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.SecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.AbstractThirdPartyRepositoryTestCase;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.rest.RestStatus;
import org.junit.ClassRule;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.net.HttpURLConnection;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;
import java.util.zip.CheckedOutputStream;

import static org.elasticsearch.common.bytes.BytesReferenceTestUtils.equalBytes;
import static org.elasticsearch.common.io.Streams.limitStream;
import static org.elasticsearch.repositories.blobstore.BlobStoreTestUtil.randomPurpose;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.blankOrNullString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;

/**
 * These tests sometimes run against a genuine Azure endpoint with credentials obtained from Vault. These credentials expire periodically
 * and must be manually renewed; the process is in the onboarding/process docs.
 * Most tests that run against a genuine Azure endpoint can take between 1 and 3 minutes, hence the large suite timeout.
 */
@TimeoutSuite(millis = 40 * TimeUnits.MINUTE)
public class AzureStorageCleanupThirdPartyTests extends AbstractThirdPartyRepositoryTestCase {
    private static final Logger logger = LogManager.getLogger(AzureStorageCleanupThirdPartyTests.class);
    private static final boolean USE_FIXTURE = Booleans.parseBoolean(System.getProperty("test.azure.fixture", "true"));

    private static final String AZURE_ACCOUNT = System.getProperty("test.azure.account");

    @ClassRule
    public static AzureHttpFixture fixture = new AzureHttpFixture(
        USE_FIXTURE ? AzureHttpFixture.Protocol.HTTP : AzureHttpFixture.Protocol.NONE,
        null,
        AZURE_ACCOUNT,
        System.getProperty("test.azure.container"),
        System.getProperty("test.azure.tenant_id"),
        System.getProperty("test.azure.client_id"),
        AzureHttpFixture.sharedKeyForAccountPredicate(AZURE_ACCOUNT),
        MockAzureBlobStore.LeaseExpiryPredicate.NEVER_EXPIRE
    );

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(AzureRepositoryPlugin.class);
    }

    @Override
    protected Settings nodeSettings() {
        if (USE_FIXTURE) {
            final String endpoint = "ignored;DefaultEndpointsProtocol=http;BlobEndpoint=" + fixture.getAddress();
            return Settings.builder().put(super.nodeSettings()).put("azure.client.default.endpoint_suffix", endpoint).build();
        }
        return super.nodeSettings();
    }

    @Override
    protected SecureSettings credentials() {
        assertThat(System.getProperty("test.azure.account"), not(blankOrNullString()));
        final boolean hasSasToken = Strings.hasText(System.getProperty("test.azure.sas_token"));
        if (hasSasToken == false) {
            assertThat(System.getProperty("test.azure.key"), not(blankOrNullString()));
        } else {
            assertThat(System.getProperty("test.azure.key"), blankOrNullString());
        }
        assertThat(System.getProperty("test.azure.container"), not(blankOrNullString()));
        assertThat(System.getProperty("test.azure.base"), not(blankOrNullString()));

        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("azure.client.default.account", System.getProperty("test.azure.account"));
        if (hasSasToken) {
            logger.info("--> Using SAS token authentication");
            secureSettings.setString("azure.client.default.sas_token", System.getProperty("test.azure.sas_token"));
        } else {
            logger.info("--> Using key authentication");
            secureSettings.setString("azure.client.default.key", System.getProperty("test.azure.key"));
        }
        return secureSettings;
    }

    @Override
    protected void createRepository(String repoName) {
        final long blockSizeBytes = ByteSizeValue.ofKb(64L).getBytes() * randomIntBetween(5, 15);
        AcknowledgedResponse putRepositoryResponse = clusterAdmin().preparePutRepository(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            repoName
        )
            .setType("azure")
            .setSettings(
                Settings.builder()
                    .put("container", System.getProperty("test.azure.container"))
                    .put("base_path", System.getProperty("test.azure.base") + randomAlphaOfLength(8))
                    .put(AzureRepository.Repository.MAX_SINGLE_PART_UPLOAD_SIZE_SETTING.getKey(), ByteSizeValue.of(1, ByteSizeUnit.MB))
                    .put(AzureRepository.Repository.MULTIPART_UPLOAD_PART_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(blockSizeBytes))
            )
            .get();
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));
        if (Strings.hasText(System.getProperty("test.azure.sas_token"))) {
            ensureSasTokenPermissions();
        }
    }

    private void ensureSasTokenPermissions() {
        final BlobStoreRepository repository = getRepository();
        final PlainActionFuture<Void> future = new PlainActionFuture<>();
        repository.threadPool().generic().execute(ActionRunnable.wrap(future, l -> {
            final AzureBlobStore blobStore = (AzureBlobStore) repository.blobStore();
            final AzureBlobServiceClient azureBlobServiceClient = blobStore.getService()
                .client(ProjectId.DEFAULT, "default", LocationMode.PRIMARY_ONLY, randomFrom(OperationPurpose.values()));
            final BlobServiceClient client = azureBlobServiceClient.getSyncClient();
            try {
                final BlobContainerClient blobContainer = client.getBlobContainerClient(blobStore.toString());
                blobContainer.exists();
                future.onFailure(
                    new RuntimeException(
                        "The SAS token used in this test allowed for checking container existence. This test only supports tokens "
                            + "that grant only the documented permission requirements for the Azure repository plugin."
                    )
                );
            } catch (BlobStorageException e) {
                if (e.getStatusCode() == HttpURLConnection.HTTP_FORBIDDEN) {
                    future.onResponse(null);
                } else {
                    future.onFailure(e);
                }
            }
        }));
        future.actionGet();
    }

    public void testMultiBlockUpload() throws Exception {
        final BlobStoreRepository repo = getRepository();
        assertThat(
            asInstanceOf(AzureBlobStore.class, repo.blobStore()).getLargeBlobThresholdInBytes(),
            equalTo(ByteSizeUnit.MB.toBytes(1L))
        );
        assertThat(asInstanceOf(AzureBlobStore.class, repo.blobStore()).getUploadBlockSize(), lessThan(ByteSizeUnit.MB.toBytes(1L)));

        // The configured threshold for this test suite is 1mb
        final long blobSize = randomLongBetween(ByteSizeUnit.MB.toBytes(2), ByteSizeUnit.MB.toBytes(4));
        final int bufferSize = 8192;

        final var file = createTempFile();
        final long expectedChecksum;
        try (var output = new CheckedOutputStream(new BufferedOutputStream(Files.newOutputStream(file)), new CRC32())) {
            long remaining = blobSize;
            while (remaining > 0L) {
                final var buffer = randomByteArrayOfLength(Math.toIntExact(Math.min(bufferSize, remaining)));
                output.write(buffer);
                remaining -= buffer.length;
            }
            output.flush();
            expectedChecksum = output.getChecksum().getValue();
        }

        PlainActionFuture<Void> future = new PlainActionFuture<>();
        repo.threadPool().generic().execute(ActionRunnable.run(future, () -> {
            final BlobContainer blobContainer = repo.blobStore().blobContainer(repo.basePath().add("large_write"));
            try {
                final var blobName = UUIDs.base64UUID();
                if (randomBoolean()) {
                    try (var input = new BufferedInputStream(Files.newInputStream(file))) {
                        blobContainer.writeBlob(randomPurpose(), blobName, input, blobSize, false);
                    }
                } else {
                    assertThat(blobContainer.supportsConcurrentMultipartUploads(), equalTo(true));
                    blobContainer.writeBlobAtomic(randomPurpose(), blobName, blobSize, (offset, length) -> {
                        var channel = Files.newByteChannel(file);
                        if (offset > 0L) {
                            if (channel.size() <= offset) {
                                throw new AssertionError();
                            }
                            channel.position(offset);
                        }
                        assert channel.position() == offset;
                        return new BufferedInputStream(limitStream(Channels.newInputStream(channel), length));
                    }, false, Runnable::run);
                }

                long bytesCount = 0L;
                try (var input = new CheckedInputStream(blobContainer.readBlob(OperationPurpose.INDICES, blobName), new CRC32())) {
                    var buffer = new byte[bufferSize];
                    int bytesRead;
                    while ((bytesRead = input.read(buffer)) != -1) {
                        bytesCount += bytesRead;
                    }

                    assertThat(bytesCount, equalTo(blobSize));
                    assertThat(input.getChecksum().getValue(), equalTo(expectedChecksum));
                }
            } finally {
                blobContainer.delete(randomPurpose());
            }
        }));
        future.get();
    }

    public void testReadFromPositionLargerThanBlobLength() {
        testReadFromPositionLargerThanBlobLength(
            e -> asInstanceOf(BlobStorageException.class, ExceptionsHelper.unwrap(e, HttpResponseException.class))
                .getStatusCode() == RestStatus.REQUESTED_RANGE_NOT_SATISFIED.getStatus()
        );
    }

    public void testCopy() throws Exception {
        final var sourceBlobName = randomIdentifier();
        final var repository = getRepository();
        final var destinationBlobName = randomIdentifier();
        final var blobStore = repository.blobStore();
        final var sourceBlobContainer = blobStore.blobContainer(repository.basePath());
        final var blobBytes = randomBytesReference(randomIntBetween(100, 2_000_000));
        sourceBlobContainer.writeBlob(randomPurpose(), sourceBlobName, blobBytes, true);
        assertBusy(() -> assertTrue(sourceBlobContainer.blobExists(randomPurpose(), sourceBlobName)));

        final var destinationBlobContainer = repository.blobStore().blobContainer(repository.basePath().add("target"));
        final var length = destinationBlobContainer.copyBlob(
            randomPurpose(),
            sourceBlobContainer,
            sourceBlobName,
            destinationBlobName,
            blobBytes.length(),
            null
        );
        assertThat(Streams.readFully(destinationBlobContainer.readBlob(randomPurpose(), destinationBlobName)), equalBytes(blobBytes));
        // both containers are in one storage account, so Copy Blob copies the whole blob, and the copy does not learn its length
        assertThat(length, equalTo(OptionalLong.empty()));

        sourceBlobContainer.delete(randomPurpose());
        assertThrows(
            NoSuchFileException.class,
            () -> destinationBlobContainer.copyBlob(
                randomPurpose(),
                sourceBlobContainer,
                sourceBlobName,
                destinationBlobName,
                blobBytes.length(),
                null
            )
        );
        destinationBlobContainer.delete(randomPurpose());
    }

    /**
     * Probes an assumption of the TLA+ model of the copies: that Azure refuses a Put Block From URL request whose source range ends
     * after the last byte of the source blob, and does not copy the part that the source has. TLA+ is a language for describing a design
     * so that a tool can check every order of its steps, and the model is not part of this repository. The copy code does not depend on
     * the assumption. A copy of a blob between two storage accounts stages its blocks with such requests, but it asks Azure for the
     * length of the source first and never asks for a range past the end, see {@link AzureBlobStore#copyBlob}. Azure does not document
     * what it does with such a range, and the in-JVM fixture cannot show it: it clamps the range, as it does for Get Blob, unless a test
     * asks it to be strict. So the test needs a real storage account, and it is skipped with the fixture.
     * <p>
     * The test sends the requests with the SDK client, and not through {@link AzureBlobStore#copyBlob}, because that method copies
     * inside one storage account with Copy Blob, and the test account is only one. The source of the request is the URL of the source
     * blob with a SAS token, which Azure needs to read a source. The test uses the token of its settings, or makes one from the account
     * key. If the test fails because Azure copies the part of the range that the source has, the assumption of the model is wrong, and
     * the model needs another look. The copy code is not wrong then: its check of the length of the source is what keeps it from asking
     * for such a range.
     */
    public void testPutBlockFromUrlWithARangePastTheEndOfTheSourceFails() throws Exception {
        assumeFalse("only a real Azure account shows what Azure does, the fixture clamps the range", USE_FIXTURE);
        final var repository = getRepository();
        final var blobStore = asInstanceOf(AzureBlobStore.class, repository.blobStore());
        final var blobContainer = blobStore.blobContainer(repository.basePath().add("range_past_the_end"));
        final String keyPrefix = repository.basePath().add("range_past_the_end").buildAsString();
        final var sourceName = randomIdentifier();
        final var sourceBytes = randomBytesReference(between(100, 2_000));
        blobContainer.writeBlob(randomPurpose(), sourceName, sourceBytes, true);
        try (
            var azureClient = blobStore.getService()
                .client(ProjectId.DEFAULT, "default", LocationMode.PRIMARY_ONLY, OperationPurpose.SNAPSHOT_DATA)
        ) {
            final BlobContainerClient containerClient = azureClient.getSyncClient().getBlobContainerClient(blobStore.toString());
            final BlobClient sourceClient = containerClient.getBlobClient(keyPrefix + sourceName);
            assertThat(sourceClient.getProperties().getBlobSize(), equalTo((long) sourceBytes.length()));
            final String sasToken = System.getProperty("test.azure.sas_token");
            final String sourceUrl = sourceClient.getBlobUrl()
                + "?"
                + (Strings.hasText(sasToken)
                    ? sasToken.startsWith("?") ? sasToken.substring(1) : sasToken
                    : sourceClient.generateSas(
                        new BlobServiceSasSignatureValues(OffsetDateTime.now(ZoneOffset.UTC).plusMinutes(30), BlobSasPermission.parse("r"))
                    ));
            final BlockBlobClient targetClient = containerClient.getBlobClient(keyPrefix + randomIdentifier()).getBlockBlobClient();
            final long length = sourceBytes.length();

            // a range that ends at the last byte of the source works
            targetClient.stageBlockFromUrl(newBlockId(), sourceUrl, new BlobRange(0, length));

            // a range that ends after the last byte of the source, and a range that starts after it, must fail
            for (BlobRange range : List.of(new BlobRange(0, length + between(1, 1_000)), new BlobRange(length, (long) between(1, 1_000)))) {
                final var e = expectThrows(
                    BlobStorageException.class,
                    "range [" + range + "] of a source of [" + length + "] bytes",
                    () -> targetClient.stageBlockFromUrl(newBlockId(), sourceUrl, range)
                );
                logger.info(
                    "--> Azure answered [{}] [{}] to the range [{}] of a source of [{}] bytes",
                    e.getStatusCode(),
                    e.getErrorCode(),
                    range,
                    length
                );
                assertThat(e.getStatusCode(), allOf(greaterThanOrEqualTo(400), lessThan(500)));
            }
        } finally {
            blobContainer.delete(randomPurpose());
        }
    }

    /**
     * A block id for a Put Block or a Put Block From URL request. All the blocks of a blob need ids of the same length, and these have 22
     * characters.
     */
    private static String newBlockId() {
        return Base64.getEncoder().withoutPadding().encodeToString(Base64.getUrlDecoder().decode(UUIDs.base64UUID()));
    }

    /**
     * Verifies that the {@code data_access_tier} / {@code metadata_access_tier} settings are applied to the correct uploads. We configure
     * two distinct access tiers, write blobs with each {@link OperationPurpose} via the single-part, multi-part and server-side copy paths,
     * and read the access tier back from the blob properties.
     */
    public void testAccessTierPerOperationPurpose() throws Exception {
        final List<AccessTier> accessTiers = shuffledList(new ArrayList<>(AzureBlobStore.ALLOWED_ACCESS_TIERS_BY_LOWER_NAME.values()));
        final AccessTier dataAccessTier = accessTiers.get(0);
        final AccessTier metadataAccessTier = accessTiers.get(1);
        logger.info("--> data_access_tier [{}], metadata_access_tier [{}]", dataAccessTier, metadataAccessTier);

        final String repoName = "test-access-tier-repo";
        final AcknowledgedResponse putRepositoryResponse = clusterAdmin().preparePutRepository(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            repoName
        )
            .setType("azure")
            .setSettings(
                Settings.builder()
                    .put("container", System.getProperty("test.azure.container"))
                    .put("base_path", System.getProperty("test.azure.base") + randomAlphaOfLength(8))
                    .put(AzureRepository.Repository.MAX_SINGLE_PART_UPLOAD_SIZE_SETTING.getKey(), ByteSizeValue.of(1, ByteSizeUnit.MB))
                    .put(AzureRepository.Repository.MULTIPART_UPLOAD_PART_SIZE_SETTING.getKey(), ByteSizeValue.of(1, ByteSizeUnit.MB))
                    .put(AzureRepository.Repository.DATA_ACCESS_TIER_SETTING.getKey(), dataAccessTier.toString())
                    .put(AzureRepository.Repository.METADATA_ACCESS_TIER_SETTING.getKey(), metadataAccessTier.toString())
            )
            .get();
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));

        try {
            final BlobStoreRepository repository = (BlobStoreRepository) getInstanceFromNode(RepositoriesService.class).repository(
                repoName
            );
            final AzureBlobStore blobStore = asInstanceOf(AzureBlobStore.class, repository.blobStore());
            final BlobContainer blobContainer = blobStore.blobContainer(repository.basePath());
            final String keyPrefix = repository.basePath().buildAsString();
            try {
                // a purpose that resolves to no explicit access tier (i.e. neither SNAPSHOT_DATA nor SNAPSHOT_METADATA)
                final OperationPurpose noTierPurpose = randomFrom(
                    OperationPurpose.REPOSITORY_ANALYSIS,
                    OperationPurpose.CLUSTER_STATE,
                    OperationPurpose.INDICES,
                    OperationPurpose.TRANSLOG
                );
                for (final OperationPurpose purpose : List.of(
                    OperationPurpose.SNAPSHOT_DATA,
                    OperationPurpose.SNAPSHOT_METADATA,
                    noTierPurpose
                )) {
                    final Optional<AccessTier> expectedTier = blobStore.resolveAccessTier(purpose);

                    // single-part upload (blob stays below the multipart threshold)
                    final String singlePartName = randomIdentifier();
                    final BytesReference singlePartBytes = randomBytesReference(between(1, 512));
                    blobContainer.writeBlob(purpose, singlePartName, singlePartBytes, true);
                    assertAccessTier(blobStore, keyPrefix + singlePartName, expectedTier, "single-part upload", purpose);

                    // multi-part upload (exceeds the single-part threshold so the block list upload path is used)
                    final String multiPartName = randomIdentifier();
                    final int multiPartSize = Math.toIntExact(blobStore.getLargeBlobThresholdInBytes()) + between(1, 1024);
                    final byte[] multiPartBytes = randomByteArrayOfLength(multiPartSize);
                    blobContainer.writeBlob(purpose, multiPartName, new ByteArrayInputStream(multiPartBytes), multiPartSize, true);
                    assertAccessTier(blobStore, keyPrefix + multiPartName, expectedTier, "multi-part upload", purpose);

                    // server-side copy (source is the small single-part blob written above)
                    final String copyName = randomIdentifier();
                    blobContainer.copyBlob(purpose, blobContainer, singlePartName, copyName, singlePartBytes.length(), null);
                    assertAccessTier(blobStore, keyPrefix + copyName, expectedTier, "server-side copy", purpose);
                }
            } finally {
                blobContainer.delete(randomPurpose());
            }
        } finally {
            clusterAdmin().prepareDeleteRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, repoName).get();
        }
    }

    private static void assertAccessTier(
        AzureBlobStore blobStore,
        String blobKey,
        Optional<AccessTier> expectedTier,
        String uploadType,
        OperationPurpose purpose
    ) {
        final BlobServiceClient client = blobStore.getService()
            .client(ProjectId.DEFAULT, "default", LocationMode.PRIMARY_ONLY, purpose)
            .getSyncClient();
        final BlobClient blobClient = client.getBlobContainerClient(blobStore.toString()).getBlobClient(blobKey);
        final BlobProperties properties = blobClient.getProperties();
        final String message = uploadType + " with purpose [" + purpose + "]";
        if (expectedTier.isPresent()) {
            assertThat(message, properties.getAccessTier(), equalTo(expectedTier.get()));
            assertThat(message + " should carry an explicit tier", Boolean.TRUE.equals(properties.isAccessTierInferred()), is(false));
        } else {
            // No tier configured: the fixture reports no tier, while a real account reports its inferred default tier
            assertThat(
                message + " should not carry an explicit tier",
                properties.getAccessTier() == null || Boolean.TRUE.equals(properties.isAccessTierInferred()),
                is(true)
            );
        }
    }
}
