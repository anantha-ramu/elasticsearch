/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.fs.FsBlobContainer;
import org.elasticsearch.common.blobstore.fs.FsBlobStore;
import org.elasticsearch.common.blobstore.support.FilterBlobContainer;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.lucene.store.InputStreamIndexInput;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotFailedException;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshots;
import org.elasticsearch.index.snapshots.blobstore.SnapshotFiles;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.repositories.SnapshotIndexCommit;
import org.elasticsearch.repositories.SnapshotShardContext;
import org.elasticsearch.snapshots.AbortedSnapshotException;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.stateless.commits.BatchedCompoundCommit;
import org.elasticsearch.xpack.stateless.commits.BlobFile;
import org.elasticsearch.xpack.stateless.commits.BlobLocation;
import org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommit;
import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;
import org.elasticsearch.xpack.stateless.lucene.StatelessCommitRef;
import org.elasticsearch.xpack.stateless.test.FakeStatelessNode;
import org.junit.After;

import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import static org.elasticsearch.xpack.stateless.commits.StatelessCommitServiceTests.waitUntilBCCIsUploaded;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

/**
 * Tests of {@link StatelessSnapshotShardContext}.
 * <p>
 * Terms used in the tests of the copy source:
 * <ul>
 * <li>A commit blob is a blob in the object store that holds the files of one or more commits of a shard.</li>
 * <li>A commit header is the start of each commit in a commit blob. It lists the files of the commit and, from that, where each file
 * is stored.</li>
 * <li>A generational file is a file that a segment gets after the segment was first written, when a document in the segment is updated
 * or deleted. The shard copies each generational file that is still in use into the first commit of every new commit blob.</li>
 * <li>A commit carries a generational file when the file first appeared in an older commit blob than the commit's own.</li>
 * </ul>
 */
public class StatelessSnapshotShardContextTests extends ESTestCase {

    // the reads with the purpose SNAPSHOT_METADATA that the object store of a test node has served, see MetadataReadContainer
    private final Queue<MetadataRead> metadataReads = ConcurrentCollections.newQueue();
    private volatile MetadataReadOverride metadataReadOverride;

    @After
    public void resetMetadataReads() {
        metadataReads.clear();
        metadataReadOverride = null;
    }

    public void testBlobStoreFileReader() throws IOException {
        final var shardId = new ShardId(new Index(randomIdentifier(), randomUUID()), between(0, 5));

        final var blobContainer = createBlobContainer();

        final var blobLength = between(500, 5000);
        final var bytes = randomByteArrayOfLength(blobLength);
        final long generation = 42L;
        final String blobName = "stateless_commit_" + generation;
        blobContainer.writeBlob(
            randomFrom(OperationPurpose.values()),
            blobName,
            new ByteArrayInputStream(bytes),
            blobLength,
            randomBoolean()
        );
        final var commitRefReleased = new AtomicBoolean();
        final var snapshotIndexCommit = new SnapshotIndexCommit(new Engine.IndexCommitRef(null, () -> commitRefReleased.set(true)));
        assertThat(snapshotIndexCommit.refCount(), equalTo(1));

        final var fileOffset = between(0, 50);
        final var fileLength = blobLength - 50;
        final List<CloseTrackingInputStream> allOpenedStreams = new ArrayList<>();

        // Sometimes abort the read halfway through
        final var abortPosition = between(0, 3) == 3 ? randomIntBetween(fileOffset, blobLength) : Integer.MAX_VALUE;
        final var snapshotShardContext = new StatelessSnapshotShardContext(
            shardId,
            new SnapshotId(randomIdentifier(), randomUUID()),
            new IndexId(shardId.getIndexName(), randomUUID()),
            randomIdentifier(),
            IndexShardSnapshotStatus.newInitializing(ShardGeneration.newGeneration(), randomLongBetween(1, Long.MAX_VALUE)),
            IndexVersion.current(),
            randomNonNegativeLong(),
            snapshotIndexCommit,
            Map.of("file", new BlobLocation(new BlobFile(blobName, new PrimaryTermAndGeneration(1L, generation)), fileOffset, fileLength)),
            (s, g) -> new FilterBlobContainer(blobContainer) {
                @Override
                protected BlobContainer wrapChild(BlobContainer child) {
                    throw new AssertionError("should not obtain child");
                }

                @Override
                public InputStream readBlob(OperationPurpose purpose, String blobName, long position, long length) throws IOException {
                    assertThat(purpose, equalTo(OperationPurpose.SNAPSHOT_DATA));
                    final var inputStream = new CloseTrackingInputStream(
                        super.readBlob(purpose, blobName, position, length),
                        position >= abortPosition
                    );
                    allOpenedStreams.add(inputStream);
                    return inputStream;
                }
            },
            () -> false,
            new PlainActionFuture<>()
        );

        final var closeEachInputStream = rarely();
        final var numberOfParts = between(0, 10);
        try (var fileReader = snapshotShardContext.fileReader("file", mock(StoreFileMetadata.class))) {
            assertThat(snapshotIndexCommit.refCount(), equalTo(2));
            if (numberOfParts != 0) {
                final var fileContentRead = readBlobContent(fileReader, numberOfParts, fileLength, closeEachInputStream);
                if (abortPosition == Integer.MAX_VALUE) { // when not aborted
                    assertThat(fileContentRead.length, equalTo(fileLength));
                }
                assertArrayEquals(Arrays.copyOfRange(bytes, fileOffset, fileOffset + fileContentRead.length), fileContentRead);
            }
        }
        assertFalse(commitRefReleased.get());
        assertThat(snapshotIndexCommit.refCount(), equalTo(1));

        for (CloseTrackingInputStream inputStream : allOpenedStreams) {
            assertTrue("Expected all opened input streams to be closed", inputStream.isClosed());
        }
    }

    /**
     * A snapshot repository keeps one folder for all indices that have the same name. So an index that is deleted and created again with
     * the same name shares the folder, and the shard generation in it, with the old index. The new index must not reuse a copy of a commit
     * blob of the old index. A copy is found by the source path of its commit blob, so the path must differ between the two indices. It
     * does, because it holds the index UUID.
     */
    public void testCopySourceDiffersForIndicesWithTheSameName() throws IOException {
        final String indexName = randomIdentifier();
        final int shard = between(0, 5);
        final var blobLocation = new BlobLocation(
            new BlobFile("stateless_commit_7", new PrimaryTermAndGeneration(1L, 7L)),
            between(0, 50),
            between(100, 1000)
        );
        final var oldCopySource = copySource(new ShardId(new Index(indexName, randomUUID()), shard), blobLocation);
        final var newCopySource = copySource(new ShardId(new Index(indexName, randomUUID()), shard), blobLocation);
        assertThat(newCopySource.source(), not(equalTo(oldCopySource.source())));

        // The shard generation holds a copy of the old index's commit blob. The old index finds it, and the new index does not.
        final var metadata = new StoreFileMetadata("_0.cfs", blobLocation.fileLength(), "checksum", Version.LATEST.toString());
        final var generation = BlobStoreIndexShardSnapshots.EMPTY.withAddedSnapshot(
            new SnapshotFiles(
                "snapshot",
                List.of(new FileInfo("__file", metadata, "c__old", oldCopySource.offset(), oldCopySource.source())),
                null
            )
        );
        assertThat(generation.findCopiedBlob(oldCopySource.source()), equalTo("c__old"));
        assertThat(generation.findCopiedBlob(newCopySource.source()), nullValue());
    }

    /**
     * The repository puts the copy name after the shard generation in the name of a copy, and the sources that one shard snapshot copies
     * need different copy names. Within a shard, the primary term and the name of a commit blob identify the commit blob, so the copy name
     * holds both.
     */
    public void testCopyNameIdentifiesTheCommitBlobWithinTheShard() throws IOException {
        final var shardId = new ShardId(new Index(randomIdentifier(), randomUUID()), between(0, 5));
        final long primaryTerm = randomLongBetween(1, 100);
        final long generation = randomLongBetween(1, 1000);
        final var copyName = copySource(shardId, commitBlobLocation(primaryTerm, generation)).copyName();

        assertThat(copyName, equalTo(primaryTerm + "_stateless_commit_" + generation));
        assertTrue(Strings.validFileName(copyName));
        assertThat(copySource(shardId, commitBlobLocation(primaryTerm, generation)).copyName(), equalTo(copyName));
        // a commit blob with another generation, or with another primary term, has another copy name
        assertThat(copySource(shardId, commitBlobLocation(primaryTerm, generation + 1)).copyName(), not(equalTo(copyName)));
        assertThat(copySource(shardId, commitBlobLocation(primaryTerm + 1, generation)).copyName(), not(equalTo(copyName)));
    }

    /**
     * A generational file is stored in more than one commit blob, because each new commit blob copies the generational files. The snapshot
     * points the file at the commit blob of the snapshotted commit. So an earlier file info, which points at a copy of an older commit
     * blob, can be reused only while the file is in the same commit blob. If the file is at another offset of the same commit blob, the
     * earlier file info can still be reused.
     */
    public void testGenerationalFileIsReusableOnlyInTheSameCommitBlob() throws IOException {
        final var shardId = new ShardId(new Index(randomIdentifier(), randomUUID()), between(0, 5));
        final String file = randomFrom("_0_1.liv", "_3_2.fnm", "_12_1_Lucene90_0.dvd", "_7_4_Lucene90_0.dvm");
        final long primaryTerm = randomLongBetween(1, 100);
        final long generation = randomLongBetween(2, 1000);
        final var earlierFileInfo = copiedFileInfo(shardId, file, commitBlobLocation(primaryTerm, generation));

        final var sameBlob = new BlobLocation(
            new BlobFile("stateless_commit_" + generation, new PrimaryTermAndGeneration(primaryTerm, generation)),
            between(0, 50),
            between(100, 1000)
        );
        assertTrue(contextWithFile(shardId, file, sameBlob, true).isCopiedFileReusable(earlierFileInfo));
        // a newer commit blob of the same primary term, and a commit blob of a newer primary term
        assertFalse(
            contextWithFile(shardId, file, commitBlobLocation(primaryTerm, generation + 1), true).isCopiedFileReusable(earlierFileInfo)
        );
        assertFalse(
            contextWithFile(shardId, file, commitBlobLocation(primaryTerm + 1, generation), true).isCopiedFileReusable(earlierFileInfo)
        );
    }

    /**
     * A file that is not a generational file stays in the commit blob that it was written to, so its earlier file info stays reusable. The
     * context keeps the reuse that the snapshot did before, whatever commit blob holds the file.
     */
    public void testFileThatIsNotGenerationalIsReusableInAnotherCommitBlob() throws IOException {
        final var shardId = new ShardId(new Index(randomIdentifier(), randomUUID()), between(0, 5));
        final String file = randomFrom("_0.cfs", "_3.fdt", "_12_Lucene90_0.dvd", "_7_Lucene90_0.dvm");
        final long primaryTerm = randomLongBetween(1, 100);
        final long generation = randomLongBetween(2, 1000);
        final var earlierFileInfo = copiedFileInfo(shardId, file, commitBlobLocation(primaryTerm, generation));

        assertTrue(
            contextWithFile(shardId, file, commitBlobLocation(primaryTerm, generation + 1), true).isCopiedFileReusable(earlierFileInfo)
        );
    }

    /**
     * With server-side copy off, the context offers no copy source. A snapshot that refused to reuse a copied generational file would
     * then upload the file again, so the context keeps reusing the earlier file info. Turning copies off must not upload files again that
     * are in the repository as copies.
     */
    public void testCopiedGenerationalFileIsReusableWhenCopiesAreOff() throws IOException {
        final var shardId = new ShardId(new Index(randomIdentifier(), randomUUID()), between(0, 5));
        final String file = randomFrom("_0_1.liv", "_3_2.fnm", "_12_1_Lucene90_0.dvd", "_7_4_Lucene90_0.dvm");
        final long primaryTerm = randomLongBetween(1, 100);
        final long generation = randomLongBetween(2, 1000);
        final var earlierFileInfo = copiedFileInfo(shardId, file, commitBlobLocation(primaryTerm, generation));

        final var context = contextWithFile(shardId, file, commitBlobLocation(primaryTerm, generation + 1), false);
        assertThat(context.copySource(file), nullValue());
        assertTrue(context.isCopiedFileReusable(earlierFileInfo));
    }

    /**
     * Each commit blob starts with copies of the generational files that were in use then. So a commit that carries a generational file
     * has a copy of the file in its own commit blob, and the header of the commit lists that copy. The copy source of the file is that
     * copy, both when the commit is the first one in its commit blob and when the commit comes later. A generational file that a commit
     * wrote into the same commit blob keeps the location where it first appeared, which is already in that commit blob.
     */
    public void testCarriedGenerationalFilesAreCopiedFromTheCommitsOwnCommitBlob() throws Exception {
        try (var node = newNode()) {
            // The first commit blob has generational files, which the later commit blobs carry. The second commit blob has commits that
            // come after a commit that wrote generational files into the same commit blob.
            final var uploadedCommits = uploadCommitBlobs(node, between(2, 3), 3, between(1, 3));

            boolean carriedByFirstCommit = false;
            boolean carriedByLaterCommit = false;
            boolean keptByLaterCommit = false;
            for (UploadedCommit uploaded : uploadedCommits) {
                final var context = contextOf(node, uploaded.commit(), true);
                final var header = readCommitHeader(node, uploaded);
                for (String file : uploaded.commit().getFileNames()) {
                    // the copy source is where the header of the commit itself lists the file
                    final BlobLocation listed = header.commitFiles().get(file);
                    final var copySource = context.copySource(file);
                    assertThat(copySource.blobName(), equalTo(listed.blobName()));
                    assertThat(copySource.offset(), equalTo(listed.offset()));

                    if (StatelessCompoundCommit.isGenerationalFile(file)) {
                        // the location where the commit service saw the file first, and where a snapshot took the file from before
                        final BlobLocation firstLocation = node.commitService.getBlobLocation(node.shardId, file);
                        if (firstLocation.equals(listed)) {
                            keptByLaterCommit |= uploaded.isFirstInBlob() == false
                                && firstLocation.compoundFileGeneration() == uploaded.blobGeneration();
                        } else {
                            assertThat(firstLocation.compoundFileGeneration(), lessThan(uploaded.blobGeneration()));
                            assertThat(listed.compoundFileGeneration(), equalTo(uploaded.blobGeneration()));
                            carriedByFirstCommit |= uploaded.isFirstInBlob();
                            carriedByLaterCommit |= uploaded.isFirstInBlob() == false;
                        }
                    }
                }
            }
            assertTrue("no first commit of a commit blob carries a generational file", carriedByFirstCommit);
            assertTrue("no later commit of a commit blob carries a generational file", carriedByLaterCommit);
            assertTrue("no later commit keeps a generational file that was written into its commit blob", keptByLaterCommit);
        }
    }

    /**
     * The files of a commit that carries generational files need exactly one GET: the start of its commit blob, up to its
     * {@code segments_N} file. The start of the commit blob is the header of its first commit, which lists a copy of each carried file.
     * The context keeps the header for the other files. Assertions are on in a test. They would trip if the read went through the
     * iterator over all the commits of the commit blob, with the position of {@code segments_N} as its end. That fails when the
     * snapshotted commit is the first commit of the blob, because that commit extends past the end. The assertions would also trip if
     * the read checked the padding between two commits with a second GET. A commit that carries no generational file needs no GET, and
     * with server-side copy off no file has a copy source.
     */
    public void testCarriedGenerationalFilesNeedOneReadOfTheStartOfTheCommitBlob() throws Exception {
        try (var node = newNode()) {
            final var uploadedCommits = uploadCommitBlobs(node, between(2, 3), 3, between(1, 3));

            boolean readForFirstCommit = false;
            boolean readForLaterCommit = false;
            boolean noReadForCommit = false;
            for (UploadedCommit uploaded : uploadedCommits) {
                metadataReads.clear();
                final var context = contextOf(node, uploaded.commit(), true);
                // the second round shows that the context reads the header once for all the files
                for (int round = 0; round < 2; round++) {
                    for (String file : uploaded.commit().getFileNames()) {
                        context.copySource(file);
                    }
                }

                if (carriedGenerationalFiles(node, uploaded).isEmpty()) {
                    assertThat(metadataReads, empty());
                    noReadForCommit = true;
                } else {
                    final var segmentsFile = node.commitService.getBlobLocation(node.shardId, uploaded.commit().getSegmentsFileName());
                    final var blobName = BatchedCompoundCommit.blobNameFromGeneration(uploaded.blobGeneration());
                    assertThat(List.copyOf(metadataReads), equalTo(List.of(new MetadataRead(blobName, 0L, segmentsFile.offset()))));
                    readForFirstCommit |= uploaded.isFirstInBlob();
                    readForLaterCommit |= uploaded.isFirstInBlob() == false;
                }
            }
            assertTrue("no first commit of a commit blob needed a read", readForFirstCommit);
            assertTrue("no later commit of a commit blob needed a read", readForLaterCommit);
            assertTrue("every commit needed a read", noReadForCommit);

            // with server-side copy off, no file has a copy source, and nothing is read
            metadataReads.clear();
            final var copiesOff = contextOf(node, uploadedCommits.getLast().commit(), false);
            for (String file : uploadedCommits.getLast().commit().getFileNames()) {
                assertThat(copiesOff.copySource(file), nullValue());
            }
            assertThat(metadataReads, empty());
        }
    }

    /**
     * If the read of the first header fails, the copy source fails with {@link IndexShardSnapshotFailedException}. That fails the shard
     * snapshot, as the failed read of a footer does. The context does not fall back to the first location of the file.
     */
    public void testFailedReadOfTheFirstHeaderFailsTheShardSnapshot() throws Exception {
        try (var node = newNode()) {
            final var uploaded = uploadCommitBlobs(node, 2, 2).getLast();
            final String carriedFile = carriedGenerationalFiles(node, uploaded).getFirst();
            metadataReadOverride = (container, read) -> { throw new IOException("simulated failure of the read"); };
            final var context = contextOf(node, uploaded.commit(), true);

            final var failure = expectThrows(IndexShardSnapshotFailedException.class, () -> context.copySource(carriedFile));

            assertThat(failure.getMessage(), containsString("failed to read the first commit header of commit blob [stateless_commit_"));
            assertThat(failure.getCause(), instanceOf(IOException.class));
        }
    }

    /**
     * Every commit blob starts with copies of the generational files that were in use then, so a first header without the copy of a
     * carried file is not expected. In production, the file keeps its first location. A test runs with assertions, which fail.
     */
    public void testFirstHeaderWithoutTheCopyOfACarriedFileFailsAnAssertion() throws Exception {
        try (var node = newNode()) {
            final var uploadedCommits = uploadCommitBlobs(node, 2, 2);
            final String carriedFile = carriedGenerationalFiles(node, uploadedCommits.getLast()).getFirst();
            // the start of the first commit blob, which has the header of a commit with no generational file
            final String firstBlobName = BatchedCompoundCommit.blobNameFromGeneration(uploadedCommits.getFirst().blobGeneration());
            metadataReadOverride = (container, read) -> {
                final long firstBlobLength = container.listBlobs(OperationPurpose.INDICES).get(firstBlobName).length();
                return container.readBlob(OperationPurpose.SNAPSHOT_METADATA, firstBlobName, 0L, firstBlobLength);
            };
            final var context = contextOf(node, uploadedCommits.getLast().commit(), true);

            final var failure = expectThrows(AssertionError.class, () -> context.copySource(carriedFile));

            assertThat(failure.getMessage(), containsString("has no copy of generational file [" + carriedFile + "]"));
        }
    }

    /**
     * A node with an object store whose reads with the purpose SNAPSHOT_METADATA the test can see and replace.
     */
    private FakeStatelessNode newNode() throws IOException {
        return new FakeStatelessNode(this::newEnvironment, this::newNodeEnvironment, xContentRegistry(), randomIntBetween(1, 100)) {
            @Override
            public BlobContainer wrapBlobContainer(BlobPath path, BlobContainer innerContainer) {
                return new MetadataReadContainer(innerContainer);
            }
        };
    }

    /**
     * A commit that a test has uploaded, and the generation of its commit blob, which is the generation of the first commit in the blob.
     */
    private record UploadedCommit(StatelessCommitRef commit, long blobGeneration) {
        boolean isFirstInBlob() {
            return commit.getGeneration() == blobGeneration;
        }
    }

    /**
     * Makes commits with generational files that stay the same in the later commits, see
     * {@link FakeStatelessNode#generateCommitsWithUnchangedGenerationalFiles}, and uploads them in commit blobs of the given sizes. The
     * commit blob of each group ends with the last commit of the group.
     */
    private static List<UploadedCommit> uploadCommitBlobs(FakeStatelessNode node, int... commitsPerBlob) throws IOException {
        final var commits = node.generateCommitsWithUnchangedGenerationalFiles(IntStream.of(commitsPerBlob).sum());
        final var uploadedCommits = new ArrayList<UploadedCommit>();
        int next = 0;
        for (int size : commitsPerBlob) {
            final var commitsOfBlob = commits.subList(next, next + size);
            next += size;
            for (StatelessCommitRef commit : commitsOfBlob) {
                node.commitService.onCommitCreation(commit);
            }
            final long lastGeneration = commitsOfBlob.getLast().getGeneration();
            node.commitService.ensureMaxGenerationToUploadForFlush(node.shardId, lastGeneration);
            waitUntilBCCIsUploaded(node.commitService, node.shardId, lastGeneration);
            for (StatelessCommitRef commit : commitsOfBlob) {
                uploadedCommits.add(new UploadedCommit(commit, commitsOfBlob.getFirst().getGeneration()));
            }
        }
        return uploadedCommits;
    }

    /**
     * The context of a snapshot of the commit, with the first location of each file that the commit service of the node has learned,
     * as the primary gives them to a snapshot.
     */
    private static StatelessSnapshotShardContext contextOf(FakeStatelessNode node, StatelessCommitRef commit, boolean serverSideCopyEnabled)
        throws IOException {
        final Map<String, BlobLocation> firstLocations = new HashMap<>();
        for (String file : commit.getFileNames()) {
            firstLocations.put(file, node.commitService.getBlobLocation(node.shardId, file));
        }
        return new StatelessSnapshotShardContext(
            node.shardId,
            new SnapshotId(randomIdentifier(), randomUUID()),
            new IndexId(node.shardId.getIndexName(), randomUUID()),
            randomIdentifier(),
            IndexShardSnapshotStatus.newInitializing(ShardGeneration.newGeneration(), randomLongBetween(1, Long.MAX_VALUE)),
            IndexVersion.current(),
            randomNonNegativeLong(),
            null,
            Map.copyOf(firstLocations),
            (shardId, primaryTerm) -> node.objectStoreService.getProjectBlobContainer(shardId, primaryTerm),
            null,
            serverSideCopyEnabled,
            new PlainActionFuture<>()
        );
    }

    /**
     * The generational files that the commit carries, which are the generational files that first appeared in an older commit blob.
     */
    private static List<String> carriedGenerationalFiles(FakeStatelessNode node, UploadedCommit uploaded) throws IOException {
        return uploaded.commit()
            .getFileNames()
            .stream()
            .filter(
                file -> StatelessCompoundCommit.isGenerationalFile(file)
                    && node.commitService.getBlobLocation(node.shardId, file).compoundFileGeneration() != uploaded.blobGeneration()
            )
            .toList();
    }

    /**
     * Reads the header that the commit blob has for the commit, from the object store.
     */
    private static StatelessCompoundCommit readCommitHeader(FakeStatelessNode node, UploadedCommit uploaded) throws IOException {
        final var container = node.getShardContainer();
        final String blobName = BatchedCompoundCommit.blobNameFromGeneration(uploaded.blobGeneration());
        final var commitBlob = BatchedCompoundCommit.readFromStore(
            blobName,
            container.listBlobs(OperationPurpose.INDICES).get(blobName).length(),
            (name, offset, length) -> new InputStreamStreamInput(container.readBlob(OperationPurpose.INDICES, name, offset, length)),
            true
        );
        return commitBlob.compoundCommits()
            .stream()
            .filter(commit -> commit.generation() == uploaded.commit().getGeneration())
            .findFirst()
            .orElseThrow();
    }

    private record MetadataRead(String blobName, long position, long length) {}

    /**
     * Serves a read with the purpose SNAPSHOT_METADATA instead of the object store.
     */
    @FunctionalInterface
    private interface MetadataReadOverride {
        InputStream read(BlobContainer container, MetadataRead read) throws IOException;
    }

    /**
     * Records the reads with the purpose SNAPSHOT_METADATA in {@link #metadataReads}. These are the reads that a snapshot makes of the
     * object store to learn what the commits hold. If a test sets {@link #metadataReadOverride}, that serves the read instead.
     */
    private final class MetadataReadContainer extends FilterBlobContainer {
        private final BlobContainer container;

        MetadataReadContainer(BlobContainer container) {
            super(container);
            this.container = container;
        }

        @Override
        protected BlobContainer wrapChild(BlobContainer child) {
            return new MetadataReadContainer(child);
        }

        @Override
        public InputStream readBlob(OperationPurpose purpose, String blobName) throws IOException {
            if (purpose == OperationPurpose.SNAPSHOT_METADATA) {
                metadataReads.add(new MetadataRead(blobName, 0L, -1L));
            }
            return super.readBlob(purpose, blobName);
        }

        @Override
        public InputStream readBlob(OperationPurpose purpose, String blobName, long position, long length) throws IOException {
            if (purpose == OperationPurpose.SNAPSHOT_METADATA) {
                final var read = new MetadataRead(blobName, position, length);
                metadataReads.add(read);
                final var override = metadataReadOverride;
                if (override != null) {
                    return override.read(container, read);
                }
            }
            return super.readBlob(purpose, blobName, position, length);
        }
    }

    /**
     * The file info that a snapshot makes for a file when it copies the file from the given commit blob.
     */
    private static FileInfo copiedFileInfo(ShardId shardId, String file, BlobLocation blobLocation) throws IOException {
        final var source = copySource(shardId, blobLocation);
        final var metadata = new StoreFileMetadata(file, blobLocation.fileLength(), "checksum", Version.LATEST.toString());
        return new FileInfo("__" + randomUUID(), metadata, "c__" + randomUUID(), source.offset(), source.source());
    }

    private static StatelessSnapshotShardContext contextWithFile(
        ShardId shardId,
        String file,
        BlobLocation blobLocation,
        boolean serverSideCopyEnabled
    ) {
        // The segments_N file of the commit is in the same commit blob as the file, so the file is not carried, and no header is read.
        final var segmentsFile = new BlobLocation(
            blobLocation.blobFile(),
            blobLocation.offset() + blobLocation.fileLength(),
            between(1, 100)
        );
        return new StatelessSnapshotShardContext(
            shardId,
            new SnapshotId(randomIdentifier(), randomUUID()),
            new IndexId(shardId.getIndexName(), randomUUID()),
            randomIdentifier(),
            IndexShardSnapshotStatus.newInitializing(ShardGeneration.newGeneration(), randomLongBetween(1, Long.MAX_VALUE)),
            IndexVersion.current(),
            randomNonNegativeLong(),
            null,
            Map.of(file, blobLocation, "segments_1", segmentsFile),
            (s, g) -> mock(BlobContainer.class),
            null,
            serverSideCopyEnabled,
            new PlainActionFuture<>()
        );
    }

    private static BlobLocation commitBlobLocation(long primaryTerm, long generation) {
        return new BlobLocation(
            new BlobFile("stateless_commit_" + generation, new PrimaryTermAndGeneration(primaryTerm, generation)),
            between(0, 50),
            between(100, 1000)
        );
    }

    private static SnapshotShardContext.CopySource copySource(ShardId shardId, BlobLocation blobLocation) throws IOException {
        final var context = new StatelessSnapshotShardContext(
            shardId,
            new SnapshotId(randomIdentifier(), randomUUID()),
            new IndexId(shardId.getIndexName(), randomUUID()),
            randomIdentifier(),
            IndexShardSnapshotStatus.newInitializing(ShardGeneration.newGeneration(), randomLongBetween(1, Long.MAX_VALUE)),
            IndexVersion.current(),
            randomNonNegativeLong(),
            null,
            Map.of("file", blobLocation),
            (s, g) -> mock(BlobContainer.class),
            null,
            true,
            new PlainActionFuture<>()
        );
        return context.copySource("file");
    }

    public void testFileReaderThrowsWhenSnapshotAborted() throws IOException {
        final var commitRefReleased = new AtomicBoolean();
        final var snapshotIndexCommit = new SnapshotIndexCommit(new Engine.IndexCommitRef(null, () -> commitRefReleased.set(true)));
        final var snapshotStatus = IndexShardSnapshotStatus.newInitializing(ShardGeneration.newGeneration(), randomNonNegativeLong());
        snapshotStatus.addAbortListener(new ActionListener<>() {
            @Override
            public void onResponse(IndexShardSnapshotStatus.AbortStatus abortStatus) {
                assertThat(abortStatus, is(IndexShardSnapshotStatus.AbortStatus.ABORTED));
                snapshotIndexCommit.releaseInitialReference();
            }

            @Override
            public void onFailure(Exception e) {
                fail(e);
            }
        });
        snapshotStatus.abortIfNotCompleted("test abort", listener -> listener.onResponse(null));
        assertTrue(commitRefReleased.get());

        final var shardId = new ShardId(new Index(randomIndexName(), randomUUID()), between(0, 5));
        final long generation = 42L;
        final String blobName = "stateless_commit_" + generation;
        final var context = new StatelessSnapshotShardContext(
            shardId,
            new SnapshotId(randomSnapshotName(), randomUUID()),
            new IndexId(shardId.getIndexName(), randomUUID()),
            randomIdentifier(),
            snapshotStatus,
            IndexVersion.current(),
            randomNonNegativeLong(),
            snapshotIndexCommit,
            Map.of("file", new BlobLocation(new BlobFile(blobName, new PrimaryTermAndGeneration(1L, generation)), 0, 100)),
            (s, g) -> mock(BlobContainer.class),
            () -> false,
            new PlainActionFuture<>()
        );

        expectThrows(AbortedSnapshotException.class, () -> context.fileReader("file", mock(StoreFileMetadata.class)));
    }

    public void testVerify() throws IOException {
        final Directory dir = newDirectory();
        final IndexOutput output = dir.createOutput("foo.bar", IOContext.DEFAULT);
        final int iters = scaledRandomIntBetween(10, 100);
        long totalLength = 16; // header + footer
        for (int i = 0; i < iters; i++) {
            BytesRef bytesRef = new BytesRef(TestUtil.randomRealisticUnicodeString(random(), 10, 1024));
            output.writeBytes(bytesRef.bytes, bytesRef.offset, bytesRef.length);
            totalLength += bytesRef.length;
        }
        CodecUtil.writeFooter(output);
        output.close();

        final long generation = randomLongBetween(1, 42);
        final String blobName = "stateless_commit_" + generation;
        final long badGeneration = randomLongBetween(50, 100);
        final String badBlobName = "stateless_commit_" + badGeneration;
        final FsBlobContainer blobContainer = createBlobContainer();

        try (
            var indexInput = dir.openInput("foo.bar", IOContext.DEFAULT);
            var indexInputStream = new InputStreamIndexInput(indexInput, totalLength)
        ) {
            assertThat(indexInput.length(), equalTo(totalLength));
            blobContainer.writeBlob(randomFrom(OperationPurpose.values()), blobName, indexInputStream, totalLength, randomBoolean());
        } finally {
            IOUtils.close(dir);
        }

        // Write a bad blob with a single bit flipped
        try (var in = blobContainer.readBlob(randomFrom(OperationPurpose.values()), blobName, 0, totalLength)) {
            final var content = in.readAllBytes();
            final int brokenPosition = randomInt(content.length - 1);
            content[brokenPosition] = (byte) (content[brokenPosition] ^ (byte) (1 << between(0, 7)));
            blobContainer.writeBlob(
                randomFrom(OperationPurpose.values()),
                badBlobName,
                new ByteArrayInputStream(content),
                totalLength,
                randomBoolean()
            );
        }

        final var shardId = new ShardId(new Index(randomIdentifier(), randomUUID()), between(0, 5));
        final var snapshotShardContext = new StatelessSnapshotShardContext(
            shardId,
            new SnapshotId(randomIdentifier(), randomUUID()),
            new IndexId(shardId.getIndexName(), randomUUID()),
            randomIdentifier(),
            IndexShardSnapshotStatus.newInitializing(ShardGeneration.newGeneration(), randomLongBetween(1, Long.MAX_VALUE)),
            IndexVersion.current(),
            randomNonNegativeLong(),
            null,
            Map.of(
                "file",
                new BlobLocation(new BlobFile(blobName, new PrimaryTermAndGeneration(1L, generation)), 0, totalLength),
                "bad_file",
                new BlobLocation(new BlobFile(badBlobName, new PrimaryTermAndGeneration(1L, badGeneration)), 0, totalLength)
            ),
            (s, g) -> blobContainer,
            () -> false,
            new PlainActionFuture<>()
        );

        final var numberOfParts = between(1, 10);
        try (var fileReader = snapshotShardContext.fileReader("file", mock(StoreFileMetadata.class))) {
            readBlobContent(fileReader, numberOfParts, (int) totalLength, randomBoolean());
            fileReader.verify();
        }

        try (var fileReader = snapshotShardContext.fileReader("bad_file", mock(StoreFileMetadata.class))) {
            readBlobContent(fileReader, numberOfParts, (int) totalLength, randomBoolean());
            final var e = expectThrows(CorruptIndexException.class, fileReader::verify);
            assertThat(e.getMessage(), allOf(containsString("bad_file"), containsString(badBlobName)));
        }
    }

    public void testResetWithoutMark() throws IOException {
        final var setup = createMarkResetTestSetup();
        try (var fileReader = setup.context.fileReader("file", mock(StoreFileMetadata.class))) {
            final var stream = fileReader.openInput(setup.fileLength);
            assertTrue(stream.markSupported());

            final int firstReadSize = between(1, setup.fileLength - 1);
            byte[] firstBytes = stream.readNBytes(firstReadSize);
            assertArrayEquals(Arrays.copyOfRange(setup.expectedContent, 0, firstReadSize), firstBytes);

            stream.reset();

            byte[] allBytes = readMixedBytes(stream, setup.fileLength);
            assertArrayEquals(setup.expectedContent, allBytes);

            fileReader.verify();
        }

        for (CloseTrackingInputStream s : setup.allOpenedStreams) {
            assertTrue("Expected all opened input streams to be closed", s.isClosed());
        }
    }

    public void testMarkAndReset() throws IOException {
        final var setup = createMarkResetTestSetup();
        try (var fileReader = setup.context.fileReader("file", mock(StoreFileMetadata.class))) {
            final var stream = fileReader.openInput(setup.fileLength);

            final int markPosition = between(1, setup.fileLength - 2);
            stream.readNBytes(markPosition);
            // Randomize the readLimit argument since is ignored by our implementation, i.e. reset is always allowed regardless of
            // how many bytes have been read after mark.
            stream.mark(randomInt());

            final int extraRead = between(1, setup.fileLength - markPosition);
            stream.readNBytes(extraRead);

            stream.reset();

            byte[] remaining = readMixedBytes(stream, setup.fileLength - markPosition);
            assertArrayEquals(Arrays.copyOfRange(setup.expectedContent, markPosition, setup.fileLength), remaining);

            fileReader.verify();
        }

        for (CloseTrackingInputStream s : setup.allOpenedStreams) {
            assertTrue("Expected all opened input streams to be closed", s.isClosed());
        }
    }

    public void testMultipleResets() throws IOException {
        final var setup = createMarkResetTestSetup();
        try (var fileReader = setup.context.fileReader("file", mock(StoreFileMetadata.class))) {
            final var stream = fileReader.openInput(setup.fileLength);

            int markPos = 0;
            final int rounds = between(2, 10);
            for (int i = 0; i < rounds; i++) {
                final int remaining = setup.fileLength - markPos;
                if (remaining <= 1) {
                    break;
                }
                final int bytesToRead = between(1, remaining - 1);
                readMixedBytes(stream, bytesToRead);

                if (randomBoolean()) {
                    final int newMarkPos = between(markPos, markPos + bytesToRead);
                    stream.reset();
                    readMixedBytes(stream, newMarkPos - markPos);
                    // Randomize the readLimit argument since is ignored by our implementation, i.e. reset is always allowed regardless of
                    // how many bytes have been read after mark.
                    stream.mark(randomInt());
                    markPos = newMarkPos;
                } else {
                    stream.reset();
                }
            }

            byte[] content = readMixedBytes(stream, setup.fileLength - markPos);
            assertArrayEquals(Arrays.copyOfRange(setup.expectedContent, markPos, setup.fileLength), content);

            fileReader.verify();
        }

        for (CloseTrackingInputStream s : setup.allOpenedStreams) {
            assertTrue("Expected all opened input streams to be closed", s.isClosed());
        }
    }

    private record MarkResetTestSetup(
        byte[] expectedContent,
        int fileLength,
        StatelessSnapshotShardContext context,
        List<CloseTrackingInputStream> allOpenedStreams
    ) {}

    private MarkResetTestSetup createMarkResetTestSetup() throws IOException {
        final Directory dir = newDirectory();
        final IndexOutput output = dir.createOutput("test.file", IOContext.DEFAULT);
        final byte[] content = randomByteArrayOfLength(between(100, 1000));
        output.writeBytes(content, content.length);
        CodecUtil.writeFooter(output);
        output.close();

        final long totalLength;
        try (var indexInput = dir.openInput("test.file", IOContext.DEFAULT)) {
            totalLength = indexInput.length();
        }

        final long generation = 42L;
        final String blobName = "stateless_commit_" + generation;
        final int blobPadding = between(0, 50);
        final FsBlobContainer blobContainer = createBlobContainer();

        final byte[] expectedContent;
        try (
            var indexInput = dir.openInput("test.file", IOContext.DEFAULT);
            var indexInputStream = new InputStreamIndexInput(indexInput, totalLength)
        ) {
            final byte[] fileContent = indexInputStream.readAllBytes();
            // Add padding bytes randomly so that the file is sometimes in the middle of a blob
            final byte[] padding = randomByteArrayOfLength(blobPadding);
            final int blobTrailing = between(0, 50);
            final byte[] trailing = randomByteArrayOfLength(blobTrailing);
            final byte[] blobContent = new byte[blobPadding + fileContent.length + blobTrailing];
            System.arraycopy(padding, 0, blobContent, 0, blobPadding);
            System.arraycopy(fileContent, 0, blobContent, blobPadding, fileContent.length);
            System.arraycopy(trailing, 0, blobContent, blobPadding + fileContent.length, blobTrailing);
            blobContainer.writeBlob(
                OperationPurpose.SNAPSHOT_DATA,
                blobName,
                new ByteArrayInputStream(blobContent),
                blobContent.length,
                false
            );
            expectedContent = fileContent;
        } finally {
            IOUtils.close(dir);
        }

        final var shardId = new ShardId(new Index(randomIdentifier(), randomUUID()), 0);
        final List<CloseTrackingInputStream> allOpenedStreams = new ArrayList<>();
        final var context = new StatelessSnapshotShardContext(
            shardId,
            new SnapshotId(randomIdentifier(), randomUUID()),
            new IndexId(shardId.getIndexName(), randomUUID()),
            randomIdentifier(),
            IndexShardSnapshotStatus.newInitializing(ShardGeneration.newGeneration(), randomLongBetween(1, Long.MAX_VALUE)),
            IndexVersion.current(),
            randomNonNegativeLong(),
            null,
            Map.of(
                "file",
                new BlobLocation(new BlobFile(blobName, new PrimaryTermAndGeneration(1L, generation)), blobPadding, totalLength)
            ),
            (s, g) -> new FilterBlobContainer(blobContainer) {
                @Override
                protected BlobContainer wrapChild(BlobContainer child) {
                    throw new AssertionError("should not obtain child");
                }

                @Override
                public InputStream readBlob(OperationPurpose purpose, String blobName, long position, long length) throws IOException {
                    var inputStream = new CloseTrackingInputStream(super.readBlob(purpose, blobName, position, length), false);
                    allOpenedStreams.add(inputStream);
                    return inputStream;
                }
            },
            () -> false,
            new PlainActionFuture<>()
        );

        return new MarkResetTestSetup(expectedContent, (int) totalLength, context, allOpenedStreams);
    }

    private static byte[] readMixedBytes(InputStream stream, int count) throws IOException {
        final byte[] result = new byte[count];
        int pos = 0;
        while (pos < count) {
            if (randomBoolean()) {
                final int b = stream.read();
                assertNotEquals("unexpected end of stream at position " + pos, -1, b);
                result[pos++] = (byte) b;
            } else {
                final int len = between(1, count - pos);
                final int n = stream.read(result, pos, len);
                assertNotEquals("unexpected end of stream at position " + pos, -1, n);
                pos += n;
            }
        }
        return result;
    }

    private static byte[] readBlobContent(
        SnapshotShardContext.FileReader fileReader,
        int numberOfParts,
        int fileLength,
        boolean closeEachInputStream
    ) throws IOException {
        final var fileContentRead = new byte[fileLength];
        final var partLength = fileLength / numberOfParts;
        int bytesRead = 0;
        try {
            for (int i = 0; i < numberOfParts; i++) {
                var limit = partLength;
                if (i == numberOfParts - 1) {
                    limit = Math.max(limit, fileLength - bytesRead);
                }
                final var inputStream = fileReader.openInput(limit);
                inputStream.readNBytes(fileContentRead, bytesRead, limit);
                bytesRead += limit;
                if (closeEachInputStream) {
                    inputStream.close();
                }
            }
        } catch (AbortedSnapshotException e) {
            // Sometimes the read is aborted halfway through, return what have been read so far
            return Arrays.copyOfRange(fileContentRead, 0, bytesRead);
        }
        return fileContentRead;
    }

    private static FsBlobContainer createBlobContainer() throws IOException {
        final var blobStorePath = PathUtils.get(createTempDir().toString());
        final var blobContainer = new FsBlobContainer(
            new FsBlobStore(randomIntBetween(1, 8) * 1024, blobStorePath, false),
            BlobPath.EMPTY,
            blobStorePath
        );
        return blobContainer;
    }

    static class CloseTrackingInputStream extends FilterInputStream {
        private final boolean shouldAbort;
        private boolean closed = false;

        CloseTrackingInputStream(InputStream in, boolean shouldAbort) {
            super(in);
            this.shouldAbort = shouldAbort;
        }

        @Override
        public int read() throws IOException {
            if (shouldAbort) {
                throw new AbortedSnapshotException();
            }
            return super.read();
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (shouldAbort) {
                throw new AbortedSnapshotException();
            }
            return super.read(b, off, len);
        }

        @Override
        public void close() throws IOException {
            super.close();
            closed = true;
        }

        public boolean isClosed() {
            return closed;
        }
    }

}
