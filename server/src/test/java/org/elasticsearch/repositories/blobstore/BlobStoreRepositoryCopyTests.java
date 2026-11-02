/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.repositories.blobstore;

import org.apache.logging.log4j.Level;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.tests.util.TestUtil;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingHelper;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.fs.FsBlobContainer;
import org.elasticsearch.common.blobstore.fs.FsBlobStore;
import org.elasticsearch.common.blobstore.support.BlobMetadata;
import org.elasticsearch.common.blobstore.support.FilterBlobContainer;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.InternalEngineFactory;
import org.elasticsearch.index.seqno.RetentionLeaseSyncer;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.LocalPrimarySnapshotShardContext;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.repositories.ShardSnapshotResult;
import org.elasticsearch.repositories.SnapshotIndexCommit;
import org.elasticsearch.repositories.SnapshotShardContext;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.MockLog;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

/**
 * Tests the server-side copy path of shard snapshots. A plain FS container stands in for the stateless object store: the shard's data
 * files are written into a few commit blobs there, with filler around each file, and a snapshot copies those commit blobs instead of
 * uploading the files.
 */
public class BlobStoreRepositoryCopyTests extends IndexShardTestCase {

    public void testSnapshotCopiesCommitBlobsAndRestores() throws IOException {
        final var shardAndRepository = new ShardAndRepository();
        try {
            shardAndRepository.indexSegments(between(2, 4));
            final Map<String, SnapshotShardContext.CopySource> sources = shardAndRepository.writeCommitBlobs(
                between(1, 3),
                name -> true,
                0
            );
            final Snapshot snapshot = shardAndRepository.newSnapshot();
            final IndexShardSnapshotStatus status = IndexShardSnapshotStatus.newInitializing(null, randomLongBetween(1, Long.MAX_VALUE));
            // The copy stats log line counts each commit blob as one new copy, of the blob's whole length. Its dead bytes are the filler
            // that writeCommitBlobs puts before and between the files, because no file uses it.
            final Map<String, BlobMetadata> objectStoreBlobs = shardAndRepository.objectStore.listBlobs(OperationPurpose.SNAPSHOT_METADATA);
            final Map<String, Long> blobLengths = new HashMap<>();
            final Map<String, Long> fileBytes = new HashMap<>();
            for (Map.Entry<String, SnapshotShardContext.CopySource> entry : sources.entrySet()) {
                final String blobName = entry.getValue().blobName();
                blobLengths.put(blobName, objectStoreBlobs.get(blobName).length());
                fileBytes.merge(blobName, shardAndRepository.fileLength(entry.getKey()), Long::sum);
            }
            final List<Long> deadBytes = blobLengths.keySet()
                .stream()
                .map(blobName -> blobLengths.get(blobName) - fileBytes.get(blobName))
                .sorted(Comparator.reverseOrder())
                .toList();
            assertCopyStatsLogged(
                () -> shardAndRepository.snapshot(snapshot, status, sources::get, name -> true),
                shardAndRepository.copyStatsPrefix(snapshot)
                    + ": new copies ["
                    + blobLengths.size()
                    + "], copied bytes ["
                    + blobLengths.values().stream().mapToLong(Long::longValue).sum()
                    + "], total copy time [*ms], slowest copy [*ms], files in new copies ["
                    + sources.size()
                    + "], reused copies [0], files in reused copies [0], files reused by name, uploaded [0],"
                    + " bytes reused by name, uploaded [0], files reused by name, copied [0], bytes reused by name, copied [0],"
                    + " uploaded files [0], uploaded bytes [0], dead bytes ["
                    + deadBytes.stream().mapToLong(Long::longValue).sum()
                    + "], dead bytes per new copy "
                    + deadBytes
            );

            // every data file is a range of a copy of the commit blob that holds it, at the same offset
            final BlobStoreIndexShardSnapshot shardSnapshot = shardAndRepository.loadShardSnapshot(snapshot);
            final Map<String, String> copyBySource = new HashMap<>();
            for (FileInfo fileInfo : shardSnapshot.indexFiles()) {
                if (fileInfo.metadata().hashEqualsContents()) {
                    assertFalse(fileInfo.isCopied());
                    continue;
                }
                assertTrue(fileInfo + " is not copied", fileInfo.isCopied());
                final SnapshotShardContext.CopySource source = sources.get(fileInfo.physicalName());
                assertThat(fileInfo.source(), equalTo(source.source()));
                assertThat(fileInfo.offset(), equalTo(source.offset()));
                assertThat(copyBySource.computeIfAbsent(fileInfo.source(), ignored -> fileInfo.blob()), equalTo(fileInfo.blob()));
            }

            // each commit blob was copied once, whole, and no file was uploaded
            final Set<String> shardBlobs = shardAndRepository.shardContainer().listBlobs(OperationPurpose.SNAPSHOT_METADATA).keySet();
            assertThat(copiedBlobs(shardBlobs), equalTo(Set.copyOf(copyBySource.values())));
            assertThat(
                shardBlobs.stream().filter(name -> name.startsWith(BlobStoreRepository.UPLOADED_DATA_BLOB_PREFIX)).toList(),
                empty()
            );
            for (Map.Entry<String, String> entry : copyBySource.entrySet()) {
                final SnapshotShardContext.CopySource source = sources.values()
                    .stream()
                    .filter(s -> s.source().equals(entry.getKey()))
                    .findFirst()
                    .orElseThrow();
                assertThat(
                    readAll(shardAndRepository.shardContainer(), entry.getValue()),
                    equalTo(readAll(source.container(), source.blobName()))
                );
            }
            assertThat(status.asCopy().getProcessedFileCount(), equalTo(status.asCopy().getIncrementalFileCount()));

            shardAndRepository.restoreAndCheck(snapshot);
        } finally {
            shardAndRepository.close();
        }
    }

    public void testLaterSnapshotReusesCopy() throws IOException {
        final var shardAndRepository = new ShardAndRepository();
        try {
            shardAndRepository.indexSegments(between(2, 4));
            final Map<String, SnapshotShardContext.CopySource> sources = shardAndRepository.writeCommitBlobs(1, name -> true, 0);
            // The first snapshot does not see one data file of the commit blob, like a snapshot of an earlier commit in the same blob.
            final String hidden = randomFrom(sources.keySet());
            final Snapshot first = shardAndRepository.newSnapshot();
            final ShardGeneration firstGeneration = shardAndRepository.snapshot(
                first,
                IndexShardSnapshotStatus.newInitializing(null, randomLongBetween(1, Long.MAX_VALUE)),
                sources::get,
                name -> name.equals(hidden) == false
            );
            final Set<String> copiesAfterFirst = copiedBlobs(shardAndRepository.shardContainer());
            assertThat(copiesAfterFirst.size(), equalTo(1));

            // the second snapshot sees that file for the first time, and points it at the existing copy instead of copying again
            final Snapshot second = shardAndRepository.newSnapshot();
            final IndexShardSnapshotStatus status = IndexShardSnapshotStatus.newInitializing(
                firstGeneration,
                randomLongBetween(1, Long.MAX_VALUE)
            );
            // The other data files of the commit are in the first snapshot, so the second snapshot reuses them by their names.
            final List<FileInfo> firstFiles = shardAndRepository.loadShardSnapshot(first).indexFiles();
            final List<FileInfo> reusedByName = firstFiles.stream().filter(FileInfo::isCopied).toList();
            assertThat(reusedByName.size(), equalTo(sources.size() - 1));
            assertCopyStatsLogged(
                () -> shardAndRepository.snapshot(second, status, sources::get, name -> true),
                shardAndRepository.copyStatsPrefix(second)
                    + ": new copies [0], copied bytes [0], total copy time [0ms], slowest copy [0ms], files in new copies [0],"
                    + " reused copies [1], files in reused copies [1], files reused by name, uploaded [0],"
                    + " bytes reused by name, uploaded [0], files reused by name, copied ["
                    + reusedByName.size()
                    + "], bytes reused by name, copied ["
                    + reusedByName.stream().mapToLong(FileInfo::length).sum()
                    + "], uploaded files [0], uploaded bytes [0], dead bytes [0], dead bytes per new copy []"
            );
            assertThat(copiedBlobs(shardAndRepository.shardContainer()), equalTo(copiesAfterFirst));
            final FileInfo hiddenFileInfo = shardAndRepository.loadShardSnapshot(second)
                .indexFiles()
                .stream()
                .filter(fileInfo -> fileInfo.physicalName().equals(hidden))
                .findFirst()
                .orElseThrow();
            assertThat(hiddenFileInfo.blob(), equalTo(copiesAfterFirst.iterator().next()));
            assertThat(status.asCopy().getIncrementalFileCount(), equalTo(1));
            assertThat(status.asCopy().getProcessedFileCount(), equalTo(1));

            shardAndRepository.restoreAndCheck(second);
        } finally {
            shardAndRepository.close();
        }
    }

    public void testFailedCopyKeepsReusedCopy() throws IOException {
        final var shardAndRepository = new ShardAndRepository();
        try {
            shardAndRepository.indexSegments(between(1, 3));
            final Map<String, SnapshotShardContext.CopySource> sources = new HashMap<>(
                shardAndRepository.writeCommitBlobs(1, name -> true, 0)
            );
            final String hidden = randomFrom(sources.keySet());
            final ShardGeneration firstGeneration = shardAndRepository.snapshot(
                shardAndRepository.newSnapshot(),
                IndexShardSnapshotStatus.newInitializing(null, randomLongBetween(1, Long.MAX_VALUE)),
                sources::get,
                name -> name.equals(hidden) == false
            );
            final Set<String> copiesAfterFirst = copiedBlobs(shardAndRepository.shardContainer());
            final String reusedCopy = copiesAfterFirst.iterator().next();
            final long reusedCopyLength = shardAndRepository.shardContainer()
                .listBlobs(OperationPurpose.SNAPSHOT_METADATA)
                .get(reusedCopy)
                .length();

            // The second snapshot reuses that copy for the hidden file and copies a second commit blob for its new files. One of its copies
            // fails inside the copy task: the listing shows a commit blob that is not there, so the length check passes and the copy fails.
            shardAndRepository.indexSegments(1);
            final Set<String> known = Set.copyOf(sources.keySet());
            final Map<String, SnapshotShardContext.CopySource> newSources = shardAndRepository.writeCommitBlobs(
                1,
                name -> known.contains(name) == false,
                0
            );
            final String missingBlob = "stateless_commit_missing";
            final BlobContainer listingWithMissingBlob = new ListingWithMissingBlob(shardAndRepository.objectStore, missingBlob);
            final String failingFile = randomFrom(newSources.keySet());
            for (Map.Entry<String, SnapshotShardContext.CopySource> entry : newSources.entrySet()) {
                final SnapshotShardContext.CopySource source = entry.getValue();
                sources.put(
                    entry.getKey(),
                    entry.getKey().equals(failingFile)
                        ? new SnapshotShardContext.CopySource(source.source() + "_missing", listingWithMissingBlob, missingBlob, 0)
                        : new SnapshotShardContext.CopySource(source.source(), listingWithMissingBlob, source.blobName(), source.offset())
                );
            }
            expectThrows(
                Exception.class,
                () -> shardAndRepository.snapshot(
                    shardAndRepository.newSnapshot(),
                    IndexShardSnapshotStatus.newInitializing(firstGeneration, randomLongBetween(1, Long.MAX_VALUE)),
                    sources::get,
                    name -> true
                )
            );

            // the failure cleanup deleted the copies the second snapshot made, but not the copy that the first snapshot refers to
            assertThat(copiedBlobs(shardAndRepository.shardContainer()), equalTo(copiesAfterFirst));
            assertThat(
                shardAndRepository.shardContainer().listBlobs(OperationPurpose.SNAPSHOT_METADATA).get(reusedCopy).length(),
                equalTo(reusedCopyLength)
            );
        } finally {
            shardAndRepository.close();
        }
    }

    public void testSnapshotWithoutCopiesLogsNoCopyStats() throws IOException {
        final var shardAndRepository = new ShardAndRepository();
        try {
            shardAndRepository.indexSegments(between(1, 3));
            // the context offers no copy source for any file, so the snapshot uploads every file whose contents are not stored in the
            // shard metadata, and copies nothing
            final Snapshot snapshot = shardAndRepository.newSnapshot();
            MockLog.assertThatLogger(
                () -> runUnchecked(() -> shardAndRepository.snapshot(snapshot, name -> null)),
                BlobStoreRepository.class,
                new MockLog.UnseenEventExpectation(
                    "no copy stats",
                    BlobStoreRepository.class.getCanonicalName(),
                    Level.INFO,
                    "*shard snapshot copy stats*"
                )
            );
            assertThat(copiedBlobs(shardAndRepository.shardContainer()), empty());
            shardAndRepository.restoreAndCheck(snapshot);
        } finally {
            shardAndRepository.close();
        }
    }

    public void testCommitBlobTooShortFailsBeforeAnyCopy() throws IOException {
        final var shardAndRepository = new ShardAndRepository();
        try {
            shardAndRepository.indexSegments(between(1, 3));
            // Each commit blob loses its last bytes, so the range of the last file written into it runs past its end.
            final Map<String, SnapshotShardContext.CopySource> sources = shardAndRepository.writeCommitBlobs(
                between(1, 3),
                name -> true,
                between(1, 16)
            );
            final Snapshot snapshot = shardAndRepository.newSnapshot();
            final Exception e = expectThrows(Exception.class, () -> shardAndRepository.snapshot(snapshot, sources::get));
            assertThat(e.getMessage(), containsString("does not fit in commit blob"));
            shardAndRepository.assertFailedSnapshotLeftNoData(snapshot);
        } finally {
            shardAndRepository.close();
        }
    }

    public void testMissingCommitBlobFailsBeforeAnyCopy() throws IOException {
        final var shardAndRepository = new ShardAndRepository();
        try {
            shardAndRepository.indexSegments(between(1, 3));
            final Map<String, SnapshotShardContext.CopySource> sources = shardAndRepository.writeCommitBlobs(
                between(1, 3),
                name -> true,
                0
            );
            // One of the commit blobs is gone from the object store, so the listing taken before any copy does not show it.
            final String missingBlob = randomFrom(sources.values()).blobName();
            shardAndRepository.objectStore.deleteBlobsIgnoringIfNotExists(OperationPurpose.SNAPSHOT_DATA, Iterators.single(missingBlob));
            final Snapshot snapshot = shardAndRepository.newSnapshot();
            final Exception e = expectThrows(Exception.class, () -> shardAndRepository.snapshot(snapshot, sources::get));
            assertThat(e.getMessage(), containsString("to copy is missing"));
            shardAndRepository.assertFailedSnapshotLeftNoData(snapshot);
        } finally {
            shardAndRepository.close();
        }
    }

    public void testBrokenCopyFailsBeforeShardSnapshotIsWritten() throws IOException {
        // The repository's copy call returns as if it worked, but the copy is then missing, or one byte short.
        final boolean dropCopy = randomBoolean();
        final var shardAndRepository = new ShardAndRepository(container -> new BrokenCopies(container, dropCopy));
        try {
            shardAndRepository.indexSegments(between(1, 3));
            final Map<String, SnapshotShardContext.CopySource> sources = shardAndRepository.writeCommitBlobs(
                between(1, 3),
                name -> true,
                0
            );
            final Snapshot snapshot = shardAndRepository.newSnapshot();
            final Exception e = expectThrows(Exception.class, () -> shardAndRepository.snapshot(snapshot, sources::get));
            assertThat(e.getMessage(), containsString("but expected"));
            assertThat(e.getMessage(), e.getMessage().contains("none, it is missing"), equalTo(dropCopy));
            // the check of the copies failed before snap-<uuid>.dat was written, and the failure cleanup deleted any copy still in the
            // shard's folder
            shardAndRepository.assertFailedSnapshotLeftNoData(snapshot);
        } finally {
            shardAndRepository.close();
        }
    }

    /**
     * Takes the snapshot and checks that it logs at least one copy stats line that matches {@code message}. A {@code *} in
     * {@code message} matches any text. Without a {@code *}, {@code message} only has to appear somewhere in the line.
     */
    private static void assertCopyStatsLogged(CheckedRunnable<IOException> snapshot, String message) {
        MockLog.assertThatLogger(
            () -> runUnchecked(snapshot),
            BlobStoreRepository.class,
            new MockLog.SeenEventExpectation("copy stats", BlobStoreRepository.class.getCanonicalName(), Level.INFO, message)
        );
    }

    private static void runUnchecked(CheckedRunnable<IOException> action) {
        try {
            action.run();
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private static Set<String> copiedBlobs(BlobContainer shardContainer) throws IOException {
        return copiedBlobs(shardContainer.listBlobs(OperationPurpose.SNAPSHOT_METADATA).keySet());
    }

    private static Set<String> copiedBlobs(Set<String> blobNames) {
        return blobNames.stream().filter(name -> name.startsWith(BlobStoreRepository.COPIED_DATA_BLOB_PREFIX)).collect(Collectors.toSet());
    }

    private static byte[] readAll(BlobContainer container, String blobName) throws IOException {
        try (InputStream in = container.readBlob(OperationPurpose.SNAPSHOT_DATA, blobName)) {
            return in.readAllBytes();
        }
    }

    /**
     * A shard, a repository to snapshot it into, and a plain FS container that stands in for the stateless object store.
     */
    private final class ShardAndRepository {
        private final IndexId indexId = new IndexId(randomAlphaOfLength(10), UUIDs.randomBase64UUID());
        private final ShardId shardId = new ShardId(indexId.getName(), indexId.getId(), 0);
        @FixForMultiProject(description = "randomize when snapshot and restore support multiple projects, see also ES-10225, ES-10228")
        private final BlobStoreRepository repository;
        private final BlobContainer objectStore;
        private IndexShard shard;
        private int commitBlobCount;

        ShardAndRepository() throws IOException {
            this(null);
        }

        /**
         * @param shardContainers if not null, wraps the repository's shard containers, see {@link TestUtils#createRepository}
         */
        ShardAndRepository(@Nullable UnaryOperator<BlobContainer> shardContainers) throws IOException {
            repository = (BlobStoreRepository) TestUtils.createRepository(
                ProjectId.DEFAULT,
                createTempDir(),
                xContentRegistry(),
                shardContainers
            );
            final Path objectStorePath = createTempDir();
            BlobContainer container = new FsBlobContainer(
                new FsBlobStore(randomIntBetween(1, 8) * 1024, objectStorePath, false),
                BlobPath.EMPTY,
                objectStorePath
            );
            // the object store's containers may be wrapped, and not the way the repository's shard containers are, so the copy must
            // unwrap its source
            for (int i = between(0, 2); i > 0; i--) {
                container = new WrappedContainer(container);
            }
            objectStore = container;
            shard = newShard(shardId, true);
            recoverShardFromStore(shard);
        }

        BlobContainer shardContainer() {
            return repository.shardContainer(indexId, shardId.id());
        }

        Snapshot newSnapshot() {
            return new Snapshot(repository.getMetadata().name(), new SnapshotId(randomAlphaOfLength(10), UUIDs.randomBase64UUID()));
        }

        BlobStoreIndexShardSnapshot loadShardSnapshot(Snapshot snapshot) {
            return repository.loadShardSnapshot(shardContainer(), snapshot.getSnapshotId());
        }

        /**
         * The start of the copy stats line that a shard snapshot of this shard logs, up to but not including the colon before the
         * numbers.
         */
        String copyStatsPrefix(Snapshot snapshot) {
            return "[" + repository.getMetadata().name() + "][" + snapshot.getSnapshotId() + "]" + shardId + " shard snapshot copy stats";
        }

        long fileLength(String file) throws IOException {
            return shard.store().directory().fileLength(file);
        }

        void indexSegments(int segments) throws IOException {
            for (int segment = 0; segment < segments; segment++) {
                for (int i = between(1, 50); i > 0; i--) {
                    indexDoc(shard, "_doc", UUIDs.randomBase64UUID(random()));
                }
                flushShard(shard, true);
            }
        }

        /**
         * Writes the chosen data files of the shard's last commit into {@code blobCount} commit blobs in the object store, with random
         * filler before and between the files, and returns where each file can be copied from. {@code truncateBy} cuts that many bytes
         * off the end of each blob, to make its last file's range too long for it.
         */
        Map<String, SnapshotShardContext.CopySource> writeCommitBlobs(int blobCount, Predicate<String> files, int truncateBy)
            throws IOException {
            final Map<String, SnapshotShardContext.CopySource> sources = new HashMap<>();
            try (Engine.IndexCommitRef commitRef = shard.acquireLastIndexCommit(true)) {
                final List<StoreFileMetadata> dataFiles = new ArrayList<>(
                    shard.store()
                        .getMetadata(commitRef.getIndexCommit())
                        .fileMetadataMap()
                        .values()
                        .stream()
                        .filter(metadata -> metadata.hashEqualsContents() == false && files.test(metadata.name()))
                        .toList()
                );
                assertFalse("no data files to write", dataFiles.isEmpty());
                Collections.shuffle(dataFiles, random());
                final int blobs = Math.min(blobCount, dataFiles.size());
                for (int blob = 0; blob < blobs; blob++) {
                    final String blobName = "stateless_commit_" + (++commitBlobCount);
                    final String source = "indices/" + indexId.getId() + "/" + shardId.id() + "/1/" + blobName;
                    try (BytesStreamOutput blobBytes = new BytesStreamOutput()) {
                        for (int i = blob; i < dataFiles.size(); i += blobs) {
                            blobBytes.writeBytes(randomByteArrayOfLength(between(0, 512)));
                            final StoreFileMetadata dataFile = dataFiles.get(i);
                            sources.put(
                                dataFile.name(),
                                new SnapshotShardContext.CopySource(source, objectStore, blobName, blobBytes.position())
                            );
                            try (IndexInput input = shard.store().directory().openInput(dataFile.name(), IOContext.READONCE)) {
                                final byte[] bytes = new byte[Math.toIntExact(input.length())];
                                input.readBytes(bytes, 0, bytes.length);
                                blobBytes.writeBytes(bytes);
                            }
                        }
                        final BytesReference bytes = blobBytes.bytes();
                        objectStore.writeBlob(OperationPurpose.SNAPSHOT_DATA, blobName, bytes.slice(0, bytes.length() - truncateBy), true);
                    }
                }
            }
            return sources;
        }

        /**
         * Takes a first shard snapshot of all the shard's files through a context that offers the given copy sources.
         */
        ShardGeneration snapshot(Snapshot snapshot, Function<String, SnapshotShardContext.CopySource> copySources) throws IOException {
            return snapshot(
                snapshot,
                IndexShardSnapshotStatus.newInitializing(null, randomLongBetween(1, Long.MAX_VALUE)),
                copySources,
                name -> true
            );
        }

        /**
         * Checks that a shard snapshot that failed left no copy, no uploaded file and no {@code snap-<uuid>.dat} blob in the shard's
         * folder. The {@code index-<uuid>} blob that a shard snapshot writes first can stay, because no generation refers to it.
         */
        void assertFailedSnapshotLeftNoData(Snapshot snapshot) throws IOException {
            final Set<String> blobs = shardContainer().listBlobs(OperationPurpose.SNAPSHOT_METADATA).keySet();
            assertThat(copiedBlobs(blobs), empty());
            assertThat(blobs.stream().filter(name -> name.startsWith(BlobStoreRepository.UPLOADED_DATA_BLOB_PREFIX)).toList(), empty());
            assertFalse(blobs.contains(BlobStoreRepository.INDEX_SHARD_SNAPSHOT_FORMAT.blobName(snapshot.getSnapshotId().getUUID())));
        }

        /**
         * Takes a shard snapshot through a context that offers the given copy sources and shows only the visible files.
         */
        ShardGeneration snapshot(
            Snapshot snapshot,
            IndexShardSnapshotStatus status,
            Function<String, SnapshotShardContext.CopySource> copySources,
            Predicate<String> visibleFiles
        ) throws IOException {
            final PlainActionFuture<ShardSnapshotResult> future = new PlainActionFuture<>();
            try (Engine.IndexCommitRef commitRef = shard.acquireLastIndexCommit(true)) {
                final var localContext = new LocalPrimarySnapshotShardContext(
                    shard.store(),
                    shard.mapperService(),
                    snapshot.getSnapshotId(),
                    indexId,
                    new SnapshotIndexCommit(commitRef),
                    null,
                    status,
                    IndexVersion.current(),
                    randomMillisUpToYear9999(),
                    future
                );
                repository.snapshotShard(new CopyingSnapshotShardContext(localContext, copySources, visibleFiles));
                return future.actionGet().getGeneration();
            }
        }

        /**
         * Restores the snapshot into an empty store and checks that every file of the shard came back unchanged.
         */
        void restoreAndCheck(Snapshot snapshot) throws IOException {
            final Store.MetadataSnapshot storeFiles = shard.snapshotStoreMetadata();
            closeShards(shard);
            try (var files = Files.list(shard.shardPath().resolveIndex())) {
                for (Path file : files.toList()) {
                    if (file.getFileName().toString().equals("write.lock") == false) {
                        Files.delete(file);
                    }
                }
            }
            final ShardRouting shardRouting = ShardRoutingHelper.initWithSameId(
                shard.routingEntry(),
                new RecoverySource.SnapshotRecoverySource(UUIDs.randomBase64UUID(), snapshot, IndexVersion.current(), indexId)
            );
            shard = newShard(
                shardRouting,
                null,
                shard.shardPath(),
                shard.indexSettings().getIndexMetadata(),
                null,
                null,
                new InternalEngineFactory(),
                NOOP_GCP_SYNCER,
                RetentionLeaseSyncer.EMPTY,
                IndexEventListener.NOOP
            );
            recoverShardFromSnapshot(shard, snapshot, repository);
            TestUtil.checkIndex(shard.store().directory());
            final Store.MetadataSnapshot restoredFiles = shard.store().getMetadata(null);
            for (StoreFileMetadata storeFile : storeFiles) {
                final StoreFileMetadata restoredFile = restoredFiles.get(storeFile.name());
                assertNotNull("file [" + storeFile.name() + "] was not restored", restoredFile);
                assertTrue("file [" + storeFile.name() + "] differs after restore", restoredFile.isSame(storeFile));
            }
        }

        void close() throws IOException {
            if (shard != null && shard.state() != IndexShardState.CLOSED) {
                try {
                    closeShardNoCheck(shard);
                } finally {
                    IOUtils.close(shard.store());
                }
            }
        }
    }

    /**
     * A context that offers copy sources for a shard's files, and can hide some files, on top of the context for a local primary.
     */
    private static final class CopyingSnapshotShardContext extends SnapshotShardContext {
        private final LocalPrimarySnapshotShardContext localContext;
        private final Function<String, CopySource> copySources;
        private final Predicate<String> visibleFiles;

        CopyingSnapshotShardContext(
            LocalPrimarySnapshotShardContext localContext,
            Function<String, CopySource> copySources,
            Predicate<String> visibleFiles
        ) {
            super(
                localContext.snapshotId(),
                localContext.indexId(),
                localContext.stateIdentifier(),
                localContext.status(),
                localContext.getRepositoryMetaVersion(),
                localContext.snapshotStartTime(),
                localContext
            );
            this.localContext = localContext;
            this.copySources = copySources;
            this.visibleFiles = visibleFiles;
        }

        @Override
        public ShardId shardId() {
            return localContext.shardId();
        }

        @Override
        public boolean isSearchableSnapshot() {
            return false;
        }

        @Override
        public Store.MetadataSnapshot metadataSnapshot() {
            return localContext.metadataSnapshot();
        }

        @Override
        public Collection<String> fileNames() {
            return localContext.fileNames().stream().filter(visibleFiles).toList();
        }

        @Override
        public boolean assertFileContentsMatchHash(FileInfo fileInfo) {
            return localContext.assertFileContentsMatchHash(fileInfo);
        }

        @Override
        public void failStoreIfCorrupted(Exception e) {
            localContext.failStoreIfCorrupted(e);
        }

        @Override
        public FileReader fileReader(String file, StoreFileMetadata metadata) throws IOException {
            return localContext.fileReader(file, metadata);
        }

        @Override
        @Nullable
        public CopySource copySource(String file) {
            return copySources.apply(file);
        }
    }

    /**
     * A container whose listing also shows one large blob that does not exist. A snapshot that copies that blob passes its length check,
     * then fails when it copies the blob, inside the copy task.
     */
    private static final class ListingWithMissingBlob extends FilterBlobContainer {
        private final String missingBlob;

        ListingWithMissingBlob(BlobContainer delegate, String missingBlob) {
            super(delegate);
            this.missingBlob = missingBlob;
        }

        @Override
        public Map<String, BlobMetadata> listBlobs(OperationPurpose purpose) throws IOException {
            final Map<String, BlobMetadata> blobs = new HashMap<>(super.listBlobs(purpose));
            blobs.put(missingBlob, new BlobMetadata(missingBlob, 1L << 30));
            return blobs;
        }

        @Override
        protected BlobContainer wrapChild(BlobContainer child) {
            return new ListingWithMissingBlob(child, missingBlob);
        }
    }

    /**
     * A shard container whose copies report success without leaving a complete blob behind: right after each copy, the copy is deleted or
     * cut one byte short. The shard snapshot's check of its copies must catch that before it writes {@code snap-<uuid>.dat}.
     */
    private static final class BrokenCopies extends FilterBlobContainer {
        private final boolean dropCopy;

        BrokenCopies(BlobContainer delegate, boolean dropCopy) {
            super(delegate);
            this.dropCopy = dropCopy;
        }

        @Override
        public void copyBlob(
            OperationPurpose purpose,
            BlobContainer sourceBlobContainer,
            String sourceBlobName,
            String blobName,
            long blobSize,
            @Nullable Executor executor
        ) throws IOException {
            super.copyBlob(purpose, sourceBlobContainer, sourceBlobName, blobName, blobSize, executor);
            if (dropCopy) {
                deleteBlobsIgnoringIfNotExists(purpose, Iterators.single(blobName));
            } else {
                final byte[] copy = readAll(this, blobName);
                writeBlob(purpose, blobName, new BytesArray(copy, 0, copy.length - 1), false);
            }
        }

        @Override
        protected BlobContainer wrapChild(BlobContainer child) {
            return new BrokenCopies(child, dropCopy);
        }
    }

    private static final class WrappedContainer extends FilterBlobContainer {
        WrappedContainer(BlobContainer delegate) {
            super(delegate);
        }

        @Override
        protected BlobContainer wrapChild(BlobContainer child) {
            return new WrappedContainer(child);
        }
    }
}
