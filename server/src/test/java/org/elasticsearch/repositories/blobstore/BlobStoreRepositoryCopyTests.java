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
import org.apache.logging.log4j.core.LogEvent;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.tests.util.TestUtil;
import org.elasticsearch.ExceptionsHelper;
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
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
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
import org.elasticsearch.repositories.ShardGenerations;
import org.elasticsearch.repositories.ShardSnapshotResult;
import org.elasticsearch.repositories.SnapshotIndexCommit;
import org.elasticsearch.repositories.SnapshotShardContext;
import org.elasticsearch.snapshots.PausedSnapshotException;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotsService;
import org.elasticsearch.snapshots.SnapshotsServiceUtils;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.test.junit.annotations.TestLogging;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

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

    /**
     * Each copy of a commit blob is logged at debug with the name of the copy, its size and the time that the copy call took. A run can
     * then show which copies were slow.
     */
    @TestLogging(reason = "the copies are logged at debug", value = "org.elasticsearch.repositories.blobstore.BlobStoreRepository:DEBUG")
    public void testEachCopyIsLoggedAtDebug() throws IOException {
        final var shardAndRepository = new ShardAndRepository();
        try {
            shardAndRepository.indexSegments(between(2, 4));
            final Map<String, SnapshotShardContext.CopySource> sources = shardAndRepository.writeCommitBlobs(
                between(1, 3),
                name -> true,
                0
            );
            final Snapshot snapshot = shardAndRepository.newSnapshot();
            final Map<String, BlobMetadata> objectStoreBlobs = shardAndRepository.objectStore.listBlobs(OperationPurpose.SNAPSHOT_METADATA);
            final Map<String, String> blobNameBySource = new HashMap<>();
            for (SnapshotShardContext.CopySource source : sources.values()) {
                blobNameBySource.put(source.source(), source.blobName());
            }
            // one line for each commit blob, with the length of the commit blob, which the copy has as well
            final List<MockLog.LoggingExpectation> expectations = new ArrayList<>();
            for (Map.Entry<String, String> entry : blobNameBySource.entrySet()) {
                expectations.add(
                    new MockLog.SeenEventExpectation(
                        "copy of " + entry.getKey(),
                        BlobStoreRepository.class.getCanonicalName(),
                        Level.DEBUG,
                        shardAndRepository.logPrefix(snapshot)
                            + " copied blob [c__*] from ["
                            + entry.getKey()
                            + "]: ["
                            + objectStoreBlobs.get(entry.getValue()).length()
                            + "] bytes in [*ms]"
                    )
                );
            }
            MockLog.assertThatLogger(
                () -> runUnchecked(() -> shardAndRepository.snapshot(snapshot, sources::get)),
                BlobStoreRepository.class,
                expectations.toArray(MockLog.LoggingExpectation[]::new)
            );
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

    /**
     * A later snapshot that needs a file from a commit blob that an earlier snapshot copied points the file at that copy, under the name
     * that the copy already has, which starts with the shard generation of the earlier snapshot. A commit blob that the later snapshot
     * copies for the first time gets a name that starts with the shard generation of the later snapshot.
     */
    public void testReusedCopyKeepsItsNameAndNewCopyGetsTheNewShardGeneration() throws IOException {
        final var shardAndRepository = new ShardAndRepository();
        try {
            shardAndRepository.indexSegments(between(2, 4));
            final Map<String, SnapshotShardContext.CopySource> sources = new HashMap<>(
                shardAndRepository.writeCommitBlobs(1, name -> true, 0)
            );
            // The first snapshot does not see one data file of the commit blob, like a snapshot of an earlier commit in the same blob.
            final String hidden = randomFrom(sources.keySet());
            final ShardGeneration firstGeneration = shardAndRepository.snapshot(
                shardAndRepository.newSnapshot(),
                IndexShardSnapshotStatus.newInitializing(null, randomLongBetween(1, Long.MAX_VALUE)),
                sources::get,
                name -> name.equals(hidden) == false
            );
            final Set<String> firstCopies = copyNames(firstGeneration, sources.values());
            assertThat(copiedBlobs(shardAndRepository.shardContainer()), equalTo(firstCopies));

            // One more segment adds new data files. They are in a second commit blob, which the second snapshot copies.
            shardAndRepository.indexSegments(1);
            final Set<String> known = Set.copyOf(sources.keySet());
            final Map<String, SnapshotShardContext.CopySource> newSources = shardAndRepository.writeCommitBlobs(
                1,
                name -> known.contains(name) == false,
                0
            );
            sources.putAll(newSources);
            final Snapshot second = shardAndRepository.newSnapshot();
            final ShardGeneration secondGeneration = shardAndRepository.snapshot(
                second,
                IndexShardSnapshotStatus.newInitializing(firstGeneration, randomLongBetween(1, Long.MAX_VALUE)),
                sources::get,
                name -> true
            );

            // The second snapshot made one new copy, and left the copy of the first snapshot as it was.
            final Set<String> secondCopies = copyNames(secondGeneration, newSources.values());
            assertThat(secondGeneration, not(equalTo(firstGeneration)));
            assertThat(copiedBlobs(shardAndRepository.shardContainer()), equalTo(Sets.union(firstCopies, secondCopies)));
            final String firstCopy = firstCopies.iterator().next();
            final String secondCopy = secondCopies.iterator().next();
            final List<FileInfo> files = shardAndRepository.loadShardSnapshot(second).indexFiles();
            for (FileInfo fileInfo : files) {
                if (fileInfo.isCopied()) {
                    assertThat(
                        fileInfo.toString(),
                        fileInfo.blob(),
                        equalTo(newSources.containsKey(fileInfo.physicalName()) ? secondCopy : firstCopy)
                    );
                }
            }
            // the file that the first snapshot did not see points at the copy of the first snapshot, under the name of that copy
            assertThat(
                files.stream().filter(fileInfo -> fileInfo.physicalName().equals(hidden)).findFirst().orElseThrow().blob(),
                equalTo(firstCopy)
            );

            shardAndRepository.restoreAndCheck(second);
        } finally {
            shardAndRepository.close();
        }
    }

    /**
     * Two snapshots of the same shard copy the same commit blobs. The second snapshot starts from a generation that does not know the
     * copies of the first snapshot, as a snapshot does after an earlier snapshot failed while it was being finalized. Each snapshot names
     * its copies after the shard generation that it wrote as an {@code index-<uuid>} blob, so the two snapshots give the same commit blob
     * two different copy names. The test framework's check of blob types accepts these names.
     */
    public void testAttemptsNameTheirCopiesAfterTheirOwnShardGeneration() throws IOException {
        final var shardAndRepository = new ShardAndRepository();
        try {
            shardAndRepository.indexSegments(between(2, 4));
            final Map<String, SnapshotShardContext.CopySource> sources = shardAndRepository.writeCommitBlobs(
                between(1, 3),
                name -> true,
                0
            );
            final ShardGeneration firstGeneration = shardAndRepository.snapshot(shardAndRepository.newSnapshot(), sources::get);
            final Set<String> firstCopies = copiedBlobs(shardAndRepository.shardContainer());

            // the second attempt starts as if the shard had no snapshot yet, so it copies every commit blob again
            final ShardGeneration secondGeneration = shardAndRepository.snapshot(
                shardAndRepository.newSnapshot(),
                IndexShardSnapshotStatus.newInitializing(ShardGenerations.NEW_SHARD_GEN, randomLongBetween(1, Long.MAX_VALUE)),
                sources::get,
                name -> true
            );
            final Set<String> allCopies = copiedBlobs(shardAndRepository.shardContainer());

            assertThat(secondGeneration, not(equalTo(firstGeneration)));
            assertThat(firstCopies, equalTo(copyNames(firstGeneration, sources.values())));
            assertThat(Sets.difference(allCopies, firstCopies), equalTo(copyNames(secondGeneration, sources.values())));
            // each attempt wrote an index-<uuid> blob for the generation that is in the names of its copies
            final Set<String> blobs = shardAndRepository.shardContainer().listBlobs(OperationPurpose.SNAPSHOT_METADATA).keySet();
            assertTrue(blobs.contains(BlobStoreRepository.SNAPSHOT_INDEX_PREFIX + firstGeneration.getGenerationUUID()));
            assertTrue(blobs.contains(BlobStoreRepository.SNAPSHOT_INDEX_PREFIX + secondGeneration.getGenerationUUID()));
            shardAndRepository.assertCopiesHaveTheShardDataType();
        } finally {
            shardAndRepository.close();
        }
    }

    /**
     * A repository in the old format has numeric shard generations. A numeric generation repeats across attempts, so a copy cannot be
     * named after it. The context offers a copy source for every file, but the snapshot uploads the files and makes no copy.
     */
    public void testNumericShardGenerationsUploadInsteadOfCopying() throws IOException {
        final var shardAndRepository = new ShardAndRepository();
        try {
            shardAndRepository.indexSegments(between(1, 3));
            final Map<String, SnapshotShardContext.CopySource> sources = shardAndRepository.writeCommitBlobs(
                between(1, 3),
                name -> true,
                0
            );
            final IndexVersion oldVersion = IndexVersionUtils.randomVersionBetween(
                IndexVersions.V_7_0_0,
                SnapshotsService.OLD_SNAPSHOT_FORMAT
            );
            assertFalse(SnapshotsServiceUtils.useShardGenerations(oldVersion));
            final Snapshot snapshot = shardAndRepository.newSnapshot();
            final var generation = new AtomicReference<ShardGeneration>();
            MockLog.assertThatLogger(
                () -> runUnchecked(
                    () -> generation.set(
                        shardAndRepository.snapshot(
                            snapshot,
                            IndexShardSnapshotStatus.newInitializing(null, randomLongBetween(1, Long.MAX_VALUE)),
                            oldVersion,
                            sources::get,
                            name -> true
                        )
                    )
                ),
                BlobStoreRepository.class,
                new MockLog.UnseenEventExpectation(
                    "no copy stats",
                    BlobStoreRepository.class.getCanonicalName(),
                    Level.INFO,
                    "*shard snapshot copy stats*"
                )
            );

            // the shard generation is a number, and the snapshot wrote the index-N blob for it
            assertThat(generation.get().getGenerationUUID(), equalTo("0"));
            final Set<String> blobs = shardAndRepository.shardContainer().listBlobs(OperationPurpose.SNAPSHOT_METADATA).keySet();
            assertTrue(blobs.contains(BlobStoreRepository.SNAPSHOT_INDEX_PREFIX + "0"));
            assertThat(copiedBlobs(blobs), empty());
            assertThat(
                blobs.stream().filter(name -> name.startsWith(BlobStoreRepository.UPLOADED_DATA_BLOB_PREFIX)).toList(),
                not(empty())
            );
            for (FileInfo fileInfo : shardAndRepository.loadShardSnapshot(snapshot).indexFiles()) {
                assertFalse(fileInfo + " is copied", fileInfo.isCopied());
            }
            shardAndRepository.restoreAndCheck(snapshot);
        } finally {
            shardAndRepository.close();
        }
    }

    /**
     * The shard snapshot copy stats log line counts the files that a snapshot reuses by name. These are files that an earlier snapshot
     * already holds with the same name, length and checksum. The line splits them by how the earlier snapshot stored them. Here the first
     * snapshot uploads one data file and copies all the other data files. The second snapshot adds one segment and reuses those data
     * files by name, so the line shows one uploaded file and the copied files.
     */
    public void testStatsCountFilesReusedByName() throws IOException {
        final var shardAndRepository = new ShardAndRepository();
        try {
            shardAndRepository.indexSegments(between(2, 4));
            final Map<String, SnapshotShardContext.CopySource> sources = new HashMap<>(
                shardAndRepository.writeCommitBlobs(1, name -> true, 0)
            );
            // the first snapshot has no copy source for one data file, so it uploads that file
            final String uploaded = randomFrom(sources.keySet());
            final Snapshot first = shardAndRepository.newSnapshot();
            final ShardGeneration firstGeneration = shardAndRepository.snapshot(
                first,
                IndexShardSnapshotStatus.newInitializing(null, randomLongBetween(1, Long.MAX_VALUE)),
                name -> name.equals(uploaded) ? null : sources.get(name),
                name -> true
            );

            // One more segment adds new data files. They go into one new commit blob, which the second snapshot copies. The data files of
            // the first snapshot are still in the commit, so the second snapshot finds their file infos in the generation of the first
            // snapshot by name, length and checksum, and reuses them.
            shardAndRepository.indexSegments(1);
            final Set<String> known = Set.copyOf(sources.keySet());
            sources.putAll(shardAndRepository.writeCommitBlobs(1, name -> known.contains(name) == false, 0));
            final Map<String, StoreFileMetadata> commitFiles = shardAndRepository.commitFiles();
            final List<FileInfo> reusedFiles = shardAndRepository.loadShardSnapshot(first)
                .indexFiles()
                .stream()
                .filter(
                    fileInfo -> commitFiles.containsKey(fileInfo.physicalName())
                        && fileInfo.isSame(commitFiles.get(fileInfo.physicalName()))
                )
                .toList();
            final List<FileInfo> reusedCopied = reusedFiles.stream().filter(FileInfo::isCopied).toList();
            final List<FileInfo> reusedUploaded = reusedFiles.stream()
                .filter(fileInfo -> fileInfo.isCopied() == false && fileInfo.metadata().hashEqualsContents() == false)
                .toList();
            assertThat(reusedUploaded.size(), equalTo(1));

            final Snapshot second = shardAndRepository.newSnapshot();
            assertCopyStatsLogged(
                () -> shardAndRepository.snapshot(
                    second,
                    IndexShardSnapshotStatus.newInitializing(firstGeneration, randomLongBetween(1, Long.MAX_VALUE)),
                    sources::get,
                    name -> true
                ),
                shardAndRepository.copyStatsPrefix(second)
                    + ": new copies [1], copied bytes [*], total copy time [*ms], slowest copy [*ms], files in new copies [*],"
                    + " reused copies [0], files in reused copies [0], files reused by name, uploaded [1],"
                    + " bytes reused by name, uploaded ["
                    + reusedUploaded.get(0).length()
                    + "], files reused by name, copied ["
                    + reusedCopied.size()
                    + "], bytes reused by name, copied ["
                    + reusedCopied.stream().mapToLong(FileInfo::length).sum()
                    + "], uploaded files [0], uploaded bytes [0], dead bytes [*], dead bytes per new copy [*]"
            );

            shardAndRepository.restoreAndCheck(second);
        } finally {
            shardAndRepository.close();
        }
    }

    /**
     * A context can refuse to reuse a copied file info. The file then gets a new file info that points at the commit blob that holds the
     * file now, here a second commit blob that has another copy of the file. The snapshot makes one new copy, for that commit blob, and
     * keeps the copy of the first snapshot. The other files keep their file infos.
     */
    public void testRefusedReuseGivesTheFileANewFileInfoAtItsNewSource() throws IOException {
        final var shardAndRepository = new ShardAndRepository();
        try {
            shardAndRepository.indexSegments(between(2, 4));
            final Map<String, SnapshotShardContext.CopySource> sources = new HashMap<>(
                shardAndRepository.writeCommitBlobs(1, name -> true, 0)
            );
            final Snapshot first = shardAndRepository.newSnapshot();
            final ShardGeneration firstGeneration = shardAndRepository.snapshot(first, sources::get);
            final Set<String> firstCopies = copiedBlobs(shardAndRepository.shardContainer());
            final Map<String, FileInfo> firstFiles = filesByPhysicalName(shardAndRepository.loadShardSnapshot(first));

            // The second commit blob has another copy of one data file, as the commit blob of a later commit has of a generational file.
            final String moved = randomFrom(sources.keySet());
            final SnapshotShardContext.CopySource newSource = shardAndRepository.writeCommitBlobs(1, name -> name.equals(moved), 0)
                .get(moved);
            sources.put(moved, newSource);

            final Snapshot second = shardAndRepository.newSnapshot();
            final IndexShardSnapshotStatus status = IndexShardSnapshotStatus.newInitializing(
                firstGeneration,
                randomLongBetween(1, Long.MAX_VALUE)
            );
            final ShardGeneration secondGeneration = shardAndRepository.snapshot(
                second,
                status,
                IndexVersion.current(),
                sources::get,
                name -> true,
                // reuse a file info unless the file is in another commit blob now
                fileInfo -> sources.get(fileInfo.physicalName()).source().equals(fileInfo.source())
            );

            final Map<String, FileInfo> secondFiles = filesByPhysicalName(shardAndRepository.loadShardSnapshot(second));
            assertThat(secondFiles.keySet(), equalTo(firstFiles.keySet()));
            for (Map.Entry<String, FileInfo> entry : firstFiles.entrySet()) {
                if (entry.getKey().equals(moved) == false) {
                    final FileInfo secondFile = secondFiles.get(entry.getKey());
                    assertTrue(secondFile + " is not the file info " + entry.getValue(), secondFile.isSame(entry.getValue()));
                }
            }
            final FileInfo movedFile = secondFiles.get(moved);
            assertThat(movedFile.name(), not(equalTo(firstFiles.get(moved).name())));
            assertThat(movedFile.source(), equalTo(newSource.source()));
            assertThat(movedFile.offset(), equalTo(newSource.offset()));
            final Set<String> newCopies = copyNames(secondGeneration, List.of(newSource));
            assertThat(Set.of(movedFile.blob()), equalTo(newCopies));
            assertThat(copiedBlobs(shardAndRepository.shardContainer()), equalTo(Sets.union(firstCopies, newCopies)));
            assertThat(status.asCopy().getIncrementalFileCount(), equalTo(1));
            assertThat(status.asCopy().getProcessedFileCount(), equalTo(1));

            shardAndRepository.restoreAndCheck(second);
        } finally {
            shardAndRepository.close();
        }
    }

    /**
     * The repository asks the context only about copied file infos. It reuses an uploaded file info without asking. When the context
     * refuses to reuse the copied file infos, the new file infos point at the existing copy of the same commit blob, so the refusal
     * costs new file infos and no new copy.
     */
    public void testContextIsAskedOnlyAboutCopiedFileInfos() throws IOException {
        final var shardAndRepository = new ShardAndRepository();
        try {
            shardAndRepository.indexSegments(between(2, 4));
            final Map<String, SnapshotShardContext.CopySource> sources = shardAndRepository.writeCommitBlobs(1, name -> true, 0);
            // the first snapshot has no copy source for one data file, so it uploads that file
            final String uploaded = randomFrom(sources.keySet());
            final Snapshot first = shardAndRepository.newSnapshot();
            final ShardGeneration firstGeneration = shardAndRepository.snapshot(
                first,
                IndexShardSnapshotStatus.newInitializing(null, randomLongBetween(1, Long.MAX_VALUE)),
                name -> name.equals(uploaded) ? null : sources.get(name),
                name -> true
            );
            final Set<String> firstCopies = copiedBlobs(shardAndRepository.shardContainer());
            final Map<String, FileInfo> firstFiles = filesByPhysicalName(shardAndRepository.loadShardSnapshot(first));
            assertFalse(firstFiles.get(uploaded).isCopied());

            final List<FileInfo> offered = Collections.synchronizedList(new ArrayList<>());
            final Snapshot second = shardAndRepository.newSnapshot();
            shardAndRepository.snapshot(
                second,
                IndexShardSnapshotStatus.newInitializing(firstGeneration, randomLongBetween(1, Long.MAX_VALUE)),
                IndexVersion.current(),
                sources::get,
                name -> true,
                fileInfo -> {
                    offered.add(fileInfo);
                    return false;
                }
            );

            assertThat(offered, not(empty()));
            assertTrue(offered.stream().allMatch(FileInfo::isCopied));
            final Map<String, FileInfo> secondFiles = filesByPhysicalName(shardAndRepository.loadShardSnapshot(second));
            assertThat(secondFiles.keySet(), equalTo(firstFiles.keySet()));
            for (Map.Entry<String, FileInfo> entry : firstFiles.entrySet()) {
                final FileInfo firstFile = entry.getValue();
                final FileInfo secondFile = secondFiles.get(entry.getKey());
                if (firstFile.isCopied()) {
                    // a new file info at the same range of the same copy
                    assertThat(secondFile.name(), not(equalTo(firstFile.name())));
                    assertThat(secondFile.blob(), equalTo(firstFile.blob()));
                    assertThat(secondFile.offset(), equalTo(firstFile.offset()));
                } else {
                    assertTrue(secondFile + " is not the file info " + firstFile, secondFile.isSame(firstFile));
                }
            }
            assertThat(copiedBlobs(shardAndRepository.shardContainer()), equalTo(firstCopies));

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
                        ? new SnapshotShardContext.CopySource(
                            source.source() + "_missing",
                            "1_" + missingBlob,
                            listingWithMissingBlob,
                            missingBlob,
                            0
                        )
                        : new SnapshotShardContext.CopySource(
                            source.source(),
                            source.copyName(),
                            listingWithMissingBlob,
                            source.blobName(),
                            source.offset()
                        )
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
     * The status of a finished shard snapshot comes from its {@code snap-<uuid>.dat} blob. A copied file counts like an uploaded file:
     * once, and with its own length, not with the length of the copied blob that holds it. The second snapshot copies a new commit blob and
     * reuses the files of the first snapshot, so only its new files count as incremental.
     */
    public void testSnapshotStatusCountsCopiedFilesByTheirOwnLength() throws IOException {
        final var shardAndRepository = new ShardAndRepository();
        try {
            shardAndRepository.indexSegments(between(1, 3));
            final Map<String, SnapshotShardContext.CopySource> sources = new HashMap<>(
                shardAndRepository.writeCommitBlobs(between(1, 3), name -> true, 0)
            );
            final Collection<StoreFileMetadata> firstFiles = shardAndRepository.commitFiles().values();
            final Snapshot first = shardAndRepository.newSnapshot();
            final ShardGeneration firstGeneration = shardAndRepository.snapshot(first, sources::get);
            // every file is new in the first snapshot
            shardAndRepository.assertStatusCounts(first, firstFiles, firstFiles);

            // The second snapshot has a new segment next to the files of the first snapshot. It copies the new commit blob, and reuses the
            // files that the first snapshot has.
            shardAndRepository.indexSegments(1);
            final Map<String, StoreFileMetadata> secondFiles = shardAndRepository.commitFiles();
            final List<StoreFileMetadata> newFiles = secondFiles.values()
                .stream()
                .filter(file -> firstFiles.stream().noneMatch(firstFile -> firstFile.name().equals(file.name())))
                .toList();
            assertThat(newFiles, not(empty()));
            final Set<String> known = Set.copyOf(sources.keySet());
            sources.putAll(shardAndRepository.writeCommitBlobs(between(1, 2), name -> known.contains(name) == false, 0));
            final Snapshot second = shardAndRepository.newSnapshot();
            shardAndRepository.snapshot(
                second,
                IndexShardSnapshotStatus.newInitializing(firstGeneration, randomLongBetween(1, Long.MAX_VALUE)),
                sources::get,
                name -> true
            );
            shardAndRepository.assertStatusCounts(second, secondFiles.values(), newFiles);
            assertThat(copiedBlobs(shardAndRepository.shardContainer()), not(empty()));
        } finally {
            shardAndRepository.close();
        }
    }

    /**
     * An attempt that fails after it has made copies logs what it spent on copies. It logs that before the failure cleanup deletes the
     * copies.
     */
    public void testFailedAttemptLogsCopyStatsBeforeCleanup() throws IOException {
        // The copy calls return, but the copies are then missing or one byte short, so the check of the copies fails after all the copies
        // are made.
        final boolean dropCopy = randomBoolean();
        final var statsLogged = new AtomicBoolean();
        final var statsLoggedBeforeCleanup = new AtomicReference<Boolean>();
        final var shardAndRepository = new ShardAndRepository(
            container -> new RunBeforeDelete(
                new BrokenCopies(container, dropCopy),
                () -> statsLoggedBeforeCleanup.compareAndSet(null, statsLogged.get())
            )
        );
        try {
            shardAndRepository.indexSegments(between(1, 3));
            final Map<String, SnapshotShardContext.CopySource> sources = shardAndRepository.writeCommitBlobs(
                between(1, 3),
                name -> true,
                0
            );
            final long copies = sources.values().stream().map(SnapshotShardContext.CopySource::blobName).distinct().count();
            final Snapshot snapshot = shardAndRepository.newSnapshot();
            final String message = shardAndRepository.copyStatsPrefix(snapshot)
                + ": attempt [failed], copies made ["
                + copies
                + "], copied bytes [*], total copy time [*ms]";
            MockLog.assertThatLogger(
                () -> expectThrows(Exception.class, () -> shardAndRepository.snapshot(snapshot, sources::get)),
                BlobStoreRepository.class,
                new LogMessageSeen(message, statsLogged)
            );
            assertThat(statsLoggedBeforeCleanup.get(), equalTo(true));
            shardAndRepository.assertFailedSnapshotLeftNoData(snapshot);
        } finally {
            shardAndRepository.close();
        }
    }

    /**
     * An attempt that pauses after a copy, as a shard snapshot does when its node is shutting down, logs what it spent on copies.
     */
    public void testPausedAttemptLogsCopyStats() throws IOException {
        final var statusRef = new AtomicReference<IndexShardSnapshotStatus>();
        final var shardAndRepository = new ShardAndRepository(container -> new PausingAfterCopy(container, statusRef::get));
        try {
            shardAndRepository.indexSegments(between(1, 3));
            // one commit blob, so the shard snapshot makes one copy, and pauses right after it
            final Map<String, SnapshotShardContext.CopySource> sources = shardAndRepository.writeCommitBlobs(1, name -> true, 0);
            final Snapshot snapshot = shardAndRepository.newSnapshot();
            final var status = IndexShardSnapshotStatus.newInitializing(null, randomLongBetween(1, Long.MAX_VALUE));
            statusRef.set(status);
            final String message = shardAndRepository.copyStatsPrefix(snapshot)
                + ": attempt [paused], copies made [1], copied bytes [*], total copy time [*ms]";
            MockLog.assertThatLogger(() -> {
                final Exception e = expectThrows(
                    Exception.class,
                    () -> shardAndRepository.snapshot(snapshot, status, sources::get, name -> true)
                );
                assertThat(ExceptionsHelper.unwrap(e, PausedSnapshotException.class), notNullValue());
            }, BlobStoreRepository.class, new LogMessageSeen(message, new AtomicBoolean()));
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

    /**
     * The names that the copies of the given commit blobs have when an attempt with the given shard generation makes them: {@code c__},
     * the shard generation, an underscore, and the copy name of the source.
     */
    private static Set<String> copyNames(ShardGeneration generation, Collection<SnapshotShardContext.CopySource> sources) {
        return sources.stream()
            .map(source -> BlobStoreRepository.COPIED_DATA_BLOB_PREFIX + generation.getGenerationUUID() + "_" + source.copyName())
            .collect(Collectors.toSet());
    }

    private static Map<String, FileInfo> filesByPhysicalName(BlobStoreIndexShardSnapshot shardSnapshot) {
        return shardSnapshot.indexFiles().stream().collect(Collectors.toMap(FileInfo::physicalName, fileInfo -> fileInfo));
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
            return logPrefix(snapshot) + " shard snapshot copy stats";
        }

        /**
         * The start of every log line of a shard snapshot of this shard that names the repository, the snapshot and the shard.
         */
        String logPrefix(Snapshot snapshot) {
            return "[" + repository.getMetadata().name() + "][" + snapshot.getSnapshotId() + "]" + shardId;
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
                    // like the stateless side: the primary term and the blob name
                    final String copyName = "1_" + blobName;
                    try (BytesStreamOutput blobBytes = new BytesStreamOutput()) {
                        for (int i = blob; i < dataFiles.size(); i += blobs) {
                            blobBytes.writeBytes(randomByteArrayOfLength(between(0, 512)));
                            final StoreFileMetadata dataFile = dataFiles.get(i);
                            sources.put(
                                dataFile.name(),
                                new SnapshotShardContext.CopySource(source, copyName, objectStore, blobName, blobBytes.position())
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
         * The files of the shard's last commit, by name.
         */
        Map<String, StoreFileMetadata> commitFiles() throws IOException {
            try (Engine.IndexCommitRef commitRef = shard.acquireLastIndexCommit(true)) {
                return shard.store().getMetadata(commitRef.getIndexCommit()).fileMetadataMap();
            }
        }

        /**
         * Checks the counts in the status of a finished shard snapshot, which the repository reads from the snapshot's blob: the number and
         * the total length of all the files in the commit, and of the files that were new in the snapshot.
         */
        void assertStatusCounts(Snapshot snapshot, Collection<StoreFileMetadata> allFiles, Collection<StoreFileMetadata> newFiles) {
            final IndexShardSnapshotStatus.Copy status = repository.getShardSnapshotStatus(snapshot.getSnapshotId(), indexId, shardId);
            assertThat(status.getStage(), equalTo(IndexShardSnapshotStatus.Stage.DONE));
            assertThat(status.getTotalFileCount(), equalTo(allFiles.size()));
            assertThat(status.getTotalSize(), equalTo(allFiles.stream().mapToLong(StoreFileMetadata::length).sum()));
            assertThat(status.getIncrementalFileCount(), equalTo(newFiles.size()));
            assertThat(status.getIncrementalSize(), equalTo(newFiles.stream().mapToLong(StoreFileMetadata::length).sum()));
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
         * Checks that the test framework's check of blob types knows every blob in the shard's folder, and takes each copy for shard data.
         */
        void assertCopiesHaveTheShardDataType() throws IOException {
            final Path repositoryRoot = ((FsBlobStore) repository.blobStore()).path();
            final Path shardFolder = repositoryRoot.resolve("indices").resolve(indexId.getId()).resolve(Integer.toString(shardId.id()));
            try (Stream<Path> blobs = Files.list(shardFolder)) {
                for (Path blob : blobs.toList()) {
                    final RepositoryFileType type = RepositoryFileType.getRepositoryFileType(repositoryRoot, blob);
                    if (blob.getFileName().toString().startsWith(BlobStoreRepository.COPIED_DATA_BLOB_PREFIX)) {
                        assertThat(blob.toString(), type, equalTo(RepositoryFileType.SHARD_DATA));
                    }
                }
            }
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
            return snapshot(snapshot, status, IndexVersion.current(), copySources, visibleFiles);
        }

        /**
         * Takes a shard snapshot into a repository that has the given metadata version, through a context that offers the given copy
         * sources and shows only the visible files.
         */
        ShardGeneration snapshot(
            Snapshot snapshot,
            IndexShardSnapshotStatus status,
            IndexVersion repositoryMetaVersion,
            Function<String, SnapshotShardContext.CopySource> copySources,
            Predicate<String> visibleFiles
        ) throws IOException {
            return snapshot(snapshot, status, repositoryMetaVersion, copySources, visibleFiles, null);
        }

        /**
         * Takes a shard snapshot into a repository that has the given metadata version, through a context that offers the given copy
         * sources, shows only the visible files, and answers {@link SnapshotShardContext#isCopiedFileReusable} with the given rule. The
         * context keeps the default answer of {@link SnapshotShardContext} if the rule is {@code null}.
         */
        ShardGeneration snapshot(
            Snapshot snapshot,
            IndexShardSnapshotStatus status,
            IndexVersion repositoryMetaVersion,
            Function<String, SnapshotShardContext.CopySource> copySources,
            Predicate<String> visibleFiles,
            @Nullable Predicate<FileInfo> reusableCopies
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
                    repositoryMetaVersion,
                    randomMillisUpToYear9999(),
                    future
                );
                repository.snapshotShard(new CopyingSnapshotShardContext(localContext, copySources, visibleFiles, reusableCopies));
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
     * A context that offers copy sources for a shard's files, can hide some files, and can refuse to reuse copied file infos, on top of
     * the context for a local primary.
     */
    private static final class CopyingSnapshotShardContext extends SnapshotShardContext {
        private final LocalPrimarySnapshotShardContext localContext;
        private final Function<String, CopySource> copySources;
        private final Predicate<String> visibleFiles;
        @Nullable // to keep the default answer of the superclass
        private final Predicate<FileInfo> reusableCopies;

        CopyingSnapshotShardContext(
            LocalPrimarySnapshotShardContext localContext,
            Function<String, CopySource> copySources,
            Predicate<String> visibleFiles,
            @Nullable Predicate<FileInfo> reusableCopies
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
            this.reusableCopies = reusableCopies;
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

        @Override
        public boolean isCopiedFileReusable(FileInfo existingFileInfo) {
            return reusableCopies == null ? super.isCopiedFileReusable(existingFileInfo) : reusableCopies.test(existingFileInfo);
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

    /**
     * Expects at least one INFO message that matches the given pattern, in which {@code *} matches any text, and sets the flag as soon as
     * it has seen one. The flag lets a test check what happened before the message, not only that the message came.
     */
    private static final class LogMessageSeen implements MockLog.LoggingExpectation {
        private final String pattern;
        private final AtomicBoolean seen;

        LogMessageSeen(String pattern, AtomicBoolean seen) {
            this.pattern = pattern;
            this.seen = seen;
        }

        @Override
        public void match(LogEvent event) {
            if (event.getLevel() == Level.INFO && Regex.simpleMatch(pattern, event.getMessage().getFormattedMessage())) {
                seen.set(true);
            }
        }

        @Override
        public void assertMatched() {
            assertTrue("no log message matched [" + pattern + "]", seen.get());
        }
    }

    /**
     * A shard container that runs an action right before each call that deletes blobs, and then deletes them.
     */
    private static final class RunBeforeDelete extends FilterBlobContainer {
        private final Runnable beforeDelete;

        RunBeforeDelete(BlobContainer delegate, Runnable beforeDelete) {
            super(delegate);
            this.beforeDelete = beforeDelete;
        }

        @Override
        public void deleteBlobsIgnoringIfNotExists(OperationPurpose purpose, Iterator<String> blobNames) throws IOException {
            beforeDelete.run();
            super.deleteBlobsIgnoringIfNotExists(purpose, blobNames);
        }

        @Override
        protected BlobContainer wrapChild(BlobContainer child) {
            return new RunBeforeDelete(child, beforeDelete);
        }
    }

    /**
     * A shard container that pauses the shard snapshot once a copy has been made, as a node that is shutting down does.
     */
    private static final class PausingAfterCopy extends FilterBlobContainer {
        private final Supplier<IndexShardSnapshotStatus> status;

        PausingAfterCopy(BlobContainer delegate, Supplier<IndexShardSnapshotStatus> status) {
            super(delegate);
            this.status = status;
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
            status.get().pauseIfNotCompleted(notifyOnPaused -> notifyOnPaused.onResponse(() -> {}));
        }

        @Override
        protected BlobContainer wrapChild(BlobContainer child) {
            return new PausingAfterCopy(child, status);
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
