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
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingHelper;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.fs.FsBlobContainer;
import org.elasticsearch.common.blobstore.fs.FsBlobStore;
import org.elasticsearch.common.blobstore.support.BlobMetadata;
import org.elasticsearch.common.blobstore.support.FilterBlobContainer;
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
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshots;
import org.elasticsearch.index.snapshots.blobstore.SnapshotFiles;
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
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.IntPredicate;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;

/**
 * Tests the server-side copy path of shard snapshots. A plain FS container stands in for the stateless object store: the shard's data
 * files are written into a few commit blobs there, with filler around each file, and a snapshot copies those commit blobs instead of
 * uploading the files.
 */
@LuceneTestCase.SuppressFileSystems("ExtrasFS") // assertCopiesHaveTheShardDataType rejects the extra files as unknown blob types
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
            // The copy stats log line counts each commit blob as one new copy, of the blob's whole length, which the copy call returns. Its
            // dead bytes are the filler that writeCommitBlobs puts before, between and after the files, because no file uses it. The
            // needed end of a copy is where the last file in the blob ends, so it is shorter than the blob if there is filler after it.
            final Map<String, BlobMetadata> objectStoreBlobs = shardAndRepository.objectStore.listBlobs(OperationPurpose.SNAPSHOT_METADATA);
            final Map<String, Long> blobLengths = new HashMap<>();
            final Map<String, Long> fileBytes = new HashMap<>();
            final Map<String, Long> neededEnds = new HashMap<>();
            for (Map.Entry<String, SnapshotShardContext.CopySource> entry : sources.entrySet()) {
                final String blobName = entry.getValue().blobName();
                blobLengths.put(blobName, objectStoreBlobs.get(blobName).length());
                final long fileLength = shardAndRepository.fileLength(entry.getKey());
                fileBytes.merge(blobName, fileLength, Long::sum);
                neededEnds.merge(blobName, entry.getValue().offset() + fileLength, Math::max);
            }
            final List<Long> deadBytes = blobLengths.keySet()
                .stream()
                .map(blobName -> blobLengths.get(blobName) - fileBytes.get(blobName))
                .sorted(Comparator.reverseOrder())
                .toList();
            final AtomicReference<ShardGeneration> generation = new AtomicReference<>();
            assertCopyStatsLogged(
                () -> generation.set(shardAndRepository.snapshot(snapshot, status, sources::get, name -> true)),
                shardAndRepository.copyStatsPrefix(snapshot)
                    + ": new copies ["
                    + blobLengths.size()
                    + "], copied bytes ["
                    + blobLengths.values().stream().mapToLong(Long::longValue).sum()
                    + "], total copy time [*ms], slowest copy [*ms], files in new copies ["
                    + sources.size()
                    + "], files reused by name, uploaded [0],"
                    + " bytes reused by name, uploaded [0], files reused by name, copied [0], bytes reused by name, copied [0],"
                    + " uploaded files [0], uploaded bytes [0], dead bytes ["
                    + deadBytes.stream().mapToLong(Long::longValue).sum()
                    + "], dead bytes per new copy "
                    + deadBytes
                    + statsTail(neededEnds.values().stream().mapToLong(Long::longValue).sum(), blobLengths.size(), blobLengths.size())
            );

            // Every data file is a range of a copy of the commit blob that holds it, at the same offset. The files of one commit blob
            // share the name of its copy, and the copies of two commit blobs have two names.
            final BlobStoreIndexShardSnapshot shardSnapshot = shardAndRepository.loadShardSnapshot(snapshot);
            final Map<String, String> copyBySource = new HashMap<>();
            for (FileInfo fileInfo : shardSnapshot.indexFiles()) {
                if (fileInfo.metadata().hashEqualsContents()) {
                    assertFalse(fileInfo.isCopied());
                    continue;
                }
                assertTrue(fileInfo + " is not copied", fileInfo.isCopied());
                final SnapshotShardContext.CopySource source = sources.get(fileInfo.physicalName());
                assertThat(fileInfo.offset(), equalTo(source.offset()));
                assertThat(copyBySource.computeIfAbsent(source.source(), ignored -> fileInfo.name()), equalTo(fileInfo.name()));
            }
            assertThat(Set.copyOf(copyBySource.values()).size(), equalTo(copyBySource.size()));
            shardAndRepository.assertShardGenerationHasTheFilesOf(snapshot, generation.get());

            // each commit blob was copied once, whole, and no file was uploaded: the only data blobs are the copies
            final Set<String> shardBlobs = shardAndRepository.shardContainer().listBlobs(OperationPurpose.SNAPSHOT_METADATA).keySet();
            assertThat(dataBlobs(shardBlobs), equalTo(Set.copyOf(copyBySource.values())));
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
     * Each copy of a commit blob is logged at debug with the name of the copy, the bytes that the snapshot needs from the commit blob,
     * the length of the commit blob and the time that the copy call took. A run can then show which copies were slow.
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
            // one line for each commit blob, with the length of the commit blob, which the copy has as well, and the needed end
            final Map<String, Long> neededEnds = new HashMap<>();
            for (Map.Entry<String, SnapshotShardContext.CopySource> entry : sources.entrySet()) {
                neededEnds.merge(
                    entry.getValue().source(),
                    entry.getValue().offset() + shardAndRepository.fileLength(entry.getKey()),
                    Math::max
                );
            }
            final List<MockLog.LoggingExpectation> expectations = new ArrayList<>();
            for (Map.Entry<String, String> entry : blobNameBySource.entrySet()) {
                expectations.add(
                    new MockLog.SeenEventExpectation(
                        "copy of " + entry.getKey(),
                        BlobStoreRepository.class.getCanonicalName(),
                        Level.DEBUG,
                        shardAndRepository.logPrefix(snapshot)
                            + " copied blob [__*] from ["
                            + entry.getKey()
                            + "]: needed ["
                            + neededEnds.get(entry.getKey())
                            + "] bytes, source length ["
                            + objectStoreBlobs.get(entry.getValue()).length()
                            + "], in [*ms]"
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

    /**
     * A later snapshot that sees a file of a commit blob for the first time makes its own copy of the whole commit blob, although an
     * earlier snapshot copied that commit blob already. The repository never looks for an earlier copy of the same commit blob. It reuses
     * file infos only by the Lucene name, length and checksum of a file, so the other files of the commit keep their file infos, which
     * point at the first copy.
     */
    public void testLaterSnapshotCopiesTheCommitBlobAgainForAFileThatIsNew() throws IOException {
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
            final Set<String> copiesAfterFirst = dataBlobs(shardAndRepository.shardContainer());
            assertThat(copiesAfterFirst.size(), equalTo(1));
            final String firstCopy = copiesAfterFirst.iterator().next();

            // The second snapshot sees that file for the first time, and makes a second copy of the commit blob for it.
            final Snapshot second = shardAndRepository.newSnapshot();
            final IndexShardSnapshotStatus status = IndexShardSnapshotStatus.newInitializing(
                firstGeneration,
                randomLongBetween(1, Long.MAX_VALUE)
            );
            // The other data files of the commit are in the first snapshot, so the second snapshot reuses them by their names.
            final List<FileInfo> firstFiles = shardAndRepository.loadShardSnapshot(first).indexFiles();
            final List<FileInfo> reusedByName = firstFiles.stream().filter(FileInfo::isCopied).toList();
            assertThat(reusedByName.size(), equalTo(sources.size() - 1));
            final AtomicReference<ShardGeneration> secondGeneration = new AtomicReference<>();
            assertCopyStatsLogged(
                () -> secondGeneration.set(shardAndRepository.snapshot(second, status, sources::get, name -> true)),
                shardAndRepository.copyStatsPrefix(second)
                    + ": new copies [1], copied bytes [*], total copy time [*ms], slowest copy [*ms], files in new copies [1],"
                    + " files reused by name, uploaded [0], bytes reused by name, uploaded [0], files reused by name, copied ["
                    + reusedByName.size()
                    + "], bytes reused by name, copied ["
                    + reusedByName.stream().mapToLong(FileInfo::length).sum()
                    + "], uploaded files [0], uploaded bytes [0], dead bytes [*], dead bytes per new copy [*], copied bytes at least [*],"
                    + " exact lengths [1 of 1], copied bytes and dead bytes count only the new copies with an exact length"
            );

            final Set<String> copiesAfterSecond = dataBlobs(shardAndRepository.shardContainer());
            assertThat(copiesAfterSecond.size(), equalTo(2));
            assertTrue(copiesAfterSecond.contains(firstCopy));
            final String secondCopy = Sets.difference(copiesAfterSecond, copiesAfterFirst).iterator().next();
            final Map<String, FileInfo> secondFiles = filesByPhysicalName(shardAndRepository.loadShardSnapshot(second));
            final FileInfo hiddenFileInfo = secondFiles.get(hidden);
            assertThat(hiddenFileInfo.name(), equalTo(secondCopy));
            assertThat(hiddenFileInfo.offset(), equalTo(sources.get(hidden).offset()));
            for (FileInfo fileInfo : reusedByName) {
                assertTrue(
                    secondFiles.get(fileInfo.physicalName()) + " is not " + fileInfo,
                    secondFiles.get(fileInfo.physicalName()).isSame(fileInfo)
                );
                assertThat(secondFiles.get(fileInfo.physicalName()).name(), equalTo(firstCopy));
            }
            assertThat(status.asCopy().getIncrementalFileCount(), equalTo(1));
            assertThat(status.asCopy().getProcessedFileCount(), equalTo(1));
            shardAndRepository.assertShardGenerationHasTheFilesOf(second, secondGeneration.get());

            shardAndRepository.restoreAndCheck(second);
        } finally {
            shardAndRepository.close();
        }
    }

    /**
     * A snapshot of an unchanged shard, one that has the same shard state identifier as an earlier snapshot, takes the file list of that
     * snapshot as its own and looks at no file. The files of one copied blob share its name, so the shard generation has to keep every
     * one of them apart, and the list that the shortcut takes has to hold all of them, each at its own offset.
     */
    public void testUnchangedShardShortcutKeepsFilesThatShareAName() throws IOException {
        final var shardAndRepository = new ShardAndRepository();
        try {
            shardAndRepository.indexSegments(between(2, 4));
            // one commit blob, so all the copied files have the name of one copy
            final Map<String, SnapshotShardContext.CopySource> sources = shardAndRepository.writeCommitBlobs(1, name -> true, 0);
            assertThat(sources.size(), greaterThanOrEqualTo(2));
            final String shardStateIdentifier = randomAlphaOfLength(10);
            shardAndRepository.shardStateIdentifier = shardStateIdentifier;
            final Snapshot first = shardAndRepository.newSnapshot();
            final ShardGeneration firstGeneration = shardAndRepository.snapshot(first, sources::get);
            final List<FileInfo> firstFiles = shardAndRepository.loadShardSnapshot(first).indexFiles();
            final Set<String> copies = dataBlobs(shardAndRepository.shardContainer());
            assertThat(copies.size(), equalTo(1));
            assertThat(firstFiles.stream().filter(FileInfo::isCopied).count(), equalTo((long) sources.size()));

            // The shard has not changed, so the second snapshot takes the file list of the first. The shortcut looks at no file: only the
            // path that goes through the files one by one asks the context for the file names, and for the copy source of a new file. A
            // snapshot that took that path would reuse every file by its name and write the same list, so the test makes the context fail
            // when it is asked for either.
            final Snapshot second = shardAndRepository.newSnapshot();
            final ShardGeneration secondGeneration = shardAndRepository.snapshot(
                second,
                IndexShardSnapshotStatus.newInitializing(firstGeneration, randomLongBetween(1, Long.MAX_VALUE)),
                name -> {
                    throw new AssertionError("the shortcut asked for the copy source of [" + name + "]");
                },
                name -> { throw new AssertionError("the shortcut asked for the file names, and so looked at the file [" + name + "]"); }
            );

            final List<FileInfo> secondFiles = shardAndRepository.loadShardSnapshot(second).indexFiles();
            assertThat(secondFiles.size(), equalTo(firstFiles.size()));
            for (int i = 0; i < firstFiles.size(); i++) {
                assertTrue(secondFiles.get(i) + " is not " + firstFiles.get(i), secondFiles.get(i).isSame(firstFiles.get(i)));
                assertThat(secondFiles.get(i).isCopied(), equalTo(firstFiles.get(i).isCopied()));
            }
            assertThat(dataBlobs(shardAndRepository.shardContainer()), equalTo(copies));
            // the shard generation of the second snapshot lists both snapshots, each with every file at its own offset
            shardAndRepository.assertShardGenerationHasTheFilesOf(first, secondGeneration);
            shardAndRepository.assertShardGenerationHasTheFilesOf(second, secondGeneration);
            // both snapshots have the same shard state identifier, which is what lets the second one take the shortcut
            assertThat(
                shardAndRepository.shardStateIdentifiers(secondGeneration),
                equalTo(List.of(shardStateIdentifier, shardStateIdentifier))
            );

            shardAndRepository.restoreAndCheck(second);
        } finally {
            shardAndRepository.close();
        }
    }

    /**
     * Every copy gets a fresh name of its own, like an uploaded blob: the prefix {@code __} and a random UUID. Nothing in the name comes
     * from the commit blob that was copied or from the shard generation. Two snapshots that copy the same commit blobs make copies with
     * different names, here the second snapshot starts from a generation that does not know the copies of the first, as a snapshot does
     * after an earlier snapshot failed while it was being finalized. All the files of one commit blob share the name of its copy.
     * The test framework's check of blob types accepts these names.
     */
    public void testEveryCopyGetsAFreshName() throws IOException {
        final var shardAndRepository = new ShardAndRepository();
        try {
            shardAndRepository.indexSegments(between(2, 4));
            final Map<String, SnapshotShardContext.CopySource> sources = shardAndRepository.writeCommitBlobs(
                between(1, 3),
                name -> true,
                0
            );
            final Snapshot first = shardAndRepository.newSnapshot();
            final ShardGeneration firstGeneration = shardAndRepository.snapshot(first, sources::get);
            final Set<String> firstCopies = dataBlobs(shardAndRepository.shardContainer());

            // the second attempt starts as if the shard had no snapshot yet, so it copies every commit blob again
            final Snapshot second = shardAndRepository.newSnapshot();
            final ShardGeneration secondGeneration = shardAndRepository.snapshot(
                second,
                IndexShardSnapshotStatus.newInitializing(ShardGenerations.NEW_SHARD_GEN, randomLongBetween(1, Long.MAX_VALUE)),
                sources::get,
                name -> true
            );
            final Set<String> allCopies = dataBlobs(shardAndRepository.shardContainer());

            assertThat(secondGeneration, not(equalTo(firstGeneration)));
            final Set<String> secondCopies = Sets.difference(allCopies, firstCopies);
            assertThat(copyNames(shardAndRepository.loadShardSnapshot(first)), equalTo(firstCopies));
            assertThat(copyNames(shardAndRepository.loadShardSnapshot(second)), equalTo(secondCopies));
            final long commitBlobs = sources.values().stream().map(SnapshotShardContext.CopySource::source).distinct().count();
            assertThat(firstCopies.size(), equalTo(Math.toIntExact(commitBlobs)));
            assertThat(secondCopies.size(), equalTo(Math.toIntExact(commitBlobs)));
            for (String copy : allCopies) {
                // the same kind of name as an uploaded blob, and a random one
                assertThat(copy, startsWith(BlobStoreRepository.UPLOADED_DATA_BLOB_PREFIX));
                assertThat(
                    copy.length(),
                    equalTo(BlobStoreRepository.UPLOADED_DATA_BLOB_PREFIX.length() + UUIDs.randomBase64UUID().length())
                );
                assertTrue(copy, Strings.validFileName(copy));
                for (SnapshotShardContext.CopySource source : sources.values()) {
                    assertThat(copy, not(containsString(source.blobName())));
                }
                assertThat(copy, not(containsString(firstGeneration.getGenerationUUID())));
                assertThat(copy, not(containsString(secondGeneration.getGenerationUUID())));
            }
            // the files of one commit blob share the name of its copy in each snapshot
            for (Snapshot snapshot : List.of(first, second)) {
                final Map<String, String> copyBySource = new HashMap<>();
                for (FileInfo fileInfo : shardAndRepository.loadShardSnapshot(snapshot).indexFiles()) {
                    if (fileInfo.isCopied()) {
                        final String source = sources.get(fileInfo.physicalName()).source();
                        assertThat(copyBySource.computeIfAbsent(source, ignored -> fileInfo.name()), equalTo(fileInfo.name()));
                    }
                }
                assertThat(Set.copyOf(copyBySource.values()).size(), equalTo(copyBySource.size()));
            }
            shardAndRepository.assertCopiesHaveTheShardDataType();
        } finally {
            shardAndRepository.close();
        }
    }

    /**
     * A repository in the old format has numeric shard generations. It is older than any repository version that may hold copied files,
     * so the context offers a copy source for every file, but the snapshot uploads the files and makes no copy.
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
            // every data blob is the blob of an uploaded file, because no file info has an offset
            assertThat(dataBlobs(blobs), not(empty()));
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
                    + " files reused by name, uploaded [1], bytes reused by name, uploaded ["
                    + reusedUploaded.get(0).length()
                    + "], files reused by name, copied ["
                    + reusedCopied.size()
                    + "], bytes reused by name, copied ["
                    + reusedCopied.stream().mapToLong(FileInfo::length).sum()
                    + "], uploaded files [0], uploaded bytes [0], dead bytes [*], dead bytes per new copy [*], copied bytes at least [*],"
                    + " exact lengths [1 of 1], copied bytes and dead bytes count only the new copies with an exact length"
            );

            shardAndRepository.restoreAndCheck(second);
        } finally {
            shardAndRepository.close();
        }
    }

    /**
     * A context can refuse to reuse a copied file info, as the stateless context does for a generational file. The file is then handled
     * as a new file: it gets a new file info that points at a new copy of the commit blob that holds the file now, here a second commit
     * blob that has another copy of the file. The snapshot makes one new copy, for that commit blob, and the copy of the first snapshot
     * stays. The other files keep their file infos.
     */
    public void testRefusedReuseGivesTheFileANewFileInfoInANewCopy() throws IOException {
        final var shardAndRepository = new ShardAndRepository();
        try {
            shardAndRepository.indexSegments(between(2, 4));
            final Map<String, SnapshotShardContext.CopySource> sources = new HashMap<>(
                shardAndRepository.writeCommitBlobs(1, name -> true, 0)
            );
            final Snapshot first = shardAndRepository.newSnapshot();
            final ShardGeneration firstGeneration = shardAndRepository.snapshot(first, sources::get);
            final Set<String> firstCopies = dataBlobs(shardAndRepository.shardContainer());
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
                // refuse to reuse the file info of the file that is in another commit blob now
                fileInfo -> fileInfo.physicalName().equals(moved) == false
            );

            final Map<String, FileInfo> secondFiles = filesByPhysicalName(shardAndRepository.loadShardSnapshot(second));
            assertThat(secondFiles.keySet(), equalTo(firstFiles.keySet()));
            for (Map.Entry<String, FileInfo> entry : firstFiles.entrySet()) {
                if (entry.getKey().equals(moved) == false) {
                    final FileInfo secondFile = secondFiles.get(entry.getKey());
                    assertTrue(secondFile + " is not the file info " + entry.getValue(), secondFile.isSame(entry.getValue()));
                }
            }
            final Set<String> newCopies = Sets.difference(dataBlobs(shardAndRepository.shardContainer()), firstCopies);
            assertThat(newCopies.size(), equalTo(1));
            final FileInfo movedFile = secondFiles.get(moved);
            assertThat(movedFile.name(), not(equalTo(firstFiles.get(moved).name())));
            assertThat(Set.of(movedFile.name()), equalTo(newCopies));
            assertThat(movedFile.offset(), equalTo(newSource.offset()));
            assertThat(status.asCopy().getIncrementalFileCount(), equalTo(1));
            assertThat(status.asCopy().getProcessedFileCount(), equalTo(1));
            shardAndRepository.assertShardGenerationHasTheFilesOf(second, secondGeneration);

            shardAndRepository.restoreAndCheck(second);
        } finally {
            shardAndRepository.close();
        }
    }

    /**
     * A file can have a copied file info and an uploaded file info in the shard generation, after a repository switched from copying to
     * uploading. When the context refuses to reuse the copied file info, as the stateless context does for a generational file, the
     * copied file info must not hide the uploaded one. Every later snapshot reuses the uploaded file info, so it uploads and copies
     * nothing. Each snapshot starts from a shard generation that it reads again, and so from file infos that are new objects, to show
     * that the answer does not depend on which of the two file infos the lookup meets first.
     */
    public void testRefusedCopiedFileInfoDoesNotHideAnUploadedFileInfo() throws IOException {
        final var shardAndRepository = new ShardAndRepository();
        try {
            shardAndRepository.indexSegments(between(2, 4));
            final Map<String, SnapshotShardContext.CopySource> sources = shardAndRepository.writeCommitBlobs(1, name -> true, 0);
            final String refused = randomFrom(sources.keySet());
            final Predicate<FileInfo> reusable = fileInfo -> fileInfo.physicalName().equals(refused) == false;

            // The first snapshot copies every file, so the shard generation has a copied file info for the refused file.
            ShardGeneration generation = shardAndRepository.snapshot(shardAndRepository.newSnapshot(), sources::get);

            // The second snapshot refuses that copied file info and has no copy source for the file, so it uploads the file. The other
            // files are reused.
            final Snapshot second = shardAndRepository.newSnapshot();
            generation = shardAndRepository.snapshot(
                second,
                IndexShardSnapshotStatus.newInitializing(generation, randomLongBetween(1, Long.MAX_VALUE)),
                IndexVersion.current(),
                name -> name.equals(refused) ? null : sources.get(name),
                name -> true,
                reusable
            );
            final FileInfo uploaded = filesByPhysicalName(shardAndRepository.loadShardSnapshot(second)).get(refused);
            assertFalse(uploaded + " is copied", uploaded.isCopied());
            final Set<String> dataBlobsAfterSecond = dataBlobs(shardAndRepository.shardContainer());

            // The copy source is there for every later snapshot, so a snapshot that does not reuse the uploaded file info copies the commit
            // blob again, and the test sees that as a new data blob and as a new file.
            for (int i = 0; i < 5; i++) {
                final Snapshot snapshot = shardAndRepository.newSnapshot();
                final IndexShardSnapshotStatus status = IndexShardSnapshotStatus.newInitializing(
                    generation,
                    randomLongBetween(1, Long.MAX_VALUE)
                );
                generation = shardAndRepository.snapshot(snapshot, status, IndexVersion.current(), sources::get, name -> true, reusable);
                final FileInfo reused = filesByPhysicalName(shardAndRepository.loadShardSnapshot(snapshot)).get(refused);
                assertTrue(reused + " is not " + uploaded, reused.isSame(uploaded));
                assertThat(status.asCopy().getIncrementalFileCount(), equalTo(0));
                assertThat(dataBlobs(shardAndRepository.shardContainer()), equalTo(dataBlobsAfterSecond));
            }
        } finally {
            shardAndRepository.close();
        }
    }

    /**
     * The repository asks the context only about copied file infos. It reuses an uploaded file info without asking. When the context
     * refuses to reuse every copied file info, the files get new file infos that point at one new copy of their commit blob, so the
     * refusal costs a new copy of the commit blob. The repository does not point them at the copy that the first snapshot made.
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
            final Map<String, FileInfo> firstFiles = filesByPhysicalName(shardAndRepository.loadShardSnapshot(first));
            final Set<String> firstCopies = copyNames(shardAndRepository.loadShardSnapshot(first));
            assertThat(firstCopies.size(), equalTo(1));
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
            final Set<String> secondCopies = copyNames(shardAndRepository.loadShardSnapshot(second));
            assertThat(secondCopies.size(), equalTo(1));
            assertThat(Sets.intersection(firstCopies, secondCopies), empty());
            for (Map.Entry<String, FileInfo> entry : firstFiles.entrySet()) {
                final FileInfo firstFile = entry.getValue();
                final FileInfo secondFile = secondFiles.get(entry.getKey());
                if (firstFile.isCopied()) {
                    // a new file info at the same offset of the new copy
                    assertThat(secondFile.name(), equalTo(secondCopies.iterator().next()));
                    assertThat(secondFile.offset(), equalTo(firstFile.offset()));
                } else {
                    assertTrue(secondFile + " is not the file info " + firstFile, secondFile.isSame(firstFile));
                }
            }
            // the copy of the first snapshot, the blob of the file that it uploaded, and the new copy
            assertThat(dataBlobs(shardAndRepository.shardContainer()).size(), equalTo(3));

            shardAndRepository.restoreAndCheck(second);
        } finally {
            shardAndRepository.close();
        }
    }

    /**
     * A copy that fails fails the shard snapshot, and the failure cleanup deletes the copies that this attempt made, and only those. The
     * copy that an earlier snapshot made, which that snapshot's file infos refer to, stays as it was.
     */
    public void testFailedAttemptKeepsTheCopiesOfEarlierSnapshots() throws IOException {
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
            final Set<String> copiesAfterFirst = dataBlobs(shardAndRepository.shardContainer());
            final String firstCopy = copiesAfterFirst.iterator().next();
            final long firstCopyLength = shardAndRepository.shardContainer()
                .listBlobs(OperationPurpose.SNAPSHOT_METADATA)
                .get(firstCopy)
                .length();

            // The second snapshot copies the first commit blob again for the hidden file, and a second commit blob for its new files. One
            // of its copies fails inside the copy task: the commit blob of one new file is not in the object store.
            shardAndRepository.indexSegments(1);
            final Set<String> known = Set.copyOf(sources.keySet());
            final Map<String, SnapshotShardContext.CopySource> newSources = shardAndRepository.writeCommitBlobs(
                1,
                name -> known.contains(name) == false,
                0
            );
            final String missingBlob = "stateless_commit_missing";
            final String failingFile = randomFrom(newSources.keySet());
            sources.putAll(newSources);
            final SnapshotShardContext.CopySource failingSource = newSources.get(failingFile);
            sources.put(
                failingFile,
                new SnapshotShardContext.CopySource(failingSource.source() + "_missing", shardAndRepository.objectStore, missingBlob, 0)
            );
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
            assertThat(dataBlobs(shardAndRepository.shardContainer()), equalTo(copiesAfterFirst));
            assertThat(
                shardAndRepository.shardContainer().listBlobs(OperationPurpose.SNAPSHOT_METADATA).get(firstCopy).length(),
                equalTo(firstCopyLength)
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
            // every file that has blobs of its own was uploaded, and no file info has an offset
            assertThat(dataBlobs(shardAndRepository.shardContainer()), not(empty()));
            assertThat(copyNames(shardAndRepository.loadShardSnapshot(snapshot)), empty());
            shardAndRepository.restoreAndCheck(snapshot);
        } finally {
            shardAndRepository.close();
        }
    }

    /**
     * The repository does not list the commit blobs before it copies them, so the copy call checks that a commit blob reaches to the end
     * of the last file that the snapshot takes from it. Here each commit blob loses its last bytes, so the range of the last file in it
     * runs past its end. The FS container knows the length of the source for free, and it fails the copy before it copies a byte.
     */
    public void testCommitBlobTooShortFailsTheShardSnapshot() throws IOException {
        final var shardAndRepository = new ShardAndRepository();
        try {
            shardAndRepository.indexSegments(between(1, 3));
            final Map<String, SnapshotShardContext.CopySource> sources = shardAndRepository.writeCommitBlobs(
                between(1, 3),
                name -> true,
                between(1, 16)
            );
            final Snapshot snapshot = shardAndRepository.newSnapshot();
            final Exception e = expectThrows(Exception.class, () -> shardAndRepository.snapshot(snapshot, sources::get));
            final IOException failure = (IOException) ExceptionsHelper.unwrap(e, IOException.class);
            assertThat(e.toString(), failure, notNullValue());
            assertThat(failure.getMessage(), containsString("the source is ["));
            assertThat(failure.getMessage(), containsString("bytes long, but the caller needs ["));
            shardAndRepository.assertFailedSnapshotLeftNoData(snapshot);
        } finally {
            shardAndRepository.close();
        }
    }

    /**
     * A commit blob that is not in the object store fails its copy call with a {@link NoSuchFileException}, without a listing before it.
     * The failure fails the shard snapshot, which deletes the copies that it made and writes no {@code snap-<uuid>.dat} blob.
     */
    public void testMissingCommitBlobFailsTheShardSnapshot() throws IOException {
        final var shardAndRepository = new ShardAndRepository();
        try {
            shardAndRepository.indexSegments(between(1, 3));
            final Map<String, SnapshotShardContext.CopySource> sources = shardAndRepository.writeCommitBlobs(
                between(1, 3),
                name -> true,
                0
            );
            // One of the commit blobs is gone from the object store.
            final String missingBlob = randomFrom(sources.values()).blobName();
            shardAndRepository.objectStore.deleteBlobsIgnoringIfNotExists(OperationPurpose.SNAPSHOT_DATA, Iterators.single(missingBlob));
            final Snapshot snapshot = shardAndRepository.newSnapshot();
            final Exception e = expectThrows(Exception.class, () -> shardAndRepository.snapshot(snapshot, sources::get));
            assertThat(e.toString(), ExceptionsHelper.unwrap(e, NoSuchFileException.class), notNullValue());
            shardAndRepository.assertFailedSnapshotLeftNoData(snapshot);
        } finally {
            shardAndRepository.close();
        }
    }

    /**
     * A copy call that fails, here with an {@link IOException} as a cloud client throws it, fails the shard snapshot. No other copy
     * makes the shard snapshot succeed. The shard snapshot writes no {@code snap-<uuid>.dat} blob, and its failure cleanup deletes the
     * copies that did work.
     */
    public void testFailedCopyFailsTheShardSnapshotBeforeItIsWritten() throws IOException {
        final AtomicInteger copyCalls = new AtomicInteger();
        // the copy that fails is one of them, or all of them, chosen when the number of copies is known
        final AtomicInteger failingCall = new AtomicInteger();
        final boolean failAll = randomBoolean();
        final var shardAndRepository = new ShardAndRepository(
            container -> new FailingCopies(container, copyCalls, call -> failAll || call == failingCall.get())
        );
        try {
            shardAndRepository.indexSegments(between(1, 3));
            final Map<String, SnapshotShardContext.CopySource> sources = shardAndRepository.writeCommitBlobs(
                between(1, 3),
                name -> true,
                0
            );
            final int copies = Math.toIntExact(sources.values().stream().map(SnapshotShardContext.CopySource::blobName).distinct().count());
            failingCall.set(between(1, copies));
            final Snapshot snapshot = shardAndRepository.newSnapshot();
            final Exception e = expectThrows(Exception.class, () -> shardAndRepository.snapshot(snapshot, sources::get));
            assertThat(e.toString(), ExceptionsHelper.unwrap(e, IOException.class), notNullValue());
            assertThat(copyCalls.get(), greaterThanOrEqualTo(1));
            shardAndRepository.assertFailedSnapshotLeftNoData(snapshot);
        } finally {
            shardAndRepository.close();
        }
    }

    /**
     * A copy call can return no length, as an S3 CopyObject request does. The line of the copy stats says how many of the new copies
     * have an exact length. Copied bytes and dead bytes count only those copies, and the needed ends of all the new copies add up to a
     * lower bound of the bytes that the snapshot copied.
     */
    public void testStatsCountOnlyTheCopiesWithAnExactLength() throws IOException {
        final var hiddenLengths = new HashSet<String>();
        final var shardAndRepository = new ShardAndRepository(container -> new HidingLengthOfCopies(container, hiddenLengths::contains));
        try {
            shardAndRepository.indexSegments(between(2, 4));
            final Map<String, SnapshotShardContext.CopySource> sources = shardAndRepository.writeCommitBlobs(
                between(2, 4),
                name -> true,
                0
            );
            // at least one commit blob has an exact length, and at least one has none
            final List<String> blobNames = sources.values().stream().map(SnapshotShardContext.CopySource::blobName).distinct().toList();
            assertThat(blobNames.size(), greaterThanOrEqualTo(2));
            final List<String> hidden = randomNonEmptySubsetOf(blobNames.subList(1, blobNames.size()));
            hiddenLengths.addAll(hidden);

            final Map<String, BlobMetadata> objectStoreBlobs = shardAndRepository.objectStore.listBlobs(OperationPurpose.SNAPSHOT_METADATA);
            final Map<String, Long> fileBytes = new HashMap<>();
            final Map<String, Long> neededEnds = new HashMap<>();
            for (Map.Entry<String, SnapshotShardContext.CopySource> entry : sources.entrySet()) {
                final String blobName = entry.getValue().blobName();
                final long fileLength = shardAndRepository.fileLength(entry.getKey());
                fileBytes.merge(blobName, fileLength, Long::sum);
                neededEnds.merge(blobName, entry.getValue().offset() + fileLength, Math::max);
            }
            long copiedBytes = 0;
            final List<Long> deadBytes = new ArrayList<>();
            for (String blobName : blobNames) {
                if (hidden.contains(blobName) == false) {
                    copiedBytes += objectStoreBlobs.get(blobName).length();
                    deadBytes.add(objectStoreBlobs.get(blobName).length() - fileBytes.get(blobName));
                }
            }
            deadBytes.sort(Comparator.reverseOrder());

            final Snapshot snapshot = shardAndRepository.newSnapshot();
            assertCopyStatsLogged(
                () -> shardAndRepository.snapshot(snapshot, sources::get),
                shardAndRepository.copyStatsPrefix(snapshot)
                    + ": new copies ["
                    + blobNames.size()
                    + "], copied bytes ["
                    + copiedBytes
                    + "], total copy time [*ms], slowest copy [*ms], files in new copies [*],"
                    + " files reused by name, uploaded [0], bytes reused by name, uploaded [0],"
                    + " files reused by name, copied [0],"
                    + " bytes reused by name, copied [0], uploaded files [0], uploaded bytes [0], dead bytes ["
                    + deadBytes.stream().mapToLong(Long::longValue).sum()
                    + "], dead bytes per new copy "
                    + deadBytes
                    + statsTail(
                        neededEnds.values().stream().mapToLong(Long::longValue).sum(),
                        blobNames.size() - hidden.size(),
                        blobNames.size()
                    )
            );
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
            assertThat(dataBlobs(shardAndRepository.shardContainer()), not(empty()));
        } finally {
            shardAndRepository.close();
        }
    }

    /**
     * An attempt that fails after it has made copies logs what it spent on copies. It logs that before the failure cleanup deletes the
     * copies.
     */
    public void testFailedAttemptLogsCopyStatsBeforeCleanup() throws IOException {
        // The last copy call to start fails. The shard snapshot fails when all the copy calls are done, so by then the other copy calls
        // have returned, and they are the copies that the attempt made.
        final AtomicInteger copyCalls = new AtomicInteger();
        final AtomicInteger lastCall = new AtomicInteger();
        final var statsLogged = new AtomicBoolean();
        final var statsLoggedBeforeCleanup = new AtomicReference<Boolean>();
        final var shardAndRepository = new ShardAndRepository(
            container -> new RunBeforeDelete(
                new FailingCopies(container, copyCalls, call -> call == lastCall.get()),
                () -> statsLoggedBeforeCleanup.compareAndSet(null, statsLogged.get())
            )
        );
        try {
            shardAndRepository.indexSegments(between(2, 3));
            final Map<String, SnapshotShardContext.CopySource> sources = shardAndRepository.writeCommitBlobs(
                between(2, 3),
                name -> true,
                0
            );
            final int copies = Math.toIntExact(sources.values().stream().map(SnapshotShardContext.CopySource::blobName).distinct().count());
            assertThat(copies, greaterThanOrEqualTo(2));
            lastCall.set(copies);
            final Snapshot snapshot = shardAndRepository.newSnapshot();
            final String message = shardAndRepository.copyStatsPrefix(snapshot)
                + ": attempt [failed], copies made ["
                + (copies - 1)
                + "], copied bytes [*], total copy time [*ms], copied bytes at least [*], exact lengths ["
                + (copies - 1)
                + " of "
                + (copies - 1)
                + "], copied bytes count only the copies made with an exact length";
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
                + ": attempt [paused], copies made [1], copied bytes [*], total copy time [*ms], copied bytes at least [*],"
                + " exact lengths [1 of 1], copied bytes count only the copies made with an exact length";
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
     * The end of a copy stats line, which comes after the dead bytes per new copy: the sum of the needed ends of the new copies, the
     * number of new copies that have an exact length out of all the new copies, and the note about what the bytes count.
     */
    private static String statsTail(long neededBytes, long exactLengths, long newCopies) {
        return ", copied bytes at least ["
            + neededBytes
            + "], exact lengths ["
            + exactLengths
            + " of "
            + newCopies
            + "], copied bytes and dead bytes count only the new copies with an exact length";
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

    /**
     * The data blobs in the shard's folder: the blobs that are named like an uploaded blob. A copy is named like an uploaded blob, so
     * the data blobs are the copies and the uploaded blobs together. A test that makes no upload can take them for the copies.
     */
    private static Set<String> dataBlobs(BlobContainer shardContainer) throws IOException {
        return dataBlobs(shardContainer.listBlobs(OperationPurpose.SNAPSHOT_METADATA).keySet());
    }

    private static Set<String> dataBlobs(Set<String> blobNames) {
        return blobNames.stream()
            .filter(name -> name.startsWith(BlobStoreRepository.UPLOADED_DATA_BLOB_PREFIX))
            .collect(Collectors.toSet());
    }

    /**
     * The names of the copied blobs that the copied files of the shard snapshot are ranges of. A file info is copied if it has an offset.
     */
    private static Set<String> copyNames(BlobStoreIndexShardSnapshot shardSnapshot) {
        return shardSnapshot.indexFiles().stream().filter(FileInfo::isCopied).map(FileInfo::name).collect(Collectors.toSet());
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
        // the calls that list blobs or folders, on the shard containers of the repository and on the object store
        private final AtomicInteger listings = new AtomicInteger();
        private IndexShard shard;
        private int commitBlobCount;
        // the identifier of the shard's state that the shard snapshots get, null if the shard snapshots have none, see the shortcut test
        @Nullable
        private String shardStateIdentifier;

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
                container -> new ListCounting(shardContainers == null ? container : shardContainers.apply(container), listings)
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
            objectStore = new ListCounting(container, listings);
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
         * filler before and between the files, and returns where each file can be copied from. With no cut there is filler after the
         * last file too, so the blob is longer than the end of its last file. {@code truncateBy} cuts that many bytes off the end of
         * each blob instead, to make its last file's range too long for it.
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
                        if (truncateBy == 0) {
                            blobBytes.writeBytes(randomByteArrayOfLength(between(0, 512)));
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
                    if (blob.getFileName().toString().startsWith(BlobStoreRepository.UPLOADED_DATA_BLOB_PREFIX)) {
                        assertThat(blob.toString(), type, equalTo(RepositoryFileType.SHARD_DATA));
                    }
                }
            }
        }

        /**
         * The shard state identifier of each snapshot that the shard generation lists, in the order of the snapshots.
         */
        List<String> shardStateIdentifiers(ShardGeneration generation) throws IOException {
            return repository.getBlobStoreIndexShardSnapshots(indexId, shardId.id(), generation)
                .snapshots()
                .stream()
                .map(SnapshotFiles::shardStateIdentifier)
                .toList();
        }

        /**
         * Reads the shard generation that a shard snapshot wrote, the {@code index-<uuid>} blob, and checks that it lists the same files as
         * the {@code snap-<uuid>.dat} blob of the snapshot, with the same names and offsets, in the same order.
         */
        void assertShardGenerationHasTheFilesOf(Snapshot snapshot, ShardGeneration generation) throws IOException {
            final List<FileInfo> expected = loadShardSnapshot(snapshot).indexFiles();
            final BlobStoreIndexShardSnapshots shardGeneration = repository.getBlobStoreIndexShardSnapshots(
                indexId,
                shardId.id(),
                generation
            );
            final SnapshotFiles snapshotFiles = shardGeneration.snapshots()
                .stream()
                .filter(files -> files.snapshot().equals(snapshot.getSnapshotId().getName()))
                .findFirst()
                .orElseThrow();
            assertThat(snapshotFiles.indexFiles().size(), equalTo(expected.size()));
            for (int i = 0; i < expected.size(); i++) {
                final FileInfo inGeneration = snapshotFiles.indexFiles().get(i);
                assertTrue(inGeneration + " is not the file info " + expected.get(i), inGeneration.isSame(expected.get(i)));
            }
            for (FileInfo fileInfo : expected) {
                assertThat(shardGeneration.findNameFile(fileInfo.name()), notNullValue());
            }
        }

        /**
         * Checks that a shard snapshot that failed left no copy, no uploaded file and no {@code snap-<uuid>.dat} blob in the shard's
         * folder. The {@code index-<uuid>} blob that a shard snapshot writes first can stay, because no generation refers to it.
         */
        void assertFailedSnapshotLeftNoData(Snapshot snapshot) throws IOException {
            final Set<String> blobs = shardContainer().listBlobs(OperationPurpose.SNAPSHOT_METADATA).keySet();
            assertThat(dataBlobs(blobs), empty());
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
            final int listingsBefore = listings.get();
            // the shard snapshot sets the generation of the status when it is done, so ask before it starts
            final boolean startsWithoutGeneration = status.generation() == null;
            try (Engine.IndexCommitRef commitRef = shard.acquireLastIndexCommit(true)) {
                final var localContext = new LocalPrimarySnapshotShardContext(
                    shard.store(),
                    shard.mapperService(),
                    snapshot.getSnapshotId(),
                    indexId,
                    new SnapshotIndexCommit(commitRef),
                    shardStateIdentifier,
                    status,
                    repositoryMetaVersion,
                    randomMillisUpToYear9999(),
                    future
                );
                repository.snapshotShard(new CopyingSnapshotShardContext(localContext, copySources, visibleFiles, reusableCopies));
                return future.actionGet().getGeneration();
            } finally {
                // A shard snapshot lists neither the commit blobs before it copies them, nor the copies after. The only listing that it
                // makes is the one of the shard's folder that finds the latest shard generation when the repository gives none, which is
                // the case for the first shard snapshot of a shard. A repository with numeric shard generations lists more.
                if (SnapshotsServiceUtils.useShardGenerations(repositoryMetaVersion)) {
                    assertThat(
                        "the shard snapshot listed blobs other than to find the latest shard generation",
                        listings.get() - listingsBefore,
                        equalTo(startsWithoutGeneration ? 1 : 0)
                    );
                }
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
     * A container that counts its calls that list blobs or folders in {@code listings}.
     */
    private static final class ListCounting extends FilterBlobContainer {
        private final AtomicInteger listings;

        ListCounting(BlobContainer delegate, AtomicInteger listings) {
            super(delegate);
            this.listings = listings;
        }

        @Override
        public Map<String, BlobMetadata> listBlobs(OperationPurpose purpose) throws IOException {
            listings.incrementAndGet();
            return super.listBlobs(purpose);
        }

        @Override
        public Map<String, BlobMetadata> listBlobsByPrefix(OperationPurpose purpose, String blobNamePrefix) throws IOException {
            listings.incrementAndGet();
            return super.listBlobsByPrefix(purpose, blobNamePrefix);
        }

        @Override
        public Map<String, BlobContainer> children(OperationPurpose purpose) throws IOException {
            listings.incrementAndGet();
            return super.children(purpose);
        }

        @Override
        protected BlobContainer wrapChild(BlobContainer child) {
            return new ListCounting(child, listings);
        }
    }

    /**
     * A shard container whose copy calls fail with an {@link IOException}, as a cloud client throws it, when {@code fails} accepts the
     * number of the call. The calls are counted from 1 in {@code copyCalls}, and the failed ones copy nothing.
     */
    private static final class FailingCopies extends FilterBlobContainer {
        private final AtomicInteger copyCalls;
        private final IntPredicate fails;

        FailingCopies(BlobContainer delegate, AtomicInteger copyCalls, IntPredicate fails) {
            super(delegate);
            this.copyCalls = copyCalls;
            this.fails = fails;
        }

        @Override
        public OptionalLong copyBlob(
            OperationPurpose purpose,
            BlobContainer sourceBlobContainer,
            String sourceBlobName,
            String blobName,
            long neededEnd,
            @Nullable Executor executor
        ) throws IOException {
            if (fails.test(copyCalls.incrementAndGet())) {
                throw new IOException("simulated failure of the copy of [" + sourceBlobName + "]");
            }
            return super.copyBlob(purpose, sourceBlobContainer, sourceBlobName, blobName, neededEnd, executor);
        }

        @Override
        protected BlobContainer wrapChild(BlobContainer child) {
            return new FailingCopies(child, copyCalls, fails);
        }
    }

    /**
     * A shard container whose copy calls return no length for the commit blobs that {@code hidden} accepts, as an S3 CopyObject request
     * does. The copy itself works.
     */
    private static final class HidingLengthOfCopies extends FilterBlobContainer {
        private final Predicate<String> hidden;

        HidingLengthOfCopies(BlobContainer delegate, Predicate<String> hidden) {
            super(delegate);
            this.hidden = hidden;
        }

        @Override
        public OptionalLong copyBlob(
            OperationPurpose purpose,
            BlobContainer sourceBlobContainer,
            String sourceBlobName,
            String blobName,
            long neededEnd,
            @Nullable Executor executor
        ) throws IOException {
            final OptionalLong length = super.copyBlob(purpose, sourceBlobContainer, sourceBlobName, blobName, neededEnd, executor);
            return hidden.test(sourceBlobName) ? OptionalLong.empty() : length;
        }

        @Override
        protected BlobContainer wrapChild(BlobContainer child) {
            return new HidingLengthOfCopies(child, hidden);
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
        public OptionalLong copyBlob(
            OperationPurpose purpose,
            BlobContainer sourceBlobContainer,
            String sourceBlobName,
            String blobName,
            long neededEnd,
            @Nullable Executor executor
        ) throws IOException {
            final OptionalLong length = super.copyBlob(purpose, sourceBlobContainer, sourceBlobName, blobName, neededEnd, executor);
            status.get().pauseIfNotCompleted(notifyOnPaused -> notifyOnPaused.onResponse(() -> {}));
            return length;
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
