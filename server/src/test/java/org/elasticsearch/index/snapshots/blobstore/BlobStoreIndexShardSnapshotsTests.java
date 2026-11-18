/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.snapshots.blobstore;

import org.apache.lucene.util.Version;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Tests how a shard generation, the {@code index-<uuid>} blob of a shard, tracks files that are stored as byte ranges of copied blobs.
 * All the files of one copied blob have the name of that blob, and only their offsets tell them apart.
 */
public class BlobStoreIndexShardSnapshotsTests extends ESTestCase {

    private static final String FIRST_COPY = BlobStoreRepository.UPLOADED_DATA_BLOB_PREFIX + "firstCopyOfACommitBlob";
    private static final String SECOND_COPY = BlobStoreRepository.UPLOADED_DATA_BLOB_PREFIX + "secondCopyOfACommitBlob";

    public void testFilesThatShareANameAreDifferentEntries() {
        final FileInfo first = copiedFile("_0.cfs", FIRST_COPY, 100L);
        final FileInfo second = copiedFile("_1.cfs", FIRST_COPY, 5000L);
        final FileInfo uploaded = uploadedFile("_2.cfs");

        final BlobStoreIndexShardSnapshots generation = BlobStoreIndexShardSnapshots.EMPTY.withAddedSnapshot(
            new SnapshotFiles("snap-1", List.of(first, second, uploaded), null)
        );

        // the lookup by the Lucene file name finds the right range of the shared blob for each file, and not the last one that was added
        assertThat(generation.findPhysicalIndexFile(first.metadata()), sameInstance(first));
        assertThat(generation.findPhysicalIndexFile(second.metadata()), sameInstance(second));
        assertThat(generation.findPhysicalIndexFile(uploaded.metadata()), sameInstance(uploaded));
        assertThat(generation.snapshots().get(0).indexFiles(), equalTo(List.of(first, second, uploaded)));
    }

    /**
     * A file can have a copied and an uploaded file info, after a repository switched between copying and uploading. A file info that
     * the caller cannot use must not hide one that it can, whichever of the two comes first. With no restriction the lookup finds the file
     * info that the oldest snapshot lists first, and it finds the same one every time, whatever the identity hashes of the file infos are.
     */
    public void testFindPhysicalIndexFileSkipsFileInfosThatAreNotCandidates() throws IOException {
        final StoreFileMetadata metadata = metadata("_0_1.liv");
        final FileInfo copied = new FileInfo(FIRST_COPY, metadata, 100L);
        final FileInfo uploaded = new FileInfo(BlobStoreRepository.UPLOADED_DATA_BLOB_PREFIX + randomAlphaOfLength(10), metadata, null);
        // the same name and length with another checksum is another file, which no file info matches
        final StoreFileMetadata otherFile = new StoreFileMetadata(
            metadata.name(),
            metadata.length(),
            metadata.checksum() + "x",
            metadata.writtenBy()
        );

        for (boolean copiedFirst : new boolean[] { true, false }) {
            BlobStoreIndexShardSnapshots generation = BlobStoreIndexShardSnapshots.EMPTY.withAddedSnapshot(
                new SnapshotFiles("snap-1", List.of(copiedFirst ? copied : uploaded), null)
            ).withAddedSnapshot(new SnapshotFiles("snap-2", List.of(copiedFirst ? uploaded : copied), null));
            // Reading the shard generation again makes new file info objects, so each round has other identity hashes.
            for (int round = 0; round < 10; round++) {
                generation = roundTrip(generation);
                assertThat(generation.findPhysicalIndexFile(metadata).isCopied(), equalTo(copiedFirst));
                assertThat(generation.findPhysicalIndexFile(metadata, fileInfo -> true).isCopied(), equalTo(copiedFirst));
                assertFalse(generation.findPhysicalIndexFile(metadata, fileInfo -> fileInfo.isCopied() == false).isCopied());
                assertTrue(generation.findPhysicalIndexFile(metadata, FileInfo::isCopied).isCopied());
                assertThat(generation.findPhysicalIndexFile(metadata, fileInfo -> false), nullValue());
                assertThat(generation.findPhysicalIndexFile(otherFile, fileInfo -> true), nullValue());
            }
        }
    }

    public void testFindNameFileFindsAnyFileWithThatName() {
        final FileInfo first = copiedFile("_0.cfs", FIRST_COPY, 100L);
        final FileInfo second = copiedFile("_1.cfs", FIRST_COPY, 5000L);
        final FileInfo inSecondCopy = copiedFile("_2.cfs", SECOND_COPY, 100L);
        final FileInfo uploaded = uploadedFile("_3.cfs");

        final BlobStoreIndexShardSnapshots generation = BlobStoreIndexShardSnapshots.EMPTY.withAddedSnapshot(
            new SnapshotFiles("snap-1", List.of(first, second), null)
        ).withAddedSnapshot(new SnapshotFiles("snap-2", List.of(second, inSecondCopy, uploaded), null));

        assertThat(generation.findNameFile(FIRST_COPY), either(sameInstance(first)).or(sameInstance(second)));
        assertThat(generation.findNameFile(FIRST_COPY).name(), equalTo(FIRST_COPY));
        // asking again gives the same answer, and the lookup is the same for each name
        assertThat(generation.findNameFile(FIRST_COPY), sameInstance(generation.findNameFile(FIRST_COPY)));
        assertThat(generation.findNameFile(SECOND_COPY), sameInstance(inSecondCopy));
        assertThat(generation.findNameFile(uploaded.name()), sameInstance(uploaded));
        assertThat(generation.findNameFile(BlobStoreRepository.UPLOADED_DATA_BLOB_PREFIX + "unknown"), nullValue());
        assertThat(BlobStoreIndexShardSnapshots.EMPTY.findNameFile(FIRST_COPY), nullValue());
    }

    /**
     * Snapshot delete removes a blob when no file of the retained snapshots has the name of the blob, see
     * {@link BlobStoreIndexShardSnapshots#findNameFile}. A copied blob stays as long as any file of any retained snapshot is in it.
     */
    public void testRetainedSnapshotsKeepABlobThatASurvivingFileShares() {
        final FileInfo inFirstOnly = copiedFile("_0.cfs", FIRST_COPY, 100L);
        final FileInfo inFirstAndLater = copiedFile("_1.cfs", FIRST_COPY, 5000L);
        final FileInfo inSecondCopy = copiedFile("_2.cfs", SECOND_COPY, 100L);
        final SnapshotId snap1 = new SnapshotId("snap-1", "uuid-1");
        final SnapshotId snap2 = new SnapshotId("snap-2", "uuid-2");

        final BlobStoreIndexShardSnapshots generation = BlobStoreIndexShardSnapshots.EMPTY.withAddedSnapshot(
            new SnapshotFiles(snap1.getName(), List.of(inFirstOnly, inFirstAndLater), null)
        ).withAddedSnapshot(new SnapshotFiles(snap2.getName(), List.of(inFirstAndLater, inSecondCopy), null));

        // deleting snap-1 keeps the first copy, because snap-2 still has a file in it, although the file that snap-1 used alone is gone
        final BlobStoreIndexShardSnapshots afterDeletingFirst = generation.withRetainedSnapshots(Set.of(snap2));
        assertThat(afterDeletingFirst.findNameFile(FIRST_COPY), sameInstance(inFirstAndLater));
        assertThat(afterDeletingFirst.findNameFile(SECOND_COPY), sameInstance(inSecondCopy));
        assertThat(afterDeletingFirst.findPhysicalIndexFile(inFirstOnly.metadata()), nullValue());

        // deleting snap-2 removes the only reference to the second copy, but keeps the first, because snap-1 has two files in it
        final BlobStoreIndexShardSnapshots afterDeletingSecond = generation.withRetainedSnapshots(Set.of(snap1));
        assertThat(afterDeletingSecond.findNameFile(FIRST_COPY).name(), equalTo(FIRST_COPY));
        assertThat(afterDeletingSecond.findNameFile(SECOND_COPY), nullValue());

        // deleting every snapshot leaves nothing that refers to a blob
        assertThat(generation.withRetainedSnapshots(Set.of()).findNameFile(FIRST_COPY), nullValue());
    }

    public void testRetainedSnapshotsKeepFilesThatShareANameInDifferentSnapshots() {
        // two snapshots hold different files of one copied blob, and no file info is in both
        final FileInfo inSnap1 = copiedFile("_0.cfs", FIRST_COPY, 100L);
        final FileInfo inSnap2 = copiedFile("_1.cfs", FIRST_COPY, 5000L);
        final SnapshotId snap1 = new SnapshotId("snap-1", "uuid-1");
        final SnapshotId snap2 = new SnapshotId("snap-2", "uuid-2");
        final BlobStoreIndexShardSnapshots generation = BlobStoreIndexShardSnapshots.EMPTY.withAddedSnapshot(
            new SnapshotFiles(snap1.getName(), List.of(inSnap1), null)
        ).withAddedSnapshot(new SnapshotFiles(snap2.getName(), List.of(inSnap2), null));

        final BlobStoreIndexShardSnapshots onlySnap1 = generation.withRetainedSnapshots(Set.of(snap1));
        assertThat(onlySnap1.findNameFile(FIRST_COPY), sameInstance(inSnap1));
        assertThat(onlySnap1.findPhysicalIndexFile(inSnap2.metadata()), nullValue());
        final BlobStoreIndexShardSnapshots onlySnap2 = generation.withRetainedSnapshots(Set.of(snap2));
        assertThat(onlySnap2.findNameFile(FIRST_COPY), sameInstance(inSnap2));
        assertThat(onlySnap2.findPhysicalIndexFile(inSnap1.metadata()), nullValue());
        assertThat(onlySnap2.snapshots().get(0).indexFiles(), equalTo(List.of(inSnap2)));
    }

    public void testAddedSnapshotMustAgreeOnTheFileAtTheSameNameAndOffset() {
        final FileInfo original = copiedFile("_0.cfs", FIRST_COPY, between(0, 4096));
        final BlobStoreIndexShardSnapshots generation = BlobStoreIndexShardSnapshots.EMPTY.withAddedSnapshot(
            new SnapshotFiles("snap-1", List.of(original), null)
        );

        // another file at another offset of the same blob is another entry, and is fine
        final FileInfo another = new FileInfo(FIRST_COPY, metadata("_1.cfs"), original.offset() + 1000L);
        final BlobStoreIndexShardSnapshots withAnother = generation.withAddedSnapshot(new SnapshotFiles("snap-2", List.of(another), null));
        assertThat(withAnother.findPhysicalIndexFile(original.metadata()), sameInstance(original));
        assertThat(withAnother.findPhysicalIndexFile(another.metadata()), sameInstance(another));

        // a different file at the same name and offset is a bug, which the assertion catches
        final FileInfo clash = new FileInfo(FIRST_COPY, metadata("_1.cfs"), original.offset());
        expectThrows(AssertionError.class, () -> generation.withAddedSnapshot(new SnapshotFiles("snap-2", List.of(clash), null)));

        // an equal file info in another instance is the same file, so adding and retaining snapshots with it is fine
        final FileInfo equal = new FileInfo(FIRST_COPY, original.metadata(), original.offset());
        final BlobStoreIndexShardSnapshots withBoth = generation.withAddedSnapshot(new SnapshotFiles("snap-2", List.of(equal), null));
        final BlobStoreIndexShardSnapshots retained = withBoth.withRetainedSnapshots(
            Set.of(new SnapshotId("snap-1", "uuid-1"), new SnapshotId("snap-2", "uuid-2"))
        );
        assertThat(retained.findNameFile(FIRST_COPY).name(), equalTo(FIRST_COPY));
        assertThat(retained.snapshots().size(), equalTo(2));
    }

    /**
     * The clone of a snapshot has the same file list, and a clone that is retried compares the list that is already there with the list
     * of the source, see {@link SnapshotFiles#isSame}.
     */
    public void testCloneRetryComparesOffsets() throws IOException {
        final FileInfo first = copiedFile("_0.cfs", FIRST_COPY, 100L);
        final FileInfo second = copiedFile("_1.cfs", FIRST_COPY, 5000L);
        final FileInfo uploaded = uploadedFile("_2.cfs");
        final String shardStateId = randomAlphaOfLength(10);
        final SnapshotFiles source = new SnapshotFiles("snap-1", List.of(first, second, uploaded), shardStateId);

        final BlobStoreIndexShardSnapshots generation = BlobStoreIndexShardSnapshots.EMPTY.withAddedSnapshot(source)
            .withClone("snap-1", "clone-1");
        final SnapshotFiles clone = generation.snapshots().get(1);
        assertThat(clone.snapshot(), equalTo("clone-1"));
        assertThat(clone.indexFiles(), equalTo(source.indexFiles()));
        assertTrue(clone.isSame(source));

        // the same is true after the generation went through the repository
        final BlobStoreIndexShardSnapshots reread = roundTrip(generation);
        assertTrue(reread.snapshots().get(1).isSame(reread.snapshots().get(0)));
        assertTrue(reread.snapshots().get(1).isSame(source));

        // a list with the same names, where one file is at another offset, is not the same list
        final FileInfo shifted = new FileInfo(FIRST_COPY, second.metadata(), second.offset() + 1);
        assertFalse(new SnapshotFiles("clone-1", List.of(first, shifted, uploaded), shardStateId).isSame(source));
        // nor is a list in which a file is stored in a blob of its own instead of in the copied blob
        final FileInfo notCopied = new FileInfo(FIRST_COPY, second.metadata(), null);
        assertFalse(new SnapshotFiles("clone-1", List.of(first, notCopied, uploaded), shardStateId).isSame(source));
    }

    public void testXContentRoundTripKeepsTwoFilesThatShareAName() throws IOException {
        final FileInfo first = copiedFile("_0.cfs", FIRST_COPY, 100L);
        final FileInfo second = copiedFile("_1.cfs", FIRST_COPY, 5000L);
        final BlobStoreIndexShardSnapshots generation = BlobStoreIndexShardSnapshots.EMPTY.withAddedSnapshot(
            new SnapshotFiles("snap-1", List.of(first, second), randomBoolean() ? null : randomAlphaOfLength(10))
        );

        final BlobStoreIndexShardSnapshots parsed = roundTrip(generation);

        assertSameSnapshots(generation, parsed);
        final List<FileInfo> parsedFiles = parsed.snapshots().get(0).indexFiles();
        assertThat(parsedFiles.size(), equalTo(2));
        assertThat(parsedFiles.get(0).name(), equalTo(FIRST_COPY));
        assertThat(parsedFiles.get(0).offset(), equalTo(100L));
        assertThat(parsedFiles.get(0).physicalName(), equalTo("_0.cfs"));
        assertThat(parsedFiles.get(1).name(), equalTo(FIRST_COPY));
        assertThat(parsedFiles.get(1).offset(), equalTo(5000L));
        assertThat(parsedFiles.get(1).physicalName(), equalTo("_1.cfs"));
        // the file infos that the list refers to are the ones of the "files" array, so the lookup by Lucene file name finds them
        assertThat(parsed.findPhysicalIndexFile(first.metadata()), sameInstance(parsedFiles.get(0)));
        assertThat(parsed.findPhysicalIndexFile(second.metadata()), sameInstance(parsedFiles.get(1)));
    }

    /**
     * The unchanged-shard shortcut of a shard snapshot takes the file list of an earlier snapshot that has the same shard state
     * identifier, from the parsed shard generation, and writes the same list for the new snapshot. This is what that takes, over a list
     * whose files share names.
     */
    public void testShortcutOverAListWithSharedNames() throws IOException {
        final String shardStateId = randomAlphaOfLength(10);
        final List<FileInfo> files = List.of(
            copiedFile("_0.cfs", FIRST_COPY, 100L),
            uploadedFile("_1.cfs"),
            copiedFile("_2.cfs", FIRST_COPY, 5000L),
            copiedFile("_3.cfs", SECOND_COPY, 100L)
        );
        BlobStoreIndexShardSnapshots generation = BlobStoreIndexShardSnapshots.EMPTY.withAddedSnapshot(
            new SnapshotFiles("snap-1", files, shardStateId)
        );

        for (int i = 2; i <= 3; i++) {
            // each shortcut reads the shard generation that the earlier one wrote
            generation = roundTrip(generation);
            final List<FileInfo> filesOfSameState = findFilesOfSameShardState(generation, shardStateId);
            assertThat(filesOfSameState, equalTo(generation.snapshots().get(0).indexFiles()));
            generation = generation.withAddedSnapshot(new SnapshotFiles("snap-" + i, filesOfSameState, shardStateId));
        }

        generation = roundTrip(generation);
        assertThat(generation.snapshots().size(), equalTo(3));
        for (SnapshotFiles snapshotFiles : generation) {
            assertThat(snapshotFiles.shardStateIdentifier(), equalTo(shardStateId));
            assertThat(snapshotFiles.indexFiles().size(), equalTo(files.size()));
            for (int i = 0; i < files.size(); i++) {
                assertTrue(
                    snapshotFiles.snapshot() + " lost [" + files.get(i) + "]",
                    snapshotFiles.indexFiles().get(i).isSame(files.get(i))
                );
            }
        }
    }

    public void testListsWithoutCopiedFilesAreWrittenAsPlainNames() throws IOException {
        final FileInfo first = uploadedFile("_0.cfs");
        final FileInfo second = uploadedFile("_1.cfs");
        final BlobStoreIndexShardSnapshots generation = BlobStoreIndexShardSnapshots.EMPTY.withAddedSnapshot(
            new SnapshotFiles("snap-1", List.of(first, second), null)
        ).withAddedSnapshot(new SnapshotFiles("snap-2", List.of(second), null));

        final Map<String, Object> written = toMap(generation);

        // the file lists are plain strings, as they were written before there were copied files, and no file info has an offset
        @SuppressWarnings("unchecked")
        final Map<String, Object> snapshots = (Map<String, Object>) written.get("snapshots");
        assertThat(snapshotFileList(snapshots, "snap-1"), equalTo(List.of(first.name(), second.name())));
        assertThat(snapshotFileList(snapshots, "snap-2"), equalTo(List.of(second.name())));
        @SuppressWarnings("unchecked")
        final List<Map<String, Object>> fileInfos = (List<Map<String, Object>>) written.get("files");
        assertThat(fileInfos.size(), equalTo(2));
        for (Map<String, Object> fileInfo : fileInfos) {
            assertFalse(fileInfo.containsKey("offset"));
        }
        assertSameSnapshots(generation, roundTrip(generation));
    }

    public void testListsWithCopiedFilesAreMixedArrays() throws IOException {
        final FileInfo uploaded = uploadedFile("_0.cfs");
        final FileInfo first = copiedFile("_1.cfs", FIRST_COPY, 100L);
        final FileInfo second = copiedFile("_2.cfs", FIRST_COPY, 0L);
        final BlobStoreIndexShardSnapshots generation = BlobStoreIndexShardSnapshots.EMPTY.withAddedSnapshot(
            new SnapshotFiles("snap-1", List.of(first, uploaded, second), null)
        );

        final Map<String, Object> written = toMap(generation);

        // a file that is not copied is a plain name, and a copied file is an object with the name of the blob and the offset
        @SuppressWarnings("unchecked")
        final Map<String, Object> snapshots = (Map<String, Object>) written.get("snapshots");
        assertThat(
            snapshotFileList(snapshots, "snap-1"),
            equalTo(List.of(Map.of("name", FIRST_COPY, "offset", 100), uploaded.name(), Map.of("name", FIRST_COPY, "offset", 0)))
        );
        // the file infos in the "files" array have the offset as well
        @SuppressWarnings("unchecked")
        final List<Map<String, Object>> fileInfos = (List<Map<String, Object>>) written.get("files");
        assertThat(fileInfos.size(), equalTo(3));
        assertThat(fileInfos.stream().filter(fileInfo -> fileInfo.containsKey("offset")).count(), equalTo(2L));

        final BlobStoreIndexShardSnapshots parsed = roundTrip(generation);
        assertSameSnapshots(generation, parsed);
        assertThat(parsed.snapshots().get(0).indexFiles().get(0).offset(), equalTo(100L));
        assertFalse(parsed.snapshots().get(0).indexFiles().get(1).isCopied());
        assertThat(parsed.snapshots().get(0).indexFiles().get(2).offset(), equalTo(0L));
    }

    /**
     * A shard generation that an earlier release wrote has only plain names in its file lists. It must read as before.
     */
    public void testReadsAShardGenerationWithPlainNamesOnly() throws IOException {
        final String json = """
            {
              "files": [
                {"name": "__0", "physical_name": "_0.cfs", "length": 100, "checksum": "abc", "written_by": "%1$s"},
                {"name": "__1", "physical_name": "_1.cfs", "length": 200, "checksum": "def", "written_by": "%1$s"}
              ],
              "snapshots": {
                "snap-1": {"files": ["__0", "__1"], "shard_state_id": "state"},
                "snap-2": {"files": ["__1"]}
              }
            }
            """;

        final BlobStoreIndexShardSnapshots parsed = parse(Strings.format(json, Version.LATEST));

        assertThat(parsed.snapshots().size(), equalTo(2));
        final SnapshotFiles snap1 = parsed.snapshots().get(0);
        assertThat(snap1.snapshot(), equalTo("snap-1"));
        assertThat(snap1.shardStateIdentifier(), equalTo("state"));
        assertThat(snap1.indexFiles().stream().map(FileInfo::name).toList(), equalTo(List.of("__0", "__1")));
        assertThat(snap1.indexFiles().stream().map(FileInfo::isCopied).toList(), equalTo(List.of(false, false)));
        final SnapshotFiles snap2 = parsed.snapshots().get(1);
        assertThat(snap2.shardStateIdentifier(), nullValue());
        assertThat(snap2.indexFiles(), equalTo(List.of(snap1.indexFiles().get(1))));
        assertThat(parsed.findNameFile("__0"), sameInstance(snap1.indexFiles().get(0)));
    }

    public void testReadsAMixedShardGeneration() throws IOException {
        final String json = """
            {
              "files": [
                {"name": "__0", "physical_name": "_0.cfs", "length": 100, "checksum": "abc", "written_by": "%1$s"},
                {"name": "__7", "physical_name": "_1.cfs", "length": 200, "checksum": "def", "written_by": "%1$s", "offset": 900},
                {"name": "__7", "physical_name": "_2.cfs", "length": 300, "checksum": "ghi", "written_by": "%1$s", "offset": 5000}
              ],
              "snapshots": {
                "snap-1": {"files": ["__0", {"name": "__7", "offset": 900}, {"offset": 5000, "name": "__7"}]},
                "snap-2": {"files": [{"name": "__7", "offset": 5000}]}
              }
            }
            """;

        final BlobStoreIndexShardSnapshots parsed = parse(Strings.format(json, Version.LATEST));

        final List<FileInfo> snap1 = parsed.snapshots().get(0).indexFiles();
        assertThat(snap1.stream().map(FileInfo::physicalName).toList(), equalTo(List.of("_0.cfs", "_1.cfs", "_2.cfs")));
        assertThat(snap1.stream().map(FileInfo::name).toList(), equalTo(List.of("__0", "__7", "__7")));
        assertFalse(snap1.get(0).isCopied());
        assertThat(snap1.get(1).offset(), equalTo(900L));
        assertThat(snap1.get(2).offset(), equalTo(5000L));
        assertThat(parsed.snapshots().get(1).indexFiles(), equalTo(List.of(snap1.get(2))));
    }

    public void testRejectsAReferenceToAFileThatIsNotThere() {
        final String json = """
            {
              "files": [
                {"name": "__7", "physical_name": "_1.cfs", "length": 200, "checksum": "def", "written_by": "%1$s", "offset": 900}
              ],
              "snapshots": {
                "snap-1": {"files": [%2$s]}
              }
            }
            """;
        // the name is there, but at another offset, or without any offset, or the name is not there
        for (String reference : List.of("{\"name\": \"__7\", \"offset\": 901}", "\"__7\"", "{\"name\": \"__8\", \"offset\": 900}")) {
            final String text = Strings.format(json, Version.LATEST, reference);
            try (var ignored = new BlobStoreIndexShardSnapshotsIntegritySuppressor()) {
                final IllegalStateException e = expectThrows(IllegalStateException.class, () -> parse(text));
                assertThat(e.getMessage(), containsString("shard index inconsistent at file"));
            }
        }
    }

    public void testRejectsAMalformedReferenceToACopiedFile() {
        final String json = """
            {
              "files": [
                {"name": "__7", "physical_name": "_1.cfs", "length": 200, "checksum": "def", "written_by": "%1$s", "offset": 900}
              ],
              "snapshots": {
                "snap-1": {"files": [%2$s]}
              }
            }
            """;
        for (String reference : List.of(
            "{\"name\": \"__7\"}",
            "{\"offset\": 900}",
            "{}",
            "{\"name\": \"__7\", \"offset\": 900, \"source\": \"indices/a/0/1/stateless_commit_1\"}",
            "{\"name\": \"__7\", \"offset\": [900]}"
        )) {
            final String text = Strings.format(json, Version.LATEST, reference);
            expectThrows(Exception.class, () -> parse(text));
        }
        // an object that lacks a name or an offset gets a message that says so
        final String text = Strings.format(json, Version.LATEST, "{\"name\": \"__7\"}");
        final ElasticsearchParseException e = expectThrows(ElasticsearchParseException.class, () -> parse(text));
        assertThat(e.getMessage(), containsString("needs both a name and an offset"));
    }

    public void testRejectsANegativeOffsetInAReferenceToACopiedFile() throws IOException {
        final String json = """
            {
              "files": [
                {"name": "__7", "physical_name": "_1.cfs", "length": 200, "checksum": "def", "written_by": "%1$s", "offset": 900}
              ],
              "snapshots": {
                "snap-1": {"files": [{"name": "__7", "offset": %2$d}]}
              }
            }
            """;
        // a file info that has a negative offset is rejected when the file infos are read, and so is a reference to one
        final long negativeOffset = -randomLongBetween(1, Long.MAX_VALUE);
        final String text = Strings.format(json, Version.LATEST, negativeOffset);
        final ElasticsearchParseException e = expectThrows(ElasticsearchParseException.class, () -> parse(text));
        assertThat(e.getMessage(), containsString("invalid offset [" + negativeOffset + "] for file [__7]"));

        // an offset of 0 and a positive offset are not negative, so the same reference is read when the offset is the file's offset
        final BlobStoreIndexShardSnapshots parsed = parse(Strings.format(json, Version.LATEST, 900));
        assertThat(parsed.snapshots().get(0).indexFiles().size(), equalTo(1));
        assertThat(parsed.snapshots().get(0).indexFiles().get(0).offset(), equalTo(900L));
    }

    private static List<FileInfo> findFilesOfSameShardState(BlobStoreIndexShardSnapshots generation, String shardStateId) {
        for (SnapshotFiles snapshotFiles : generation.snapshots()) {
            if (shardStateId.equals(snapshotFiles.shardStateIdentifier())) {
                return snapshotFiles.indexFiles();
            }
        }
        throw new AssertionError("no snapshot with shard state identifier [" + shardStateId + "]");
    }

    @SuppressWarnings("unchecked")
    private static List<Object> snapshotFileList(Map<String, Object> snapshots, String snapshotName) {
        return (List<Object>) ((Map<String, Object>) snapshots.get(snapshotName)).get("files");
    }

    private static void assertSameSnapshots(BlobStoreIndexShardSnapshots expected, BlobStoreIndexShardSnapshots actual) {
        assertThat(actual.snapshots().size(), equalTo(expected.snapshots().size()));
        for (int s = 0; s < expected.snapshots().size(); s++) {
            final SnapshotFiles expectedSnapshot = expected.snapshots().get(s);
            final SnapshotFiles actualSnapshot = actual.snapshots().get(s);
            assertThat(actualSnapshot.snapshot(), equalTo(expectedSnapshot.snapshot()));
            assertThat(actualSnapshot.shardStateIdentifier(), equalTo(expectedSnapshot.shardStateIdentifier()));
            assertThat(actualSnapshot.indexFiles().size(), equalTo(expectedSnapshot.indexFiles().size()));
            for (int f = 0; f < expectedSnapshot.indexFiles().size(); f++) {
                final FileInfo expectedFile = expectedSnapshot.indexFiles().get(f);
                final FileInfo actualFile = actualSnapshot.indexFiles().get(f);
                assertTrue(actualFile + " is not the same as " + expectedFile, actualFile.isSame(expectedFile));
                assertThat(actualFile.isCopied(), equalTo(expectedFile.isCopied()));
            }
        }
    }

    private BlobStoreIndexShardSnapshots roundTrip(BlobStoreIndexShardSnapshots generation) throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(toBuilder(generation)))) {
            return BlobStoreIndexShardSnapshots.fromXContent(parser);
        }
    }

    private static Map<String, Object> toMap(BlobStoreIndexShardSnapshots generation) throws IOException {
        return XContentHelper.convertToMap(BytesReference.bytes(toBuilder(generation)), false, XContentType.JSON).v2();
    }

    private static XContentBuilder toBuilder(BlobStoreIndexShardSnapshots generation) throws IOException {
        final XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        generation.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        return builder;
    }

    private BlobStoreIndexShardSnapshots parse(String json) throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            return BlobStoreIndexShardSnapshots.fromXContent(parser);
        }
    }

    private static FileInfo copiedFile(String physicalName, String blobName, long offset) {
        return new FileInfo(blobName, metadata(physicalName), offset);
    }

    private static FileInfo uploadedFile(String physicalName) {
        return new FileInfo(BlobStoreRepository.UPLOADED_DATA_BLOB_PREFIX + randomAlphaOfLength(10), metadata(physicalName), null);
    }

    private static StoreFileMetadata metadata(String physicalName) {
        return new StoreFileMetadata(physicalName, between(16, 1 << 20), randomAlphaOfLength(8), Version.LATEST.toString());
    }
}
