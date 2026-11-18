/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.snapshots.blobstore;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Contains information about all snapshots for the given shard in repository
 * <p>
 * This class is used to find shard files that were already snapshotted and clear out shard files that are no longer referenced by any
 * snapshots of the shard.
 * <p>
 * The class holds the file infos that the snapshots of the shard list, each of them once. A file info is identified by its name and its
 * offset, because files that were copied together share the name of the copied blob that holds them, and only the offset tells them
 * apart.
 */
public class BlobStoreIndexShardSnapshots implements Iterable<SnapshotFiles>, ToXContentFragment {

    public static final BlobStoreIndexShardSnapshots EMPTY = new BlobStoreIndexShardSnapshots(Map.of(), List.of());

    /**
     * What identifies a file info in the shard's snapshot metadata: the name, and the offset if the file is a byte range of a copied blob
     * (see {@link FileInfo#isCopied()}). The name alone is not enough, because every file of a copied blob has the name of that blob. A
     * file that is not copied has no offset, so its key is as unique as its name was before copies existed.
     *
     * @param name   the {@link FileInfo#name()} of the file info
     * @param offset the {@link FileInfo#offset()} of the file info, or {@code null} if the file is not copied
     */
    private record FileKey(String name, @Nullable Long offset) {

        static FileKey of(FileInfo fileInfo) {
            return new FileKey(fileInfo.name(), fileInfo.isCopied() ? fileInfo.offset() : null);
        }

        /**
         * The name of the file info, and its offset if it has one, for the messages about a damaged shard generation. The text for a file
         * that is not copied is its name alone, as it was before there were copied files.
         */
        @Override
        public String toString() {
            return offset == null ? name : name + " at offset " + offset;
        }
    }

    private final List<SnapshotFiles> shardSnapshots;
    private final Map<FileKey, FileInfo> files;

    private BlobStoreIndexShardSnapshots(Map<FileKey, FileInfo> files, List<SnapshotFiles> shardSnapshots) {
        this.shardSnapshots = List.copyOf(shardSnapshots);
        this.files = files;
    }

    /**
     * Creates a new list of the shard's snapshots ({@link BlobStoreIndexShardSnapshots}) retaining only the shard snapshots
     * specified by ID in {@code retainedSnapshots}. Typically used for snapshot deletions, which reduce the shard snapshots.
     */
    public BlobStoreIndexShardSnapshots withRetainedSnapshots(Set<SnapshotId> retainedSnapshots) {
        if (retainedSnapshots.isEmpty()) {
            return EMPTY;
        }
        final var survivingSnapshotNames = retainedSnapshots.stream().map(SnapshotId::getName).collect(Collectors.toSet());
        final ArrayList<SnapshotFiles> updatedSnapshots = new ArrayList<>(survivingSnapshotNames.size());
        Map<FileKey, FileInfo> newFiles = new HashMap<>();
        for (SnapshotFiles snapshot : shardSnapshots) {
            if (survivingSnapshotNames.contains(snapshot.snapshot()) == false) {
                continue;
            }
            updatedSnapshots.add(snapshot);
            for (FileInfo fileInfo : snapshot.indexFiles()) {
                FileInfo oldFile = newFiles.put(FileKey.of(fileInfo), fileInfo);
                assert oldFile == null || oldFile.isSame(fileInfo);
            }
        }
        return new BlobStoreIndexShardSnapshots(newFiles, updatedSnapshots);
    }

    /**
     * Creates a new list of the shard's snapshots ({@link BlobStoreIndexShardSnapshots}) adding a new shard snapshot
     * ({@link SnapshotFiles}).
     */
    public BlobStoreIndexShardSnapshots withAddedSnapshot(SnapshotFiles snapshotFiles) {
        Map<FileKey, FileInfo> updatedFiles = null;
        for (FileInfo fileInfo : snapshotFiles.indexFiles()) {
            final FileKey key = FileKey.of(fileInfo);
            final FileInfo known = files.get(key);
            if (known == null) {
                if (updatedFiles == null) {
                    updatedFiles = new HashMap<>(files);
                }
                updatedFiles.put(key, fileInfo);
            } else {
                assert fileInfo.isSame(known);
            }
        }
        return new BlobStoreIndexShardSnapshots(
            updatedFiles == null ? files : updatedFiles,
            CollectionUtils.appendToCopyNoNullElements(shardSnapshots, snapshotFiles)
        );
    }

    /**
     * Create a new instance that has a new snapshot by name {@code target} added which shares all files with the snapshot of name
     * {@code source}.
     *
     * @param source source snapshot name
     * @param target target snapshot name
     * @return new instance with added cloned snapshot
     */
    public BlobStoreIndexShardSnapshots withClone(String source, String target) {
        SnapshotFiles sourceFiles = null;
        for (SnapshotFiles shardSnapshot : shardSnapshots) {
            if (shardSnapshot.snapshot().equals(source)) {
                sourceFiles = shardSnapshot;
                break;
            }
        }
        if (sourceFiles == null) {
            throw new IllegalArgumentException("unknown source [" + source + "]");
        }
        return new BlobStoreIndexShardSnapshots(
            files,
            CollectionUtils.appendToCopyNoNullElements(shardSnapshots, sourceFiles.withSnapshotName(target))
        );
    }

    /**
     * Returns list of snapshots
     *
     * @return list of snapshots
     */
    public List<SnapshotFiles> snapshots() {
        return this.shardSnapshots;
    }

    // index of Lucene file name to collection of file info in the repository, in the order in which the snapshots list them
    // lazy computed because building this is map is rather expensive and only needed for the snapshot create operation
    private Map<String, Collection<FileInfo>> physicalFiles;

    /**
     * Finds reference to a snapshotted file by its {@link StoreFileMetadata}
     *
     * @param storeFileMetadata store file metadata to find file info for
     * @return the file info that matches the specified physical file or null if the file is not present in any of snapshots
     */
    public FileInfo findPhysicalIndexFile(StoreFileMetadata storeFileMetadata) {
        return findPhysicalIndexFile(storeFileMetadata, fileInfo -> true);
    }

    /**
     * Finds reference to a snapshotted file by its {@link StoreFileMetadata}, among the file infos that {@code isCandidate} accepts. The
     * repository can hold several file infos for one Lucene file, because snapshots stored the file in different places, for example in
     * an uploaded blob and in a copied blob. The caller says which of them it can use, so that a file info that it cannot use does not
     * hide one that it can.
     * <p>
     * If more than one file info matches and is accepted, the method returns the one that comes first when the snapshots are read in
     * order, each snapshot's list of files in order. That order does not change between calls or between runs.
     *
     * @param storeFileMetadata store file metadata to find file info for
     * @param isCandidate       says whether the caller can use a file info that is the same file
     * @return the first file info that matches the specified physical file and that {@code isCandidate} accepts, or null if there is none
     */
    public FileInfo findPhysicalIndexFile(StoreFileMetadata storeFileMetadata, Predicate<FileInfo> isCandidate) {
        var p = this.physicalFiles;
        if (p == null) {
            p = new HashMap<>();
            for (SnapshotFiles snapshot : shardSnapshots) {
                for (FileInfo fileInfo : snapshot.indexFiles()) {
                    // We use a linked hash set. A file info has no equals method, and we lookup all instances from the same map, so
                    // equality == instance equality and we don't add the same file to the map multiple times. The set keeps the order in
                    // which the snapshots list the files, so a lookup that finds several matches always finds the same one first.
                    p.computeIfAbsent(fileInfo.physicalName(), k -> new LinkedHashSet<>()).add(files.get(FileKey.of(fileInfo)));
                }
            }
            physicalFiles = p;
        }
        final var found = p.get(storeFileMetadata.name());
        if (found == null) {
            return null;
        }
        for (FileInfo fileInfo : found) {
            if (fileInfo.isSame(storeFileMetadata) && isCandidate.test(fileInfo)) {
                return fileInfo;
            }
        }
        return null;
    }

    // index of file info name to a file info with that name
    // lazy computed, like physicalFiles, because building this map is rather expensive and only the snapshot delete operation needs it
    private volatile Map<String, FileInfo> filesByName;

    /**
     * Finds reference to a snapshotted file by its snapshot name. Files of one copied blob share the name of that blob, so the method
     * returns any of the file infos with that name. That is enough for what callers need to know, which is whether any file still uses
     * the blob with that name.
     *
     * @param name file name
     * @return file info or null if file is not present in any of snapshots
     */
    public FileInfo findNameFile(String name) {
        var byName = this.filesByName;
        if (byName == null) {
            byName = new HashMap<>();
            for (FileInfo fileInfo : files.values()) {
                byName.putIfAbsent(fileInfo.name(), fileInfo);
            }
            filesByName = byName;
        }
        return byName.get(name);
    }

    @Override
    public Iterator<SnapshotFiles> iterator() {
        return shardSnapshots.iterator();
    }

    static final class Fields {
        static final String FILES = "files";
        static final String SNAPSHOTS = "snapshots";
        static final String SHARD_STATE_ID = "shard_state_id";
        static final String NAME = "name";
        static final String OFFSET = "offset";
    }

    /**
     * Writes index file for the shard in the following format.
     * <pre>
     * <code>
     * {
     *     "files": [{
     *         "name": "__3",
     *         "physical_name": "_0.si",
     *         "length": 310,
     *         "checksum": "1tpsg3p",
     *         "written_by": "5.1.0",
     *         "meta_hash": "P9dsFxNMdWNlb......"
     *     }, {
     *         "name": "__2",
     *         "physical_name": "segments_2",
     *         "length": 150,
     *         "checksum": "11qjpz6",
     *         "written_by": "5.1.0",
     *         "meta_hash": "P9dsFwhzZWdtZ......."
     *     }, {
     *         "name": "__1",
     *         "physical_name": "_0.cfe",
     *         "length": 363,
     *         "checksum": "er9r9g",
     *         "written_by": "5.1.0"
     *     }, {
     *         "name": "__0",
     *         "physical_name": "_0.cfs",
     *         "length": 3354,
     *         "checksum": "491liz",
     *         "written_by": "5.1.0"
     *     }, {
     *         "name": "__4",
     *         "physical_name": "segments_3",
     *         "length": 150,
     *         "checksum": "134567",
     *         "written_by": "5.1.0",
     *         "meta_hash": "P9dsFwhzZWdtZ......."
     *     }],
     *     "snapshots": {
     *         "snapshot_1": {
     *             "files": ["__0", "__1", "__2", "__3"]
     *         },
     *         "snapshot_2": {
     *             "files": ["__0", "__1", "__2", "__4"]
     *         }
     *     }
     * }
     * }
     * </code>
     * </pre>
     * Each snapshot lists its files by name, as in the example. Files that were copied together share the name of the copied blob, so
     * the list refers to a copied file by an object that also gives the offset, and the "files" array above holds one file info for each
     * pair of name and offset. A list can hold both kinds of reference. So a shard whose snapshots have no copied files is written as in
     * the example, and a list with one file that is not copied and two copied files looks like this:
     * <pre>
     * <code>
     *         "snapshot_3": {
     *             "files": ["__0", {"name": "__7", "offset": 900}, {"name": "__7", "offset": 5000}]
     *         }
     * </code>
     * </pre>
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        // First we list all blobs with their file infos:
        builder.startArray(Fields.FILES);
        for (FileInfo fileInfo : files.values()) {
            FileInfo.toXContent(fileInfo, builder, params);
        }
        builder.endArray();
        // Then we list all snapshots with list of all blobs that are used by the snapshot
        builder.startObject(Fields.SNAPSHOTS);
        for (SnapshotFiles snapshot : shardSnapshots) {
            builder.startObject(snapshot.snapshot());
            builder.startArray(Fields.FILES);
            for (FileInfo fileInfo : snapshot.indexFiles()) {
                if (fileInfo.isCopied()) {
                    // The name is shared with other files of the same copied blob, so the offset says which of them it is.
                    builder.startObject();
                    builder.field(Fields.NAME, fileInfo.name());
                    builder.field(Fields.OFFSET, fileInfo.offset());
                    builder.endObject();
                } else {
                    builder.value(fileInfo.name());
                }
            }
            builder.endArray();
            if (snapshot.shardStateIdentifier() != null) {
                builder.field(Fields.SHARD_STATE_ID, snapshot.shardStateIdentifier());
            }
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    static volatile boolean INTEGRITY_ASSERTIONS_ENABLED = true;

    public static boolean areIntegrityAssertionsEnabled() {
        return INTEGRITY_ASSERTIONS_ENABLED;
    }

    public static BlobStoreIndexShardSnapshots fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        if (token == null) { // New parser
            token = parser.nextToken();
        }
        // list of tuples of snapshot name and the keys of the files in the snapshot
        List<Tuple<String, List<FileKey>>> snapshotsAndFiles = new ArrayList<>();
        Map<String, String> historyUUIDs = new HashMap<>();
        Map<FileKey, FileInfo> files = new HashMap<>();
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);
        String currentFieldName;
        while ((currentFieldName = parser.nextFieldName()) != null) {
            token = parser.nextToken();
            switch (token) {
                case START_ARRAY -> {
                    if (Fields.FILES.equals(currentFieldName) == false) {
                        XContentParserUtils.throwUnknownField(currentFieldName, parser);
                    }
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        FileInfo fileInfo = FileInfo.fromXContent(parser);
                        files.put(FileKey.of(fileInfo), fileInfo);
                    }
                }
                case START_OBJECT -> {
                    if (Fields.SNAPSHOTS.equals(currentFieldName) == false) {
                        XContentParserUtils.throwUnknownField(currentFieldName, parser);
                    }
                    String snapshot;
                    while ((snapshot = parser.nextFieldName()) != null) {
                        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                        while ((currentFieldName = parser.nextFieldName()) != null) {
                            token = parser.nextToken();
                            if (Fields.FILES.equals(currentFieldName)) {
                                if (token == XContentParser.Token.START_ARRAY) {
                                    snapshotsAndFiles.add(
                                        Tuple.tuple(
                                            snapshot,
                                            XContentParserUtils.parseList(parser, BlobStoreIndexShardSnapshots::parseFileKey)
                                        )
                                    );
                                }
                            } else if (Fields.SHARD_STATE_ID.equals(currentFieldName)) {
                                historyUUIDs.put(snapshot, parser.text());
                            }
                        }
                    }
                }
                default -> XContentParserUtils.throwUnknownToken(token, parser);
            }
        }

        List<SnapshotFiles> snapshots = new ArrayList<>(snapshotsAndFiles.size());
        for (Tuple<String, List<FileKey>> entry : snapshotsAndFiles) {
            List<FileInfo> fileInfosBuilder = new ArrayList<>();
            for (FileKey file : entry.v2()) {
                FileInfo fileInfo = files.get(file);
                if (fileInfo == null) {
                    // could happen in production if the repo contents are corrupted
                    final var exception = new IllegalStateException("shard index inconsistent at file [" + file + "]");
                    assert INTEGRITY_ASSERTIONS_ENABLED == false : exception;
                    throw exception;
                }
                fileInfosBuilder.add(fileInfo);
            }
            snapshots.add(new SnapshotFiles(entry.v1(), Collections.unmodifiableList(fileInfosBuilder), historyUUIDs.get(entry.v1())));
        }
        return new BlobStoreIndexShardSnapshots(files, snapshots);
    }

    /**
     * Parses one reference from a snapshot's list of files, which is either the name of a file that is not copied, as a string, or an
     * object with the name and the offset of a copied file. The parser is at the value to read.
     */
    private static FileKey parseFileKey(XContentParser parser) throws IOException {
        if (parser.currentToken().isValue()) {
            return new FileKey(parser.text(), null);
        }
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        String name = null;
        Long offset = null;
        String currentFieldName;
        while ((currentFieldName = parser.nextFieldName()) != null) {
            final XContentParser.Token token = parser.nextToken();
            if (token.isValue() == false) {
                XContentParserUtils.throwUnknownToken(token, parser);
            }
            switch (currentFieldName) {
                case Fields.NAME -> name = parser.text();
                case Fields.OFFSET -> offset = parser.longValue();
                default -> XContentParserUtils.throwUnknownField(currentFieldName, parser);
            }
        }
        if (name == null || offset == null) {
            throw new ElasticsearchParseException(
                "a reference to a copied file needs both a name and an offset, got name [" + name + "] and offset [" + offset + "]"
            );
        }
        return new FileKey(name, offset);
    }
}
