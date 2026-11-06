/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.snapshots.blobstore;

import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.xcontent.XContentParserUtils;
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
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Contains information about all snapshots for the given shard in repository
 * <p>
 * This class is used to find shard files that were already snapshotted and clear out shard files that are no longer referenced by any
 * snapshots of the shard.
 */
public class BlobStoreIndexShardSnapshots implements Iterable<SnapshotFiles>, ToXContentFragment {

    public static final BlobStoreIndexShardSnapshots EMPTY = new BlobStoreIndexShardSnapshots(Map.of(), List.of());

    private final List<SnapshotFiles> shardSnapshots;
    private final Map<String, FileInfo> files;

    private BlobStoreIndexShardSnapshots(Map<String, FileInfo> files, List<SnapshotFiles> shardSnapshots) {
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
        Map<String, FileInfo> newFiles = new HashMap<>();
        for (SnapshotFiles snapshot : shardSnapshots) {
            if (survivingSnapshotNames.contains(snapshot.snapshot()) == false) {
                continue;
            }
            updatedSnapshots.add(snapshot);
            for (FileInfo fileInfo : snapshot.indexFiles()) {
                FileInfo oldFile = newFiles.put(fileInfo.name(), fileInfo);
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
        Map<String, FileInfo> updatedFiles = null;
        for (FileInfo fileInfo : snapshotFiles.indexFiles()) {
            final FileInfo known = files.get(fileInfo.name());
            if (known == null) {
                if (updatedFiles == null) {
                    updatedFiles = new HashMap<>(files);
                }
                updatedFiles.put(fileInfo.name(), fileInfo);
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

    // index of Lucene file name to collection of file info in the repository
    // lazy computed because building this is map is rather expensive and only needed for the snapshot create operation
    private Map<String, Collection<FileInfo>> physicalFiles;

    /**
     * Finds reference to a snapshotted file by its {@link StoreFileMetadata}
     *
     * @param storeFileMetadata store file metadata to find file info for
     * @return the file info that matches the specified physical file or null if the file is not present in any of snapshots
     */
    public FileInfo findPhysicalIndexFile(StoreFileMetadata storeFileMetadata) {
        var p = this.physicalFiles;
        if (p == null) {
            p = new HashMap<>();
            for (SnapshotFiles snapshot : shardSnapshots) {
                for (FileInfo fileInfo : snapshot.indexFiles()) {
                    // we use identity hash set since we lookup all instances from the same map and thus equality == instance equality
                    // and we don't want to add the same file to the map multiple times
                    p.computeIfAbsent(fileInfo.physicalName(), k -> Collections.newSetFromMap(new IdentityHashMap<>()))
                        .add(files.get(fileInfo.name()));
                }
            }
            physicalFiles = p;
        }
        final var found = p.get(storeFileMetadata.name());
        if (found == null) {
            return null;
        }
        for (FileInfo fileInfo : found) {
            if (fileInfo.isSame(storeFileMetadata)) {
                return fileInfo;
            }
        }
        return null;
    }

    /**
     * Finds reference to a snapshotted file by its snapshot name
     *
     * @param name file name
     * @return file info or null if file is not present in any of snapshots
     */
    public FileInfo findNameFile(String name) {
        return files.get(name);
    }

    // Built on first use, like physicalFiles, because only snapshot create and delete need them: the names of the copied blobs that
    // file infos refer to, and each copy keyed by the commit blob it was copied from.
    private Set<String> copiedBlobNames;
    private Map<String, String> copiedBlobsBySource;

    /**
     * Returns the names of all copied blobs that file infos of this shard refer to. Snapshot delete keeps these copied blobs and deletes
     * every other copied blob in the shard's folder, including copies that a failed snapshot left behind.
     *
     * @return the names of the referenced copied blobs, see {@link FileInfo#blob()}
     */
    public Set<String> copiedBlobNames() {
        var names = this.copiedBlobNames;
        if (names == null) {
            final Set<String> found = new HashSet<>();
            for (FileInfo fileInfo : files.values()) {
                if (fileInfo.isCopied()) {
                    found.add(fileInfo.blob());
                }
            }
            names = Collections.unmodifiableSet(found);
            copiedBlobNames = names;
        }
        return names;
    }

    /**
     * Finds the copied blob that is a copy of the given commit blob, so that a new snapshot that needs a file from that commit blob can
     * point its file info at the existing copy instead of copying the commit blob again. If this object holds several copies of the commit
     * blob, the method returns one of them, and which one is not defined. The same object returns the same copy on every call.
     *
     * @param source path of a commit blob, see {@link FileInfo#source()}
     * @return the name of the copied blob, or null if no file info refers to a copy of {@code source}
     */
    public String findCopiedBlob(String source) {
        var bySource = this.copiedBlobsBySource;
        if (bySource == null) {
            bySource = new HashMap<>();
            for (FileInfo fileInfo : files.values()) {
                if (fileInfo.isCopied()) {
                    // This object can hold two copies of the same commit blob. index-<uuid> is the blob that lists the files of every
                    // snapshot of the shard, and snap-<uuid>.dat is the blob that lists the files of a single snapshot. If index-<uuid>
                    // is missing, the repository rebuilds it from all the snap-<uuid>.dat blobs. Then the copy that a failed snapshot
                    // left behind comes in next to the copy of a later snapshot. Keep the first copy found. The map has no order, so the
                    // pick is arbitrary. The picked copy can already be queued for deletion, as a __ blob can: a snapshot delete may not
                    // have removed the snap-<uuid>.dat blob of the snapshot that it deleted, and the rebuild reads that blob too.
                    // Otherwise the copy stays: the snapshot that reuses the copy writes a new index-<uuid> that refers to the copy, so
                    // a later snapshot delete does not remove the copy.
                    bySource.putIfAbsent(fileInfo.source(), fileInfo.blob());
                }
            }
            copiedBlobsBySource = bySource;
        }
        return bySource.get(source);
    }

    @Override
    public Iterator<SnapshotFiles> iterator() {
        return shardSnapshots.iterator();
    }

    static final class Fields {
        static final String FILES = "files";
        static final String SNAPSHOTS = "snapshots";
        static final String SHARD_STATE_ID = "shard_state_id";
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
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        // First we list all blobs with their file infos:
        builder.startArray(Fields.FILES);
        for (Map.Entry<String, FileInfo> entry : files.entrySet()) {
            FileInfo.toXContent(entry.getValue(), builder, params);
        }
        builder.endArray();
        // Then we list all snapshots with list of all blobs that are used by the snapshot
        builder.startObject(Fields.SNAPSHOTS);
        for (SnapshotFiles snapshot : shardSnapshots) {
            builder.startObject(snapshot.snapshot());
            builder.startArray(Fields.FILES);
            for (FileInfo fileInfo : snapshot.indexFiles()) {
                builder.value(fileInfo.name());
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
        // list of tuples of snapshot name and file ids in the snapshot
        List<Tuple<String, List<String>>> snapshotsAndFiles = new ArrayList<>();
        Map<String, String> historyUUIDs = new HashMap<>();
        Map<String, FileInfo> files = new HashMap<>();
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
                        files.put(fileInfo.name(), fileInfo);
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
                                        Tuple.tuple(snapshot, XContentParserUtils.parseList(parser, XContentParser::text))
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
        for (Tuple<String, List<String>> entry : snapshotsAndFiles) {
            List<FileInfo> fileInfosBuilder = new ArrayList<>();
            for (String file : entry.v2()) {
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

}
