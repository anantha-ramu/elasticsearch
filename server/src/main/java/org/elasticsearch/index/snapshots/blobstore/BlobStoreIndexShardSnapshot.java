/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.snapshots.blobstore;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.gateway.CorruptStateException;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

import static org.elasticsearch.index.store.StoreFileMetadata.UNAVAILABLE_WRITER_UUID;

/**
 * Shard snapshot metadata
 */
public class BlobStoreIndexShardSnapshot implements ToXContentFragment {

    /**
     * Information about snapshotted file.
     * <p>
     * A file is stored in one of two ways:
     * <ul>
     *     <li>In blobs of its own: {@link #partName} gives the name of each part, and the parts hold exactly the file's bytes. This is
     *     how snapshots upload files, and it is the only layout older versions understand.</li>
     *     <li>As a byte range inside a copied blob: the repository holds a server-side copy of a whole stateless commit blob, named
     *     {@link #blob()}, and the file's bytes start at {@link #offset()} in it and run for {@link #length()} bytes. Many files share one
     *     copied blob, and the copied blob also holds bytes that belong to no file. No blob is written under {@link #name()}, which only
     *     keeps the file unique in the shard's snapshot metadata. {@link #source()} records which commit blob was copied, so that a later
     *     snapshot can find the copy and reuse it.</li>
     * </ul>
     */
    public static final class FileInfo implements Writeable {
        public static final String SERIALIZE_WRITER_UUID = "serialize_writer_uuid";

        private final String name;
        @Nullable
        private final ByteSizeValue partSize;
        private final long partBytes;
        private final int numberOfParts;
        private final StoreFileMetadata metadata;
        @Nullable // unless the file is stored inside a copied blob
        private final String blob;
        private final long offset;
        @Nullable // unless the file is stored inside a copied blob
        private final String source;

        /**
         * Constructs a new instance of file info
         *
         * @param name         file name as stored in the blob store
         * @param metadata  the files meta data
         * @param partSize     size of the single chunk
         */
        public FileInfo(String name, StoreFileMetadata metadata, @Nullable ByteSizeValue partSize) {
            this(name, metadata, partSize, null, 0L, null);
        }

        /**
         * Constructs the file info of a file that is stored as a byte range inside a copied blob.
         *
         * @param name     unique name of the file in the shard's snapshot metadata; no blob is written under this name
         * @param metadata the file's metadata, whose length is the length of the range
         * @param blob     name of the copied blob in the shard's folder of the repository
         * @param offset   position in {@code blob} where the file starts
         * @param source   path of the commit blob that was copied, used to find the copy again
         */
        public FileInfo(String name, StoreFileMetadata metadata, String blob, long offset, String source) {
            this(name, metadata, null, Objects.requireNonNull(blob), offset, Objects.requireNonNull(source));
        }

        private FileInfo(
            String name,
            StoreFileMetadata metadata,
            @Nullable ByteSizeValue partSize,
            @Nullable String blob,
            long offset,
            @Nullable String source
        ) {
            this.name = Objects.requireNonNull(name);
            this.metadata = metadata;
            assert blob == null || (partSize == null && offset >= 0) : "copied file [" + name + "] must be one range at offset >= 0";
            assert (blob == null) == (source == null) : "blob [" + blob + "] and source [" + source + "] must be set together";
            this.blob = blob;
            this.offset = offset;
            this.source = source;

            long partBytes = Long.MAX_VALUE;
            if (partSize != null && partSize.getBytes() > 0) {
                partBytes = partSize.getBytes();
            }

            if (metadata.length() == 0) {
                numberOfParts = 1;
            } else {
                long longNumberOfParts = 1L + (metadata.length() - 1L) / partBytes; // ceil(len/partBytes), but beware of long overflow
                numberOfParts = (int) longNumberOfParts;
                if (numberOfParts != longNumberOfParts) { // also beware of int overflow, although 2^32 parts is already ludicrous
                    throw new IllegalArgumentException("part size [" + partSize + "] too small for file [" + metadata + "]");
                }
            }

            this.partSize = partSize;
            this.partBytes = partBytes;
            assert IntStream.range(0, numberOfParts).mapToLong(this::partBytes).sum() == metadata.length();
        }

        public FileInfo(StreamInput in) throws IOException {
            this(in.readString(), new StoreFileMetadata(in), in.readOptionalWriteable(ByteSizeValue::readFrom));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            // Only snapshot-based peer recovery sends a FileInfo to another node, and it never uses a snapshot that holds copied files.
            // Refusing here means the wire format needs no field for them.
            if (blob != null) {
                throw new IllegalStateException("cannot send file [" + name + "] stored inside copied blob [" + blob + "] to another node");
            }
            out.writeString(name);
            metadata.writeTo(out);
            out.writeOptionalWriteable(partSize);
        }

        /**
         * Returns the base file name
         *
         * @return file name
         */
        public String name() {
            return name;
        }

        /**
         * Returns part name if file is stored as multiple parts
         *
         * @param part part number
         * @return part name
         */
        public String partName(int part) {
            assert blob == null : "file [" + name + "] has no part blobs, it is stored inside copied blob [" + blob + "]";
            if (numberOfParts > 1) {
                return name + ".part" + part;
            } else {
                return name;
            }
        }

        /**
         * Returns base file name from part name
         *
         * @param blobName part name
         * @return base file name
         */
        public static String canonicalName(String blobName) {
            if (blobName.contains(".part")) {
                return blobName.substring(0, blobName.indexOf(".part"));
            }
            return blobName;
        }

        /**
         * Returns original file name
         *
         * @return original file name
         */
        public String physicalName() {
            return metadata.name();
        }

        /**
         * File length
         *
         * @return file length
         */
        public long length() {
            return metadata.length();
        }

        /**
         * Returns part size
         *
         * @return part size
         */
        public ByteSizeValue partSize() {
            return partSize;
        }

        /**
         * Returns the size (in bytes) of a given part
         *
         * @return the size (in bytes) of a given part
         */
        public long partBytes(int part) {
            assert 0 <= part && part < numberOfParts : part + " vs " + numberOfParts;
            if (numberOfParts == 1) {
                return length();
            }
            // First and last-but-one parts have a size equal to partBytes
            if (part < (numberOfParts - 1)) {
                return partBytes;
            }
            // Last part size is deducted from the length and the number of parts
            final long lastPartBytes = length() - (this.partBytes * (numberOfParts - 1));
            assert 0 < lastPartBytes && lastPartBytes <= partBytes : lastPartBytes + " vs " + partBytes;
            return lastPartBytes;
        }

        /**
         * Returns number of parts
         *
         * @return number of parts
         */
        public int numberOfParts() {
            return numberOfParts;
        }

        /**
         * Returns file md5 checksum provided by {@link org.elasticsearch.index.store.Store}
         *
         * @return file checksum
         */
        public String checksum() {
            return metadata.checksum();
        }

        /**
         * Returns the StoreFileMetadata for this file info.
         */
        public StoreFileMetadata metadata() {
            return metadata;
        }

        /**
         * Whether this file is stored as a byte range inside a copied blob rather than in blobs of its own. See the class Javadoc.
         */
        public boolean isCopied() {
            return blob != null;
        }

        /**
         * The name of the copied blob that holds this file, or {@code null} if the file is stored in blobs of its own.
         */
        @Nullable
        public String blob() {
            return blob;
        }

        /**
         * The position in {@link #blob()} where this file starts, or {@code 0} if the file is stored in blobs of its own.
         */
        public long offset() {
            return offset;
        }

        /**
         * The path of the commit blob that {@link #blob()} is a copy of, or {@code null} if the file is stored in blobs of its own.
         */
        @Nullable
        public String source() {
            return source;
        }

        /**
         * Checks if a file in a store is the same file
         *
         * @param md file in a store
         * @return true if file in a store this this file have the same checksum and length
         */
        public boolean isSame(StoreFileMetadata md) {
            return metadata.isSame(md);
        }

        /**
         * Checks if a file in a store is the same file
         *
         * @param fileInfo file in a store
         * @return true if file in a store this this file have the same checksum and length
         */
        public boolean isSame(FileInfo fileInfo) {
            if (numberOfParts != fileInfo.numberOfParts) {
                return false;
            }
            if (partBytes != fileInfo.partBytes) {
                return false;
            }
            if (name.equals(fileInfo.name) == false) {
                return false;
            }
            if (partSize != null) {
                if (partSize.equals(fileInfo.partSize) == false) {
                    return false;
                }
            } else {
                if (fileInfo.partSize != null) {
                    return false;
                }
            }
            // Where the bytes are stored matters too, so that a shard generation and a shard snapshot that disagree about it are caught.
            if (Objects.equals(blob, fileInfo.blob) == false
                || offset != fileInfo.offset
                || Objects.equals(source, fileInfo.source) == false) {
                return false;
            }
            return metadata.isSame(fileInfo.metadata);
        }

        static final String NAME = "name";
        static final String PHYSICAL_NAME = "physical_name";
        static final String LENGTH = "length";
        static final String CHECKSUM = "checksum";
        static final String PART_SIZE = "part_size";
        static final String WRITTEN_BY = "written_by";
        static final String META_HASH = "meta_hash";
        static final String WRITER_UUID = "writer_uuid";
        static final String BLOB = "blob";
        static final String OFFSET = "offset";
        static final String SOURCE = "source";

        /**
         * Serializes file info into JSON
         *
         * @param file    file info
         * @param builder XContent builder
         */
        public static void toXContent(FileInfo file, XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(NAME, file.name);
            builder.field(PHYSICAL_NAME, file.metadata.name());
            builder.field(LENGTH, file.metadata.length());
            builder.field(CHECKSUM, file.metadata.checksum());
            if (file.partSize != null) {
                builder.field(PART_SIZE, file.partSize.getBytes());
            }

            if (file.metadata.writtenBy() != null) {
                builder.field(WRITTEN_BY, file.metadata.writtenBy());
            }

            final BytesRef hash = file.metadata.hash();
            if (hash != null && hash.length > 0) {
                builder.field(META_HASH, hash.bytes, hash.offset, hash.length);
            }

            final BytesRef writerUuid = file.metadata.writerUuid();
            // We serialize by default when SERIALIZE_WRITER_UUID is not present since in deletes/clones
            // we read the serialized files from the blob store and we enforce the version invariants when
            // the snapshot was done
            if (writerUuid.length > 0 && params.paramAsBoolean(SERIALIZE_WRITER_UUID, true)) {
                builder.field(WRITER_UUID, writerUuid.bytes, writerUuid.offset, writerUuid.length);
            }

            if (file.blob != null) {
                builder.field(BLOB, file.blob);
                builder.field(OFFSET, file.offset);
                builder.field(SOURCE, file.source);
            }

            builder.endObject();
        }

        /**
         * Parses JSON that represents file info
         *
         * @param parser parser
         * @return file info
         */
        public static FileInfo fromXContent(XContentParser parser) throws IOException {
            XContentParser.Token token = parser.currentToken();
            String name = null;
            String physicalName = null;
            long length = -1;
            String checksum = null;
            ByteSizeValue partSize = null;
            String writtenBy = null;
            BytesRef metaHash = new BytesRef();
            BytesRef writerUuid = UNAVAILABLE_WRITER_UUID;
            String blob = null;
            Long offset = null;
            String source = null;
            XContentParserUtils.ensureExpectedToken(token, XContentParser.Token.START_OBJECT, parser);
            String currentFieldName;
            while ((currentFieldName = parser.nextFieldName()) != null) {
                token = parser.nextToken();
                if (token.isValue() == false) {
                    XContentParserUtils.throwUnknownToken(token, parser);
                }
                switch (currentFieldName) {
                    case NAME -> name = parser.text();
                    case PHYSICAL_NAME -> physicalName = parser.text();
                    case LENGTH -> length = parser.longValue();
                    case CHECKSUM -> checksum = parser.text();
                    case PART_SIZE -> partSize = ByteSizeValue.ofBytes(parser.longValue());
                    case WRITTEN_BY -> writtenBy = parser.text();
                    case META_HASH -> {
                        metaHash.bytes = parser.binaryValue();
                        metaHash.offset = 0;
                        metaHash.length = metaHash.bytes.length;
                    }
                    case WRITER_UUID -> {
                        writerUuid = new BytesRef(parser.binaryValue());
                        assert BlobStoreIndexShardSnapshots.INTEGRITY_ASSERTIONS_ENABLED == false || writerUuid.length > 0;
                        if (writerUuid.length == 0) {
                            // we never write UNAVAILABLE_WRITER_UUID, so this must be due to corruption
                            throw new ElasticsearchParseException("invalid (empty) writer uuid");
                        }
                    }
                    case BLOB -> blob = parser.text();
                    case OFFSET -> offset = parser.longValue();
                    case SOURCE -> source = parser.text();
                    default -> XContentParserUtils.throwUnknownField(currentFieldName, parser);
                }
            }

            // Verify that file information is complete
            if (name == null || Strings.validFileName(name) == false) {
                throw new ElasticsearchParseException("missing or invalid file name [" + name + "]");
            } else if (physicalName == null || Strings.validFileName(physicalName) == false) {
                throw new ElasticsearchParseException("missing or invalid physical file name [" + physicalName + "]");
            } else if (length < 0) {
                throw new ElasticsearchParseException("missing or invalid file length");
            } else if (writtenBy == null) {
                throw new ElasticsearchParseException("missing or invalid written_by [" + writtenBy + "]");
            } else if (checksum == null) {
                throw new ElasticsearchParseException("missing checksum for name [" + name + "]");
            }
            try {
                // check for corruption before asserting writtenBy is parseable in the StoreFileMetadata constructor
                org.apache.lucene.util.Version.parse(writtenBy);
            } catch (Exception e) {
                throw new ElasticsearchParseException("invalid written_by [" + writtenBy + "]");
            }
            // A file stored inside a copied blob has all of blob, offset and source, and is one range, so it has no part size. Other files
            // have none of them. The source is a path in another bucket and contains '/', so it is not checked as a file name.
            if (blob != null || offset != null || source != null) {
                if (blob == null || Strings.validFileName(blob) == false) {
                    throw new ElasticsearchParseException("missing or invalid copied blob name [" + blob + "] for file [" + name + "]");
                } else if (offset == null || offset < 0) {
                    throw new ElasticsearchParseException("missing or invalid offset [" + offset + "] for file [" + name + "]");
                } else if (Strings.hasLength(source) == false) {
                    throw new ElasticsearchParseException("missing source of copied blob [" + blob + "] for file [" + name + "]");
                } else if (partSize != null) {
                    throw new ElasticsearchParseException("file [" + name + "] inside copied blob [" + blob + "] cannot have a part size");
                }
            }
            final var metadata = new StoreFileMetadata(physicalName, length, checksum, writtenBy, metaHash, writerUuid);
            return blob == null ? new FileInfo(name, metadata, partSize) : new FileInfo(name, metadata, blob, offset, source);
        }

        @Override
        public String toString() {
            return "[name: "
                + name
                + ", numberOfParts: "
                + numberOfParts
                + ", partSize: "
                + partSize
                + ", partBytes: "
                + partBytes
                + ", metadata: "
                + metadata
                + (blob == null ? "" : ", blob: " + blob + ", offset: " + offset + ", source: " + source)
                + "]";
        }
    }

    /**
     * Snapshot name
     */
    private final String snapshot;

    private final long startTime;

    private final long time;

    private final int incrementalFileCount;

    private final long incrementalSize;

    private final List<FileInfo> indexFiles;

    /**
     * Constructs new shard snapshot metadata from snapshot metadata
     *
     * @param snapshot              snapshot name
     * @param indexFiles            list of files in the shard
     * @param startTime             snapshot start time
     * @param time                  snapshot running time
     * @param incrementalFileCount  incremental of files that were snapshotted
     * @param incrementalSize       incremental size of snapshot
     */
    public BlobStoreIndexShardSnapshot(
        String snapshot,
        List<FileInfo> indexFiles,
        long startTime,
        long time,
        int incrementalFileCount,
        long incrementalSize
    ) {
        assert snapshot != null;
        this.snapshot = snapshot;
        this.indexFiles = List.copyOf(indexFiles);
        this.startTime = startTime;
        this.time = time;
        this.incrementalFileCount = incrementalFileCount;
        this.incrementalSize = incrementalSize;
    }

    /**
     * Creates a new instance has a different name and zero incremental file counts but is identical to this instance in terms of the files
     * it references.
     *
     * @param targetSnapshotName target snapshot name
     * @param startTime          time the clone operation on the repository was started
     * @param time               time it took to create the clone
     */
    public BlobStoreIndexShardSnapshot asClone(String targetSnapshotName, long startTime, long time) {
        return new BlobStoreIndexShardSnapshot(targetSnapshotName, indexFiles, startTime, time, 0, 0);
    }

    /**
     * Returns snapshot name
     *
     * @return snapshot name
     */
    public String snapshot() {
        return snapshot;
    }

    /**
     * Returns list of files in the shard
     *
     * @return list of files
     */
    public List<FileInfo> indexFiles() {
        return indexFiles;
    }

    /**
     * Returns snapshot start time
     */
    public long startTime() {
        return startTime;
    }

    /**
     * Returns snapshot running time
     */
    public long time() {
        return time;
    }

    /**
     * Returns incremental of files that were snapshotted
     */
    public int incrementalFileCount() {
        return incrementalFileCount;
    }

    /**
     * Returns total number of files that are referenced by this snapshot
     */
    public int totalFileCount() {
        return indexFiles.size();
    }

    /**
     * Returns incremental of files size that were snapshotted
     */
    public long incrementalSize() {
        return incrementalSize;
    }

    /**
     * Returns total size of all files that where snapshotted
     */
    public long totalSize() {
        return totalSize(indexFiles);
    }

    public static long totalSize(List<FileInfo> indexFiles) {
        return indexFiles.stream().mapToLong(fi -> fi.metadata().length()).sum();
    }

    private static final String NAME = "name";
    private static final String START_TIME = "start_time";
    private static final String TIME = "time";
    private static final String FILES = "files";
    // for the sake of BWC keep the actual property names as in 6.x
    // + there is a constraint in #fromXContent() that leads to ElasticsearchParseException("unknown parameter [incremental_file_count]");
    private static final String INCREMENTAL_FILE_COUNT = "number_of_files";
    private static final String INCREMENTAL_SIZE = "total_size";

    private static final ParseField PARSE_NAME = new ParseField(NAME);
    private static final ParseField PARSE_START_TIME = new ParseField(START_TIME);
    private static final ParseField PARSE_TIME = new ParseField(TIME);
    private static final ParseField PARSE_INCREMENTAL_FILE_COUNT = new ParseField(INCREMENTAL_FILE_COUNT);
    private static final ParseField PARSE_INCREMENTAL_SIZE = new ParseField(INCREMENTAL_SIZE);
    private static final ParseField PARSE_FILES = new ParseField(FILES);

    // pre-8.9.0 versions included this (unused) field so we must accept its existence
    private static final String INDEX_VERSION = "index_version";
    private static final ParseField PARSE_INDEX_VERSION = new ParseField(INDEX_VERSION, "index-version");

    /**
     * Serializes shard snapshot metadata info into JSON
     *
     * @param builder  XContent builder
     * @param params   parameters
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(NAME, snapshot);
        builder.field(INDEX_VERSION, 0); // pre-8.9.0 versions require this field to be present and non-negative
        builder.field(START_TIME, startTime);
        builder.field(TIME, time);
        builder.field(INCREMENTAL_FILE_COUNT, incrementalFileCount);
        builder.field(INCREMENTAL_SIZE, incrementalSize);
        builder.startArray(FILES);
        for (FileInfo fileInfo : indexFiles) {
            FileInfo.toXContent(fileInfo, builder, params);
        }
        builder.endArray();
        return builder;
    }

    /**
     * Parses shard snapshot metadata
     *
     * @param parser parser
     * @return shard snapshot metadata
     */
    public static BlobStoreIndexShardSnapshot fromXContent(XContentParser parser) throws IOException {
        String snapshot = null;
        long startTime = 0;
        long time = 0;
        int incrementalFileCount = 0;
        long incrementalSize = 0;

        List<FileInfo> indexFiles = null;
        if (parser.currentToken() == null) { // fresh parser? move to the first token
            parser.nextToken();
        }
        XContentParser.Token token = parser.currentToken();
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser);
            final String currentFieldName = parser.currentName();
            token = parser.nextToken();
            if (token.isValue()) {
                if (PARSE_NAME.match(currentFieldName, parser.getDeprecationHandler())) {
                    snapshot = parser.text();
                } else if (PARSE_INDEX_VERSION.match(currentFieldName, parser.getDeprecationHandler())) {
                    // pre-8.9.0 versions included this (unused) field so we must accept its existence
                    parser.longValue();
                } else if (PARSE_START_TIME.match(currentFieldName, parser.getDeprecationHandler())) {
                    startTime = parser.longValue();
                } else if (PARSE_TIME.match(currentFieldName, parser.getDeprecationHandler())) {
                    time = parser.longValue();
                } else if (PARSE_INCREMENTAL_FILE_COUNT.match(currentFieldName, parser.getDeprecationHandler())) {
                    incrementalFileCount = parser.intValue();
                } else if (PARSE_INCREMENTAL_SIZE.match(currentFieldName, parser.getDeprecationHandler())) {
                    incrementalSize = parser.longValue();
                } else {
                    XContentParserUtils.throwUnknownField(currentFieldName, parser);
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (PARSE_FILES.match(currentFieldName, parser.getDeprecationHandler())) {
                    indexFiles = XContentParserUtils.parseList(parser, FileInfo::fromXContent);
                } else {
                    XContentParserUtils.throwUnknownField(currentFieldName, parser);
                }
            } else {
                XContentParserUtils.throwUnknownToken(token, parser);
            }
        }

        // check for corruption before asserting snapshot != null in the BlobStoreIndexShardSnapshot ctor
        if (snapshot == null) {
            throw new CorruptStateException("snapshot missing");
        }

        return new BlobStoreIndexShardSnapshot(
            snapshot,
            indexFiles == null ? List.of() : indexFiles,
            startTime,
            time,
            incrementalFileCount,
            incrementalSize
        );
    }
}
