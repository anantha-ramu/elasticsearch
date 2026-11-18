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
import org.apache.lucene.util.Version;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;

import static org.elasticsearch.index.store.StoreFileMetadata.UNAVAILABLE_WRITER_UUID;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class FileInfoTests extends ESTestCase {
    private static final Version MIN_SUPPORTED_LUCENE_VERSION = IndexVersions.MINIMUM_COMPATIBLE.luceneVersion();

    public void testToFromXContent() throws IOException {
        final int iters = scaledRandomIntBetween(1, 10);
        for (int iter = 0; iter < iters; iter++) {
            final BytesRef writerUuid = randomBytesRef(20);
            final BytesRef hash = new BytesRef(scaledRandomIntBetween(0, 1024 * 1024));
            hash.length = hash.bytes.length;
            for (int i = 0; i < hash.length; i++) {
                hash.bytes[i] = randomByte();
            }
            StoreFileMetadata meta = new StoreFileMetadata(
                "foobar",
                Math.abs(randomLong()),
                randomAlphaOfLengthBetween(1, 10),
                Version.LATEST.toString(),
                hash,
                writerUuid
            );
            ByteSizeValue size = ByteSizeValue.ofBytes(Math.abs(randomLong()));
            BlobStoreIndexShardSnapshot.FileInfo info = new BlobStoreIndexShardSnapshot.FileInfo("_foobar", meta, size);
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON).prettyPrint();
            boolean serializeWriterUUID = randomBoolean();
            final ToXContent.Params params;
            if (serializeWriterUUID && randomBoolean()) {
                // We serialize by the writer uuid by default
                params = new ToXContent.MapParams(Collections.emptyMap());
            } else {
                params = new ToXContent.MapParams(
                    Collections.singletonMap(FileInfo.SERIALIZE_WRITER_UUID, Boolean.toString(serializeWriterUUID))
                );
            }
            BlobStoreIndexShardSnapshot.FileInfo.toXContent(info, builder, params);
            byte[] xcontent = BytesReference.toBytes(BytesReference.bytes(shuffleXContent(builder)));

            final BlobStoreIndexShardSnapshot.FileInfo parsedInfo;
            try (XContentParser parser = createParser(JsonXContent.jsonXContent, xcontent)) {
                parser.nextToken();
                parsedInfo = BlobStoreIndexShardSnapshot.FileInfo.fromXContent(parser);
            }
            assertThat(info.name(), equalTo(parsedInfo.name()));
            assertThat(info.physicalName(), equalTo(parsedInfo.physicalName()));
            assertThat(info.length(), equalTo(parsedInfo.length()));
            assertThat(info.checksum(), equalTo(parsedInfo.checksum()));
            assertThat(info.partSize(), equalTo(parsedInfo.partSize()));
            assertThat(parsedInfo.metadata().hash().length, equalTo(hash.length));
            assertThat(parsedInfo.metadata().hash(), equalTo(hash));
            assertThat(parsedInfo.metadata().writtenBy(), equalTo(Version.LATEST.toString()));
            if (serializeWriterUUID) {
                assertThat(parsedInfo.metadata().writerUuid(), equalTo(writerUuid));
            } else {
                assertThat(parsedInfo.metadata().writerUuid(), equalTo(UNAVAILABLE_WRITER_UUID));
            }
            assertThat(parsedInfo.isSame(info.metadata()), is(true));
        }
    }

    private static BytesRef randomBytesRef(int maxSize) {
        final BytesRef hash = new BytesRef(scaledRandomIntBetween(1, maxSize));
        hash.length = hash.bytes.length;
        for (int i = 0; i < hash.length; i++) {
            hash.bytes[i] = randomByte();
        }
        return hash;
    }

    public void testInvalidFieldsInFromXContent() throws IOException {
        final int iters = scaledRandomIntBetween(1, 10);
        for (int iter = 0; iter < iters; iter++) {
            String name = "foobar";
            String physicalName = "_foobar";
            String failure = null;
            long length = Math.max(0, Math.abs(randomLong()));
            // random corruption
            switch (randomIntBetween(0, 3)) {
                case 0:
                    name = "foo,bar";
                    failure = "missing or invalid file name";
                    break;
                case 1:
                    physicalName = "_foo,bar";
                    failure = "missing or invalid physical file name";
                    break;
                case 2:
                    length = -Math.abs(randomLong());
                    failure = "missing or invalid file length";
                    break;
                case 3:
                    break;
                default:
                    fail("shouldn't be here");
            }

            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
            builder.startObject();
            builder.field(FileInfo.NAME, name);
            builder.field(FileInfo.PHYSICAL_NAME, physicalName);
            builder.field(FileInfo.LENGTH, length);
            builder.field(FileInfo.WRITTEN_BY, Version.LATEST.toString());
            builder.field(FileInfo.CHECKSUM, "666");
            builder.endObject();
            byte[] xContent = BytesReference.toBytes(BytesReference.bytes(builder));

            if (failure == null) {
                // No failures should read as usual
                final BlobStoreIndexShardSnapshot.FileInfo parsedInfo;
                try (XContentParser parser = createParser(JsonXContent.jsonXContent, xContent)) {
                    parser.nextToken();
                    parsedInfo = BlobStoreIndexShardSnapshot.FileInfo.fromXContent(parser);
                }
                assertThat(name, equalTo(parsedInfo.name()));
                assertThat(physicalName, equalTo(parsedInfo.physicalName()));
                assertThat(length, equalTo(parsedInfo.length()));
                assertEquals("666", parsedInfo.checksum());
                assertEquals("666", parsedInfo.metadata().checksum());
                assertEquals(Version.LATEST.toString(), parsedInfo.metadata().writtenBy());
            } else {
                try (XContentParser parser = createParser(JsonXContent.jsonXContent, xContent)) {
                    parser.nextToken();
                    BlobStoreIndexShardSnapshot.FileInfo.fromXContent(parser);
                    fail("Should have failed with [" + failure + "]");
                } catch (ElasticsearchParseException ex) {
                    assertThat(ex.getMessage(), containsString(failure));
                }
            }
        }
    }

    public void testCopiedFileToFromXContent() throws IOException {
        final FileInfo info = randomCopiedFileInfo();
        final XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        FileInfo.toXContent(info, builder, ToXContent.EMPTY_PARAMS);
        final byte[] xContent = BytesReference.toBytes(BytesReference.bytes(shuffleXContent(builder)));

        final FileInfo parsedInfo = parse(xContent);
        assertTrue(parsedInfo.isCopied());
        assertThat(parsedInfo.name(), equalTo(info.name()));
        assertThat(parsedInfo.offset(), equalTo(info.offset()));
        assertThat(parsedInfo.partSize(), nullValue());
        assertThat(parsedInfo.numberOfParts(), equalTo(1));
        assertThat(parsedInfo.partBytes(0), equalTo(info.length()));
        assertTrue(parsedInfo.isSame(info));
    }

    /**
     * The offset is written whenever a file info has one, also when the caller asks to leave out the writer uuid, which is the only thing
     * that a snapshot create can switch off. A snapshot delete and a snapshot clone write file infos without that parameter.
     */
    public void testOffsetIsWrittenWithAndWithoutWriterUuid() throws IOException {
        final FileInfo info = randomCopiedFileInfo();
        for (boolean serializeWriterUuid : new boolean[] { true, false }) {
            final XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
            FileInfo.toXContent(
                info,
                builder,
                new ToXContent.MapParams(Map.of(FileInfo.SERIALIZE_WRITER_UUID, Boolean.toString(serializeWriterUuid)))
            );
            final FileInfo parsedInfo = parse(BytesReference.toBytes(BytesReference.bytes(builder)));
            assertTrue(parsedInfo.isCopied());
            assertThat(parsedInfo.offset(), equalTo(info.offset()));
        }
    }

    public void testUploadedFileHasNoOffsetInXContent() throws IOException {
        final FileInfo uploaded = new FileInfo("__" + randomAlphaOfLength(10), randomCopiedFileInfo().metadata(), null);
        final XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        FileInfo.toXContent(uploaded, builder, ToXContent.EMPTY_PARAMS);
        final byte[] xContent = BytesReference.toBytes(BytesReference.bytes(builder));
        assertThat(new String(xContent, StandardCharsets.UTF_8), not(containsString(FileInfo.OFFSET)));
        assertFalse(parse(xContent).isCopied());
    }

    public void testInvalidOffsetInFromXContent() throws IOException {
        // every way of breaking the offset, then the valid case last
        for (int corruption = 0; corruption <= 2; corruption++) {
            Long offset = randomNonNegativeLong();
            Long partSize = null;
            final String failure;
            switch (corruption) {
                case 0 -> {
                    offset = -randomLongBetween(1, Long.MAX_VALUE);
                    failure = "invalid offset";
                }
                case 1 -> {
                    partSize = randomLongBetween(1, Long.MAX_VALUE);
                    failure = "cannot have a part size";
                }
                case 2 -> failure = null;
                default -> throw new AssertionError("unexpected corruption " + corruption);
            }

            final XContentBuilder builder = fileInfoBuilder();
            builder.field(FileInfo.OFFSET, offset);
            if (partSize != null) {
                builder.field(FileInfo.PART_SIZE, partSize);
            }
            builder.endObject();
            final byte[] xContent = BytesReference.toBytes(BytesReference.bytes(builder));

            if (failure == null) {
                final FileInfo parsedInfo = parse(xContent);
                assertTrue(parsedInfo.isCopied());
                assertThat(parsedInfo.offset(), equalTo(offset));
            } else {
                final ElasticsearchParseException e = expectThrows(ElasticsearchParseException.class, () -> parse(xContent));
                assertThat(e.getMessage(), containsString(failure));
            }
        }
    }

    /**
     * An older prototype wrote the name of the copied blob and the path of the commit blob as {@code blob} and {@code source}. Nothing
     * writes them any more, and the parser rejects every field that it does not know.
     */
    public void testUnknownFieldsAreStillRejected() throws IOException {
        final String unknownField = randomFrom("blob", "source", randomAlphaOfLength(8));
        final XContentBuilder builder = fileInfoBuilder();
        builder.field(unknownField, randomAlphaOfLength(10));
        builder.endObject();
        final byte[] xContent = BytesReference.toBytes(BytesReference.bytes(builder));
        final ParsingException e = expectThrows(ParsingException.class, () -> parse(xContent));
        assertThat(e.getMessage(), containsString("unknown field [" + unknownField + "]"));
    }

    public void testIsSameComparesOffset() {
        final FileInfo copied = randomCopiedFileInfo();
        final String name = copied.name();
        final StoreFileMetadata metadata = copied.metadata();
        assertTrue(copied.isSame(new FileInfo(name, metadata, copied.offset())));
        assertFalse(copied.isSame(new FileInfo(name, metadata, copied.offset() + 1)));
        // the same file at the same offset of another blob is not the same file info either
        assertFalse(copied.isSame(new FileInfo(name + "x", metadata, copied.offset())));
        // an uploaded file info has no offset, so it is not the same as a copied one, whatever the name
        final FileInfo uploaded = new FileInfo(name, metadata, null);
        assertFalse(copied.isSame(uploaded));
        assertFalse(uploaded.isSame(copied));
        assertTrue(uploaded.isSame(new FileInfo(name, metadata, null)));
    }

    public void testIsCopiedIsDecidedByTheOffsetAndNotByTheName() {
        final StoreFileMetadata metadata = randomCopiedFileInfo().metadata();
        // an uploaded blob and a copied blob are named alike, with the prefix "__"
        final String name = BlobStoreRepository.UPLOADED_DATA_BLOB_PREFIX + randomAlphaOfLength(10);
        assertFalse(new FileInfo(name, metadata, null).isCopied());
        assertFalse(new FileInfo(name, metadata, ByteSizeValue.ofBytes(between(1, 1000))).isCopied());
        assertTrue(new FileInfo(name, metadata, 0L).isCopied());
        assertTrue(new FileInfo(name, metadata, randomNonNegativeLong()).isCopied());
    }

    public void testOffsetOverTheWire() throws IOException {
        final FileInfo copied = randomCopiedFileInfo();
        final FileInfo copy = copyWriteable(copied, writableRegistry(), FileInfo::new);
        assertTrue(copy.isCopied());
        assertThat(copy.name(), equalTo(copied.name()));
        assertThat(copy.offset(), equalTo(copied.offset()));
        assertTrue(copy.isSame(copied));

        // a file stored in blobs of its own still goes over the wire as before, and has no offset when it arrives
        final FileInfo uploaded = new FileInfo(
            copied.name(),
            copied.metadata(),
            randomBoolean() ? null : ByteSizeValue.ofBytes(between(1, 1000))
        );
        final FileInfo uploadedCopy = copyWriteable(uploaded, writableRegistry(), FileInfo::new);
        assertFalse(uploadedCopy.isCopied());
        assertTrue(uploadedCopy.isSame(uploaded));
    }

    public void testOffsetIsNotSentToANodeThatDoesNotKnowIt() throws IOException {
        final TransportVersion oldVersion = TransportVersionUtils.randomVersionNotSupporting(
            TransportVersion.fromName("snapshot_file_info_offset")
        );
        final FileInfo copied = randomCopiedFileInfo();
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(oldVersion);
            // the other node would read the whole blob as if it were the file, so the file info must not be sent at all
            final IllegalStateException e = expectThrows(IllegalStateException.class, () -> copied.writeTo(out));
            assertThat(e.getMessage(), containsString(copied.name()));
        }

        final FileInfo uploaded = new FileInfo(copied.name(), copied.metadata(), ByteSizeValue.ofBytes(between(1, 1000)));
        final FileInfo uploadedCopy = copyWriteable(uploaded, writableRegistry(), FileInfo::new, oldVersion);
        assertFalse(uploadedCopy.isCopied());
        assertTrue(uploadedCopy.isSame(uploaded));
    }

    public void testCopiedFileHasNoParts() {
        // a copied file is one range of a blob that other files share, and asking for a part blob of it is a bug
        expectThrows(AssertionError.class, () -> randomCopiedFileInfo().partName(0));
    }

    /**
     * Writes the fields that every file info needs, and leaves the object open for the test to add more fields.
     */
    private static XContentBuilder fileInfoBuilder() throws IOException {
        final XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        builder.startObject();
        builder.field(FileInfo.NAME, "__foobar");
        builder.field(FileInfo.PHYSICAL_NAME, "_0.cfs");
        builder.field(FileInfo.LENGTH, between(16, 1000));
        builder.field(FileInfo.WRITTEN_BY, Version.LATEST.toString());
        builder.field(FileInfo.CHECKSUM, "666");
        return builder;
    }

    private FileInfo parse(byte[] xContent) throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, xContent)) {
            parser.nextToken();
            return FileInfo.fromXContent(parser);
        }
    }

    private static FileInfo randomCopiedFileInfo() {
        final StoreFileMetadata metadata = new StoreFileMetadata(
            "_" + randomAlphaOfLength(5) + ".cfs",
            // small, because the tests also make an uploaded file info of the same file with a part size, and the constructor of a file
            // info checks the size of every part
            randomLongBetween(16, 100_000),
            randomAlphaOfLengthBetween(1, 10),
            Version.LATEST.toString(),
            new BytesRef(),
            randomBytesRef(20)
        );
        return new FileInfo(
            BlobStoreRepository.UPLOADED_DATA_BLOB_PREFIX + randomAlphaOfLength(10),
            metadata,
            randomLongBetween(0, Long.MAX_VALUE - 1)
        );
    }

    public void testGetPartSize() {
        BlobStoreIndexShardSnapshot.FileInfo info = new BlobStoreIndexShardSnapshot.FileInfo(
            "foo",
            new StoreFileMetadata("foo", 36, "666", MIN_SUPPORTED_LUCENE_VERSION.toString()),
            ByteSizeValue.ofBytes(6)
        );
        int numBytes = 0;
        for (int i = 0; i < info.numberOfParts(); i++) {
            numBytes += (int) info.partBytes(i);
        }
        assertEquals(numBytes, 36);

        info = new BlobStoreIndexShardSnapshot.FileInfo(
            "foo",
            new StoreFileMetadata("foo", 35, "666", MIN_SUPPORTED_LUCENE_VERSION.toString()),
            ByteSizeValue.ofBytes(6)
        );
        numBytes = 0;
        for (int i = 0; i < info.numberOfParts(); i++) {
            numBytes += (int) info.partBytes(i);
        }
        assertEquals(numBytes, 35);
        final int numIters = randomIntBetween(10, 100);
        for (int j = 0; j < numIters; j++) {
            StoreFileMetadata metadata = new StoreFileMetadata(
                "foo",
                randomIntBetween(0, 1000),
                "666",
                MIN_SUPPORTED_LUCENE_VERSION.toString()
            );
            info = new BlobStoreIndexShardSnapshot.FileInfo("foo", metadata, ByteSizeValue.ofBytes(randomIntBetween(1, 1000)));
            numBytes = 0;
            for (int i = 0; i < info.numberOfParts(); i++) {
                numBytes += (int) info.partBytes(i);
            }
            assertEquals(numBytes, metadata.length());
        }
    }
}
