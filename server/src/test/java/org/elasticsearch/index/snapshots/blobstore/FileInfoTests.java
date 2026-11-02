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
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Collections;

import static org.elasticsearch.index.store.StoreFileMetadata.UNAVAILABLE_WRITER_UUID;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
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

        final FileInfo parsedInfo;
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, xContent)) {
            parser.nextToken();
            parsedInfo = FileInfo.fromXContent(parser);
        }
        assertTrue(parsedInfo.isCopied());
        assertThat(parsedInfo.name(), equalTo(info.name()));
        assertThat(parsedInfo.blob(), equalTo(info.blob()));
        assertThat(parsedInfo.offset(), equalTo(info.offset()));
        assertThat(parsedInfo.source(), equalTo(info.source()));
        assertThat(parsedInfo.partSize(), nullValue());
        assertThat(parsedInfo.numberOfParts(), equalTo(1));
        assertThat(parsedInfo.partBytes(0), equalTo(info.length()));
        assertTrue(parsedInfo.isSame(info));
    }

    public void testInvalidCopiedFieldsInFromXContent() throws IOException {
        // every way of breaking the copied fields, then the valid case last
        for (int corruption = 0; corruption <= 6; corruption++) {
            String blob = "c__" + randomAlphaOfLength(10);
            Long offset = randomNonNegativeLong();
            String source = "indices/" + randomAlphaOfLength(10) + "/0/" + between(1, 5) + "/stateless_commit_" + between(1, 1000);
            Long partSize = null;
            final String failure;
            switch (corruption) {
                case 0 -> {
                    blob = null;
                    failure = "missing or invalid copied blob name";
                }
                case 1 -> {
                    blob = "c__foo" + randomFrom(",", "/", "*", " ") + "bar";
                    failure = "missing or invalid copied blob name";
                }
                case 2 -> {
                    offset = null;
                    failure = "missing or invalid offset";
                }
                case 3 -> {
                    offset = -randomLongBetween(1, Long.MAX_VALUE);
                    failure = "missing or invalid offset";
                }
                case 4 -> {
                    source = randomBoolean() ? null : "";
                    failure = "missing source of copied blob";
                }
                case 5 -> {
                    partSize = randomLongBetween(1, Long.MAX_VALUE);
                    failure = "cannot have a part size";
                }
                case 6 -> failure = null;
                default -> throw new AssertionError("unexpected corruption " + corruption);
            }

            final XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
            builder.startObject();
            builder.field(FileInfo.NAME, "__foobar");
            builder.field(FileInfo.PHYSICAL_NAME, "_0.cfs");
            builder.field(FileInfo.LENGTH, between(16, 1000));
            builder.field(FileInfo.WRITTEN_BY, Version.LATEST.toString());
            builder.field(FileInfo.CHECKSUM, "666");
            if (blob != null) {
                builder.field(FileInfo.BLOB, blob);
            }
            if (offset != null) {
                builder.field(FileInfo.OFFSET, offset);
            }
            if (source != null) {
                builder.field(FileInfo.SOURCE, source);
            }
            if (partSize != null) {
                builder.field(FileInfo.PART_SIZE, partSize);
            }
            builder.endObject();
            final byte[] xContent = BytesReference.toBytes(BytesReference.bytes(builder));

            try (XContentParser parser = createParser(JsonXContent.jsonXContent, xContent)) {
                parser.nextToken();
                if (failure == null) {
                    // the source is a path with '/' in it, which is fine even though it would not be a valid file name
                    final FileInfo parsedInfo = FileInfo.fromXContent(parser);
                    assertTrue(parsedInfo.isCopied());
                    assertThat(parsedInfo.source(), equalTo(source));
                } else {
                    final ElasticsearchParseException e = expectThrows(
                        ElasticsearchParseException.class,
                        () -> FileInfo.fromXContent(parser)
                    );
                    assertThat(e.getMessage(), containsString(failure));
                }
            }
        }
    }

    public void testIsSameComparesWhereBytesAreStored() {
        final FileInfo copied = randomCopiedFileInfo();
        final String name = copied.name();
        final StoreFileMetadata metadata = copied.metadata();
        assertTrue(copied.isSame(new FileInfo(name, metadata, copied.blob(), copied.offset(), copied.source())));
        assertFalse(copied.isSame(new FileInfo(name, metadata, copied.blob() + "x", copied.offset(), copied.source())));
        assertFalse(copied.isSame(new FileInfo(name, metadata, copied.blob(), copied.offset() + 1, copied.source())));
        assertFalse(copied.isSame(new FileInfo(name, metadata, copied.blob(), copied.offset(), copied.source() + "x")));
        final FileInfo uploaded = new FileInfo(name, metadata, null);
        assertFalse(copied.isSame(uploaded));
        assertFalse(uploaded.isSame(copied));
    }

    public void testCopiedFileIsNotSentToAnotherNode() throws IOException {
        final FileInfo copied = randomCopiedFileInfo();
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            final IllegalStateException e = expectThrows(IllegalStateException.class, () -> copied.writeTo(out));
            assertThat(e.getMessage(), containsString(copied.blob()));
        }
        // a copied file has no part blobs, and asking for one is a bug
        expectThrows(AssertionError.class, () -> copied.partName(0));

        // a file stored in blobs of its own still goes over the wire as before
        final FileInfo uploaded = new FileInfo(
            copied.name(),
            copied.metadata(),
            randomBoolean() ? null : ByteSizeValue.ofBytes(between(1, 1000))
        );
        assertTrue(copyWriteable(uploaded, writableRegistry(), FileInfo::new).isSame(uploaded));
    }

    private static FileInfo randomCopiedFileInfo() {
        final StoreFileMetadata metadata = new StoreFileMetadata(
            "_" + randomAlphaOfLength(5) + ".cfs",
            randomLongBetween(16, Integer.MAX_VALUE),
            randomAlphaOfLengthBetween(1, 10),
            Version.LATEST.toString(),
            new BytesRef(),
            randomBytesRef(20)
        );
        return new FileInfo(
            "__" + randomAlphaOfLength(10),
            metadata,
            "c__" + randomAlphaOfLength(10),
            randomLongBetween(0, Long.MAX_VALUE - 1),
            "indices/" + randomAlphaOfLength(10) + "/0/" + between(1, 5) + "/stateless_commit_" + between(1, 1000)
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
