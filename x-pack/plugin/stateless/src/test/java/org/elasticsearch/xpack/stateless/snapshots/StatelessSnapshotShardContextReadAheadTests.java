/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.fs.FsBlobContainer;
import org.elasticsearch.common.blobstore.fs.FsBlobStore;
import org.elasticsearch.common.blobstore.support.FilterBlobContainer;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.repositories.SnapshotShardContext;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ScalingExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.commits.BlobFile;
import org.elasticsearch.xpack.stateless.commits.BlobLocation;
import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;
import org.junit.After;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.mockito.Mockito.mock;

/**
 * Reads files through the stateless snapshot context with and without read-ahead of the source, as {@code snapshotFile} does: part by
 * part, to the end, and then {@code verify()}, which compares the checksum of the bytes that were read with the one in the footer.
 */
public class StatelessSnapshotShardContextReadAheadTests extends ESTestCase {

    private static final String POOL = "test_read_ahead";
    private static final String BLOB_NAME = "stateless_commit_42";

    private ThreadPool threadPool;
    private final AtomicInteger tasks = new AtomicInteger();
    private Executor countingExecutor;

    @Before
    public void createThreadPool() {
        threadPool = new TestThreadPool(
            getTestName(),
            new ScalingExecutorBuilder(POOL, 0, between(1, 4), TimeValue.timeValueMinutes(1), true, "test.read_ahead")
        );
        final var executor = threadPool.executor(POOL);
        countingExecutor = r -> {
            tasks.incrementAndGet();
            executor.execute(r);
        };
    }

    @After
    public void terminateThreadPool() {
        terminate(threadPool);
    }

    /** A file as Lucene writes it: some bytes, and a footer that has the checksum of everything before it. */
    private static byte[] fileOfLength(int length) throws IOException {
        assertThat(length, greaterThan(CodecUtil.footerLength() - 1));
        final var out = new ByteBuffersDataOutput();
        final var content = randomByteArrayOfLength(length - CodecUtil.footerLength());
        final var crc = new java.util.zip.CRC32();
        // the footer is magic, algorithm id and the checksum of the bytes before the checksum, which includes the magic and the id
        final var footerStart = new ByteBuffersDataOutput();
        CodecUtil.writeBEInt(footerStart, CodecUtil.FOOTER_MAGIC);
        CodecUtil.writeBEInt(footerStart, 0);
        crc.update(content);
        crc.update(footerStart.toArrayCopy());
        out.writeBytes(content);
        CodecUtil.writeBEInt(out, CodecUtil.FOOTER_MAGIC);
        CodecUtil.writeBEInt(out, 0);
        CodecUtil.writeBELong(out, crc.getValue());
        final var bytes = out.toArrayCopy();
        assertThat(bytes.length, equalTo(length));
        return bytes;
    }

    private static FsBlobContainer newBlobContainer() throws IOException {
        final var path = PathUtils.get(createTempDir().toString());
        return new FsBlobContainer(new FsBlobStore(randomIntBetween(1, 8) * 1024, path, false), BlobPath.EMPTY, path);
    }

    private StatelessSnapshotShardContext newContext(
        FsBlobContainer container,
        List<StatelessSnapshotShardContextTests.CloseTrackingInputStream> openedStreams,
        long blobOffset,
        long fileLength,
        SourceReadAhead readAhead
    ) {
        final var shardId = new ShardId(new Index(randomIdentifier(), randomUUID()), 0);
        return new StatelessSnapshotShardContext(
            shardId,
            new SnapshotId(randomIdentifier(), randomUUID()),
            new IndexId(shardId.getIndexName(), randomUUID()),
            randomIdentifier(),
            IndexShardSnapshotStatus.newInitializing(ShardGeneration.newGeneration(), randomLongBetween(1, Long.MAX_VALUE)),
            IndexVersion.current(),
            randomNonNegativeLong(),
            null,
            Map.of("file", new BlobLocation(new BlobFile(BLOB_NAME, new PrimaryTermAndGeneration(1L, 42L)), blobOffset, fileLength)),
            (s, g) -> new FilterBlobContainer(container) {
                @Override
                protected BlobContainer wrapChild(BlobContainer child) {
                    throw new AssertionError("should not obtain child");
                }

                @Override
                public InputStream readBlob(OperationPurpose purpose, String blobName, long position, long length) throws IOException {
                    final var stream = new StatelessSnapshotShardContextTests.CloseTrackingInputStream(
                        super.readBlob(purpose, blobName, position, length),
                        false
                    );
                    openedStreams.add(stream);
                    return stream;
                }
            },
            () -> false,
            readAhead,
            new PlainActionFuture<>()
        );
    }

    /** Between 1 and 5 parts of at least a byte each, that add up to the length of the file, in an order that is not sorted. */
    private static List<Integer> partLengths(int fileLength) {
        final int parts = between(1, Math.min(5, fileLength));
        final var lengths = new ArrayList<Integer>();
        int left = fileLength;
        for (int i = 0; i < parts - 1; i++) {
            final int part = between(1, left - (parts - 1 - i));
            lengths.add(part);
            left -= part;
        }
        lengths.add(left);
        Collections.shuffle(lengths, random());
        return lengths;
    }

    private static byte[] readParts(
        SnapshotShardContext.FileReader fileReader,
        List<Integer> partLengths,
        int fileLength,
        boolean closeEachPart
    ) throws IOException {
        final var content = new byte[fileLength];
        int read = 0;
        for (int partLength : partLengths) {
            final var in = fileReader.openInput(partLength);
            assertThat(in.readNBytes(content, read, partLength), equalTo(partLength));
            read += partLength;
            if (closeEachPart) {
                in.close();
            }
        }
        return content;
    }

    private static SnapshotShardContext.FileReader fileReader(StatelessSnapshotShardContext context) throws IOException {
        return context.fileReader("file", mock(StoreFileMetadata.class));
    }

    public void testSameBytesAndChecksumWithAndWithoutReadAhead() throws IOException {
        final int blockSize = between(1, 2048);
        final int fileLength = Math.max(CodecUtil.footerLength(), switch (between(0, 6)) {
            case 0 -> blockSize - 1;
            case 1 -> blockSize;
            case 2 -> blockSize + 1;
            case 3 -> 2 * blockSize;
            case 4 -> 2 * blockSize + 1;
            case 5 -> between(1, 10 * blockSize);
            case 6 -> CodecUtil.footerLength();
            default -> throw new AssertionError();
        });
        final var bytes = fileOfLength(fileLength);
        final int blobOffset = between(0, 50);
        final var blobBytes = new byte[blobOffset + fileLength + between(0, 50)];
        random().nextBytes(blobBytes);
        System.arraycopy(bytes, 0, blobBytes, blobOffset, fileLength);
        final var container = newBlobContainer();
        container.writeBlob(OperationPurpose.SNAPSHOT_DATA, BLOB_NAME, new ByteArrayInputStream(blobBytes), blobBytes.length, true);
        final var parts = partLengths(fileLength);
        final boolean closeEachPart = randomBoolean();

        final byte[][] read = new byte[2][];
        for (int run = 0; run < 2; run++) {
            final boolean withReadAhead = run == 1;
            final var opened = new ArrayList<StatelessSnapshotShardContextTests.CloseTrackingInputStream>();
            final var context = newContext(
                container,
                opened,
                blobOffset,
                fileLength,
                withReadAhead ? new SourceReadAhead(countingExecutor, true, blockSize) : null
            );
            tasks.set(0);
            try (var fileReader = fileReader(context)) {
                read[run] = readParts(fileReader, parts, fileLength, closeEachPart);
                fileReader.verify(); // the checksum was computed from the bytes in order, and is the one of the file
            }
            for (var stream : opened) {
                assertTrue("every stream of the source is closed", stream.isClosed());
            }
            if (withReadAhead) {
                final boolean anyLongerThanABlock = parts.stream().anyMatch(p -> p > blockSize);
                assertThat(tasks.get() > 0, equalTo(anyLongerThanABlock));
            } else {
                assertThat(tasks.get(), equalTo(0));
            }
        }
        assertArrayEquals(bytes, read[0]);
        assertArrayEquals(read[0], read[1]);
        assertIdle();
    }

    public void testCorruptFileIsStillDetectedWithReadAhead() throws IOException {
        final int blockSize = between(1, 512);
        final int fileLength = between(2 * blockSize + 1, 8 * blockSize) + CodecUtil.footerLength();
        final var bytes = fileOfLength(fileLength);
        final int flipped = between(0, fileLength - 1);
        bytes[flipped] ^= (byte) (1 << between(0, 7));
        final var container = newBlobContainer();
        container.writeBlob(OperationPurpose.SNAPSHOT_DATA, BLOB_NAME, new ByteArrayInputStream(bytes), fileLength, true);
        final var opened = new ArrayList<StatelessSnapshotShardContextTests.CloseTrackingInputStream>();
        final var context = newContext(container, opened, 0, fileLength, new SourceReadAhead(countingExecutor, true, blockSize));

        try (var fileReader = fileReader(context)) {
            readParts(fileReader, partLengths(fileLength), fileLength, randomBoolean());
            final var e = expectThrows(CorruptIndexException.class, fileReader::verify);
            assertThat(e.getMessage(), allOf(containsString("verification failed"), containsString(BLOB_NAME)));
        }
        assertThat(tasks.get(), greaterThan(0));
    }

    /**
     * What the GCS resumable upload does after a 410: it marks the start of the stream and resets to it, which reopens the ranged read
     * at the mark, so the read-ahead of the stream before it has to be dropped and a new one started, without the checksum seeing a byte
     * twice.
     */
    public void testMarkAndResetWithReadAhead() throws IOException {
        final int blockSize = between(1, 512);
        final int fileLength = between(3 * blockSize, 10 * blockSize) + CodecUtil.footerLength();
        final var bytes = fileOfLength(fileLength);
        final var container = newBlobContainer();
        container.writeBlob(OperationPurpose.SNAPSHOT_DATA, BLOB_NAME, new ByteArrayInputStream(bytes), fileLength, true);
        final var opened = new ArrayList<StatelessSnapshotShardContextTests.CloseTrackingInputStream>();
        final var context = newContext(container, opened, 0, fileLength, new SourceReadAhead(countingExecutor, true, blockSize));

        try (var fileReader = fileReader(context)) {
            final var in = fileReader.openInput(fileLength);
            assertTrue(in.markSupported());
            in.mark(Integer.MAX_VALUE);
            // read some, in the middle of a block or at the end of one, and start again
            final int beforeReset = between(1, fileLength - 1);
            assertThat(in.readNBytes(beforeReset).length, equalTo(beforeReset));
            final int openedBefore = opened.size();
            in.reset();
            assertThat(opened.size(), equalTo(openedBefore + 1));
            assertTrue("the stream that was dropped is closed", opened.get(openedBefore - 1).isClosed());
            assertArrayEquals(bytes, in.readAllBytes());
            fileReader.verify();
        }
        for (var stream : opened) {
            assertTrue(stream.isClosed());
        }
        assertIdle();
    }

    /**
     * An abort or a pause makes the snapshot's read fail, and the file reader is closed from there, with the stream in the middle of a file
     * and a block already read ahead.
     */
    public void testClosingTheFileReaderInTheMiddleOfAFileStopsTheReadAhead() throws Exception {
        final int blockSize = between(1, 512);
        final int fileLength = between(4 * blockSize, 10 * blockSize) + CodecUtil.footerLength();
        final var bytes = fileOfLength(fileLength);
        final var container = newBlobContainer();
        container.writeBlob(OperationPurpose.SNAPSHOT_DATA, BLOB_NAME, new ByteArrayInputStream(bytes), fileLength, true);
        final var opened = new ArrayList<StatelessSnapshotShardContextTests.CloseTrackingInputStream>();
        final var context = newContext(container, opened, 0, fileLength, new SourceReadAhead(countingExecutor, true, blockSize));

        try (var fileReader = fileReader(context)) {
            final var in = fileReader.openInput(fileLength);
            final int some = between(1, fileLength - 1);
            assertArrayEquals(Arrays.copyOf(bytes, some), in.readNBytes(some));
        }
        for (var stream : opened) {
            assertTrue(stream.isClosed());
        }
        assertIdle();
    }

    public void testSettingOffUsesNoExecutorAndTheSettingAppliesToTheNextStream() throws IOException {
        final int blockSize = between(1, 512);
        final int fileLength = between(3 * blockSize, 10 * blockSize) + CodecUtil.footerLength();
        final var bytes = fileOfLength(fileLength);
        final var container = newBlobContainer();
        container.writeBlob(OperationPurpose.SNAPSHOT_DATA, BLOB_NAME, new ByteArrayInputStream(bytes), fileLength, true);
        final var readAhead = new SourceReadAhead(countingExecutor, false, blockSize);
        // a context without a read-ahead at all, and one with it off, are the same
        for (var ra : new SourceReadAhead[] { null, readAhead }) {
            final var context = newContext(container, new ArrayList<>(), 0, fileLength, ra);
            try (var fileReader = fileReader(context)) {
                final var in = fileReader.openInput(fileLength);
                assertArrayEquals(bytes, in.readAllBytes());
                fileReader.verify();
            }
            assertThat(tasks.get(), equalTo(0));
        }

        readAhead.setEnabled(true);
        final var context = newContext(container, new ArrayList<>(), 0, fileLength, readAhead);
        try (var fileReader = fileReader(context)) {
            assertArrayEquals(bytes, fileReader.openInput(fileLength).readAllBytes());
            fileReader.verify();
        }
        assertThat(tasks.get(), greaterThan(0));
    }

    private void assertIdle() {
        try {
            assertBusy(() -> assertThat(((ThreadPoolExecutor) threadPool.executor(POOL)).getActiveCount(), equalTo(0)));
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }
}
