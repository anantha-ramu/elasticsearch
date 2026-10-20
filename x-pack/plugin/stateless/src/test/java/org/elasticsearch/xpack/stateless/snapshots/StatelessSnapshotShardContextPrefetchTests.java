/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
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
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.CheckedSupplier;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.repositories.SnapshotIndexCommit;
import org.elasticsearch.repositories.SnapshotShardContext;
import org.elasticsearch.snapshots.AbortedSnapshotException;
import org.elasticsearch.snapshots.PausedSnapshotException;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.commits.BlobFile;
import org.elasticsearch.xpack.stateless.commits.BlobLocation;
import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.zip.CRC32;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;
import static org.mockito.Mockito.mock;

/**
 * Reads files through the stateless snapshot context as {@code snapshotFile} does: part by part, to the end, and then {@code verify()},
 * which compares the checksum of the bytes that were read with the one in the footer, as it is when the file is read as it always was, read
 * ahead on one connection, and read in ranges over several connections.
 */
public class StatelessSnapshotShardContextPrefetchTests extends ESTestCase {

    private static final String POOL = "test_snapshot_read";
    private static final String BLOB_NAME = "stateless_commit_42";
    private static final int PAGE = 32;
    private static final String TIMING_LOGGER = "org.elasticsearch.xpack.stateless.snapshots.SourceReadTiming";

    private ThreadPool threadPool;
    private final AtomicInteger tasks = new AtomicInteger();
    private Executor countingExecutor;

    private enum Mode {
        OFF,
        READ_AHEAD,
        PARALLEL
    }

    @Before
    public void createThreadPool() {
        threadPool = new TestThreadPool(
            getTestName(),
            new FixedExecutorBuilder(Settings.EMPTY, POOL, 12, 48, "test.snapshot_read", EsExecutors.TaskTrackingConfig.DO_NOT_TRACK)
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
        final var crc = new CRC32();
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

    private interface RangeReader {
        InputStream read(String blobName, long position, long length) throws IOException;
    }

    private SourceReadPrefetch prefetch(Mode mode, int rangeSize, SourceReadBufferPool pool) {
        final var prefetch = new SourceReadPrefetch(countingExecutor, pool);
        prefetch.setRangeSize(rangeSize);
        prefetch.setReadAhead(mode == Mode.READ_AHEAD);
        prefetch.setConnections(mode == Mode.PARALLEL ? between(2, 4) : 1);
        return prefetch;
    }

    private StatelessSnapshotShardContext newContext(
        RangeReader reader,
        IndexShardSnapshotStatus status,
        long blobOffset,
        long fileLength,
        SourceReadPrefetch prefetch
    ) {
        return newContext(reader, status, blobOffset, fileLength, prefetch, null, () -> false);
    }

    private StatelessSnapshotShardContext newContext(
        RangeReader reader,
        IndexShardSnapshotStatus status,
        long blobOffset,
        long fileLength,
        SourceReadPrefetch prefetch,
        SnapshotIndexCommit snapshotIndexCommit,
        BooleanSupplier isShardRelocated
    ) {
        final var shardId = new ShardId(new Index(randomIdentifier(), randomUUID()), 0);
        return new StatelessSnapshotShardContext(
            shardId,
            new SnapshotId(randomIdentifier(), randomUUID()),
            new IndexId(shardId.getIndexName(), randomUUID()),
            randomIdentifier(),
            status,
            IndexVersion.current(),
            randomNonNegativeLong(),
            snapshotIndexCommit,
            Map.of("file", new BlobLocation(new BlobFile(BLOB_NAME, new PrimaryTermAndGeneration(1L, 42L)), blobOffset, fileLength)),
            (s, g) -> {
                try {
                    return new FilterBlobContainer(newBlobContainer()) {
                        @Override
                        protected BlobContainer wrapChild(BlobContainer child) {
                            throw new AssertionError("should not obtain child");
                        }

                        @Override
                        public InputStream readBlob(OperationPurpose purpose, String blobName, long position, long length)
                            throws IOException {
                            assertThat(purpose, equalTo(OperationPurpose.SNAPSHOT_DATA));
                            return reader.read(blobName, position, length);
                        }
                    };
                } catch (IOException e) {
                    throw new AssertionError(e);
                }
            },
            isShardRelocated,
            prefetch,
            new PlainActionFuture<>()
        );
    }

    private static IndexShardSnapshotStatus newStatus() {
        return IndexShardSnapshotStatus.newInitializing(ShardGeneration.newGeneration(), randomLongBetween(1, Long.MAX_VALUE));
    }

    /** A store of the blob that is the file after some bytes, and tracks its connections. */
    private static RateLimitedFakeStore blobOf(byte[] file, int blobOffset, long readRate, long writeRate) {
        final var blob = new byte[blobOffset + file.length + between(0, 20)];
        random().nextBytes(blob);
        System.arraycopy(file, 0, blob, blobOffset, file.length);
        return new RateLimitedFakeStore(blob, readRate, writeRate);
    }

    private StatelessSnapshotShardContext contextOf(
        RateLimitedFakeStore store,
        int blobOffset,
        int fileLength,
        SourceReadPrefetch prefetch
    ) {
        return newContext((blob, position, length) -> {
            assertThat(blob, equalTo(BLOB_NAME));
            return store.get(position, length);
        }, newStatus(), blobOffset, fileLength, prefetch);
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

    private void assertIdle(SourceReadBufferPool pool, RateLimitedFakeStore store) throws Exception {
        assertBusy(() -> {
            assertThat(((ThreadPoolExecutor) threadPool.executor(POOL)).getActiveCount(), equalTo(0));
            assertThat("the buffers were given back", pool.getBytesInUse(), equalTo(0L));
            assertThat("every GET was closed", store.openGets.get(), equalTo(0));
        });
    }

    public void testSameBytesAndChecksumInEveryMode() throws Exception {
        final int rangeSize = between(1, 2048);
        final int fileLength = Math.max(CodecUtil.footerLength(), switch (between(0, 6)) {
            case 0 -> rangeSize - 1;
            case 1 -> rangeSize;
            case 2 -> rangeSize + 1;
            case 3 -> 2 * rangeSize;
            case 4 -> 2 * rangeSize + 1;
            case 5 -> between(1, 10 * rangeSize);
            case 6 -> CodecUtil.footerLength();
            default -> throw new AssertionError();
        });
        final var bytes = fileOfLength(fileLength);
        final int blobOffset = between(0, 50);
        final var parts = partLengths(fileLength);
        final boolean closeEachPart = randomBoolean();

        byte[] first = null;
        for (Mode mode : Mode.values()) {
            final var store = blobOf(bytes, blobOffset, 0, 0);
            store.maxRead = between(1, 3 * PAGE);
            final var pool = new SourceReadBufferPool(PAGE, Long.MAX_VALUE / 4);
            final var context = contextOf(store, blobOffset, fileLength, prefetch(mode, rangeSize, pool));
            tasks.set(0);
            final byte[] read;
            try (var fileReader = fileReader(context)) {
                read = readParts(fileReader, parts, fileLength, closeEachPart);
                fileReader.verify(); // the checksum was computed from the bytes in order, and is the one of the file
            }
            assertArrayEquals(bytes, read);
            if (first == null) {
                first = read;
            }
            assertArrayEquals(first, read);
            final boolean anyLongerThanARange = parts.stream().anyMatch(p -> p > rangeSize);
            if (mode == Mode.OFF) {
                assertThat("with the settings off the executor is not used", tasks.get(), equalTo(0));
            } else {
                assertThat(tasks.get() > 0, equalTo(anyLongerThanARange));
            }
            assertIdle(pool, store);
        }
    }

    public void testCorruptFileIsStillDetectedInEveryMode() throws Exception {
        final int rangeSize = between(1, 512);
        final int fileLength = between(2 * rangeSize + 1, 8 * rangeSize) + CodecUtil.footerLength();
        final var bytes = fileOfLength(fileLength);
        bytes[between(0, fileLength - 1)] ^= (byte) (1 << between(0, 7));
        final var store = blobOf(bytes, 0, 0, 0);
        final var pool = new SourceReadBufferPool(PAGE, Long.MAX_VALUE / 4);
        final var context = contextOf(store, 0, fileLength, prefetch(randomFrom(Mode.values()), rangeSize, pool));

        try (var fileReader = fileReader(context)) {
            readParts(fileReader, partLengths(fileLength), fileLength, randomBoolean());
            final var e = expectThrows(CorruptIndexException.class, fileReader::verify);
            assertThat(e.getMessage(), allOf(containsString("verification failed"), containsString(BLOB_NAME)));
        }
        assertIdle(pool, store);
    }

    /**
     * What the GCS resumable upload does after a retry: it marks the start of the stream, or a position within it, and resets to it, which
     * reopens the ranged read at the mark, so the prefetching of the stream before it has to be dropped and a new one started, without the
     * checksum seeing a byte twice.
     */
    public void testMarkAndResetInEveryMode() throws Exception {
        final int rangeSize = between(1, 512);
        final int fileLength = between(3 * rangeSize, 10 * rangeSize) + CodecUtil.footerLength();
        final var bytes = fileOfLength(fileLength);
        final var store = blobOf(bytes, 0, 0, 0);
        store.maxRead = between(1, 3 * PAGE);
        final var pool = new SourceReadBufferPool(PAGE, Long.MAX_VALUE / 4);
        final var context = contextOf(store, 0, fileLength, prefetch(randomFrom(Mode.values()), rangeSize, pool));

        try (var fileReader = fileReader(context)) {
            final var in = fileReader.openInput(fileLength);
            assertTrue(in.markSupported());
            final int beforeMark = between(0, fileLength - 2);
            assertArrayEquals(Arrays.copyOf(bytes, beforeMark), in.readNBytes(beforeMark));
            in.mark(Integer.MAX_VALUE);
            for (int resets = between(1, 3); resets > 0; resets--) {
                // read some, in the middle of a range or at the end of one, and start again from the mark
                final int afterMark = between(1, fileLength - beforeMark - 1);
                assertArrayEquals(Arrays.copyOfRange(bytes, beforeMark, beforeMark + afterMark), in.readNBytes(afterMark));
                in.reset();
            }
            assertArrayEquals(Arrays.copyOfRange(bytes, beforeMark, fileLength), in.readAllBytes());
            fileReader.verify();
        }
        assertIdle(pool, store);
    }

    public void testClosingTheFileReaderInTheMiddleOfAFileGivesEverythingBack() throws Exception {
        final int rangeSize = between(1, 512);
        final int fileLength = between(4 * rangeSize, 10 * rangeSize) + CodecUtil.footerLength();
        final var bytes = fileOfLength(fileLength);
        final var store = blobOf(bytes, 0, 0, 0);
        final var pool = new SourceReadBufferPool(PAGE, Long.MAX_VALUE / 4);
        final var context = contextOf(store, 0, fileLength, prefetch(randomFrom(Mode.READ_AHEAD, Mode.PARALLEL), rangeSize, pool));

        try (var fileReader = fileReader(context)) {
            final var in = fileReader.openInput(fileLength);
            final int some = between(1, fileLength - 1);
            assertArrayEquals(Arrays.copyOf(bytes, some), in.readNBytes(some));
        }
        assertIdle(pool, store);
    }

    /**
     * An abort, or a pause for the removal of the node, while the reader waits for a range that is blocked on the network: the reader
     * fails with the exception of the snapshot, no range is started after it, and the buffers and connections are given back once the
     * file reader is closed and the reads that were blocked have returned.
     */
    public void testAbortAndPauseStopThePrefetch() throws Exception {
        final boolean pause = randomBoolean();
        final int rangeSize = 64;
        final int fileLength = 40 * rangeSize + CodecUtil.footerLength();
        final var bytes = fileOfLength(fileLength);
        final var store = blobOf(bytes, 0, 0, 0);
        final int blockFrom = rangeSize * between(1, 6);
        final var blocked = new CountDownLatch(1);
        final var release = new CountDownLatch(1);
        store.beforeRead = position -> {
            if (position >= blockFrom) {
                blocked.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    throw new IOException(e);
                }
            }
        };
        final var status = newStatus();
        final var pool = new SourceReadBufferPool(PAGE, Long.MAX_VALUE / 4);
        final var context = newContext(
            (blob, position, length) -> store.get(position, length),
            status,
            0,
            fileLength,
            prefetch(randomFrom(Mode.READ_AHEAD, Mode.PARALLEL), rangeSize, pool)
        );

        final var readerFailure = new java.util.concurrent.atomic.AtomicReference<Throwable>();
        final var readerDone = new CountDownLatch(1);
        try (var fileReader = fileReader(context)) {
            final var in = fileReader.openInput(fileLength);
            threadPool.generic().execute(() -> {
                try {
                    in.readAllBytes();
                } catch (Throwable t) {
                    readerFailure.set(t);
                } finally {
                    readerDone.countDown();
                }
            });
            safeAwait(blocked);
            if (pause) {
                status.pauseIfNotCompleted(listener -> listener.onResponse(null));
            } else {
                status.abortIfNotCompleted("test", listener -> listener.onResponse(null));
            }
            safeAwait(readerDone);
            assertThat(readerFailure.get(), instanceOf(pause ? PausedSnapshotException.class : AbortedSnapshotException.class));
            final int gets = store.gets.get();
            release.countDown();
            assertIdle(pool, store); // the tasks stop at their next read, without the file reader being closed
            assertThat("no read was started after the abort", store.gets.get(), equalTo(gets));
        }
        assertIdle(pool, store);
    }

    /**
     * When the shard relocates the snapshot gives up the commit it holds on this node, which the repository has the reader do before each
     * of its reads. A reader that waits for a range may wait for a long time, so it does that while it waits as well, as the snapshot that
     * is blocked in a read of the source would, until the read returns.
     */
    public void testCommitIsReleasedOnRelocationWhileTheReaderWaitsForARange() throws Exception {
        final int rangeSize = 64;
        final int fileLength = 40 * rangeSize + CodecUtil.footerLength();
        final var bytes = fileOfLength(fileLength);
        final var store = blobOf(bytes, 0, 0, 0);
        final var blocked = new CountDownLatch(1);
        final var release = new CountDownLatch(1);
        store.beforeRead = position -> {
            if (position >= rangeSize) {
                blocked.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    throw new IOException(e);
                }
            }
        };
        final var commitReleased = new AtomicBoolean();
        final var snapshotIndexCommit = new SnapshotIndexCommit(new Engine.IndexCommitRef(null, () -> commitReleased.set(true)));
        final var relocated = new AtomicBoolean();
        final var pool = new SourceReadBufferPool(PAGE, Long.MAX_VALUE / 4);
        final var context = newContext(
            (blob, position, length) -> store.get(position, length),
            newStatus(),
            0,
            fileLength,
            prefetch(randomFrom(Mode.READ_AHEAD, Mode.PARALLEL), rangeSize, pool),
            snapshotIndexCommit,
            relocated::get
        );
        try (var fileReader = fileReader(context)) {
            // the reader is the only one that holds the commit
            snapshotIndexCommit.releaseInitialReference();
            final var in = fileReader.openInput(fileLength);
            final var done = new CountDownLatch(1);
            threadPool.generic().execute(() -> {
                try {
                    assertThat(in.readAllBytes().length, equalTo(fileLength));
                } catch (IOException e) {
                    throw new AssertionError(e);
                } finally {
                    done.countDown();
                }
            });
            safeAwait(blocked);
            assertFalse(commitReleased.get());
            relocated.set(true);
            assertBusy(() -> assertTrue("released while the reader waits", commitReleased.get()));
            release.countDown();
            safeAwait(done);
        }
        assertIdle(pool, store);
    }

    /**
     * What the repository relies on in a file that is read by the thread of the snapshot: when the shard has relocated, the first read
     * after that lets go of the commit, before it blocks on the network. For a file that is read ahead that read is made by a task, which
     * has to do the same, as the thread of the snapshot may not read again for a long time.
     */
    public void testCommitIsReleasedBeforeTheNextReadOfARangeAfterRelocation() throws Exception {
        final int rangeSize = 64;
        final int fileLength = 40 * rangeSize + CodecUtil.footerLength();
        final var bytes = fileOfLength(fileLength);
        final var store = blobOf(bytes, 0, 0, 0);
        final var blockedFirst = new CountDownLatch(1);
        final var releaseFirst = new CountDownLatch(1);
        final var blockedSecond = new CountDownLatch(1);
        final var releaseSecond = new CountDownLatch(1);
        final var reads = new AtomicInteger();
        store.beforeRead = position -> {
            final int read = reads.incrementAndGet();
            if (read <= 2) {
                (read == 1 ? blockedFirst : blockedSecond).countDown();
                try {
                    (read == 1 ? releaseFirst : releaseSecond).await();
                } catch (InterruptedException e) {
                    throw new IOException(e);
                }
            }
        };
        final var commitReleased = new AtomicBoolean();
        final var snapshotIndexCommit = new SnapshotIndexCommit(new Engine.IndexCommitRef(null, () -> commitReleased.set(true)));
        final var relocated = new AtomicBoolean();
        final var pool = new SourceReadBufferPool(PAGE, Long.MAX_VALUE / 4);
        final var context = newContext(
            (blob, position, length) -> store.get(position, length),
            newStatus(),
            0,
            fileLength,
            prefetch(Mode.READ_AHEAD, rangeSize, pool),
            snapshotIndexCommit,
            relocated::get
        );
        try (var fileReader = fileReader(context)) {
            snapshotIndexCommit.releaseInitialReference();
            final var in = fileReader.openInput(fileLength);
            final var done = new CountDownLatch(1);
            threadPool.generic().execute(() -> {
                try {
                    assertThat(in.readAllBytes().length, equalTo(fileLength));
                } catch (IOException e) {
                    throw new AssertionError(e);
                } finally {
                    done.countDown();
                }
            });
            safeAwait(blockedFirst);
            relocated.set(true);
            releaseFirst.countDown();
            safeAwait(blockedSecond);
            assertTrue("released before the second read blocked", commitReleased.get());
            releaseSecond.countDown();
            safeAwait(done);
        }
        assertIdle(pool, store);
    }

    /**
     * What the streaming upload's resume does: the source is reset to the mark while a range that is further on is in flight, blocked on
     * the network. The stream that is dropped must not hand the new one a stale range, and must give back everything it holds once the
     * blocked read returns.
     */
    public void testResetWhileARangeIsBlockedInFlight() throws Exception {
        final int rangeSize = 64;
        final int fileLength = 30 * rangeSize + CodecUtil.footerLength();
        final var bytes = fileOfLength(fileLength);
        final var store = blobOf(bytes, 0, 0, 0);
        final int blockedRange = between(2, 6);
        final int blockFrom = blockedRange * rangeSize;
        final var blocked = new CountDownLatch(1);
        final var release = new CountDownLatch(1);
        final var blockedOnce = new AtomicBoolean();
        store.beforeRead = position -> {
            // the first read of the range blocks, the reads of the stream that replaces it do not
            if (position >= blockFrom && blockedOnce.compareAndSet(false, true)) {
                blocked.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    throw new IOException(e);
                }
            }
        };
        final var pool = new SourceReadBufferPool(PAGE, Long.MAX_VALUE / 4);
        final var context = contextOf(store, 0, fileLength, prefetch(randomFrom(Mode.READ_AHEAD, Mode.PARALLEL), rangeSize, pool));
        try (var fileReader = fileReader(context)) {
            final var in = fileReader.openInput(fileLength);
            final int mark = between(0, rangeSize - 1);
            assertArrayEquals(Arrays.copyOf(bytes, mark), in.readNBytes(mark));
            in.mark(Integer.MAX_VALUE);
            // as far as the range before the blocked one, which has the blocked one in flight behind it
            final int upTo = (blockedRange - 1) * rangeSize + between(1, rangeSize);
            assertArrayEquals(Arrays.copyOfRange(bytes, mark, upTo), in.readNBytes(upTo - mark));
            safeAwait(blocked);

            in.reset(); // while the range is blocked
            release.countDown();

            assertArrayEquals(Arrays.copyOfRange(bytes, mark, fileLength), in.readAllBytes());
            fileReader.verify();
        }
        assertIdle(pool, store); // the GET of the blocked range was closed by the task, once it stopped
    }

    public void testFailureOfAReadSurfacesToTheConsumer() throws Exception {
        final int rangeSize = between(PAGE, 256);
        final int fileLength = between(4 * rangeSize, 10 * rangeSize) + CodecUtil.footerLength();
        final var bytes = fileOfLength(fileLength);
        final var store = blobOf(bytes, 0, 0, 0);
        final int failAt = between(0, fileLength - 1);
        store.failFromPosition = failAt;
        final var pool = new SourceReadBufferPool(PAGE, Long.MAX_VALUE / 4);
        final var context = contextOf(store, 0, fileLength, prefetch(randomFrom(Mode.READ_AHEAD, Mode.PARALLEL), rangeSize, pool));

        try (var fileReader = fileReader(context)) {
            final var in = fileReader.openInput(fileLength);
            final var e = expectThrows(IOException.class, in::readAllBytes);
            assertThat(e.getMessage(), containsString("fetch failed at "));
        }
        assertIdle(pool, store);
    }

    /**
     * The upload reads a chunk and writes it in turn, over connections that are each capped, in both directions. Read ahead overlaps the
     * reads with the writes, so the file takes the time of the slower of the two instead of both.
     */
    public void testReadAheadOverlapsTheReadsWithTheWritesOfTheUpload() throws Exception {
        assertTimingWithRetries(3, this::testReadAheadOverlapsTheReadsWithTheWritesOfTheUploadOnce);
    }

    private String testReadAheadOverlapsTheReadsWithTheWritesOfTheUploadOnce() throws Exception {
        final int fileLength = 1024 * 1024;
        final var bytes = fileOfLength(fileLength);
        final long rate = 8L * 1024 * 1024;
        final int rangeSize = 128 * 1024;

        final var off = blobOf(bytes, 0, rate, rate);
        final long offNanos = upload(
            off,
            fileLength,
            prefetch(Mode.OFF, rangeSize, new SourceReadBufferPool(32 * 1024, Long.MAX_VALUE / 4))
        );
        assertThat("without read-ahead the reads never overlap the writes", off.writesDuringReads.get(), equalTo(0));

        final var pool = new SourceReadBufferPool(32 * 1024, Long.MAX_VALUE / 4);
        final var ahead = blobOf(bytes, 0, rate, rate);
        final long aheadNanos = upload(ahead, fileLength, prefetch(Mode.READ_AHEAD, rangeSize, pool));
        assertThat(
            "most writes happened while a read was in progress, writes [" + ahead.writes.get() + "]",
            ahead.writesDuringReads.get(),
            greaterThan(ahead.writes.get() / 2)
        );
        assertThat("one connection", ahead.maxOpenGets.get(), equalTo(1));
        final String timing = (double) aheadNanos < 0.85 * offNanos
            ? null
            : "read-ahead [" + aheadNanos + "ns] vs off [" + offNanos + "ns]";
        assertIdle(pool, ahead);
        return timing;
    }

    /**
     * Ranges are fetched over several connections at once, each capped, and the consumer still reads the bytes in order, so reading the
     * file is faster than over the one connection.
     */
    public void testParallelRangesOverlapAndReadFasterThanOneConnection() throws Exception {
        assertTimingWithRetries(3, this::testParallelRangesOverlapAndReadFasterThanOneConnectionOnce);
    }

    private String testParallelRangesOverlapAndReadFasterThanOneConnectionOnce() throws Exception {
        final int fileLength = 1024 * 1024;
        final var bytes = fileOfLength(fileLength);
        final long rate = 8L * 1024 * 1024;
        final int rangeSize = 128 * 1024;

        final var off = blobOf(bytes, 0, rate, 0);
        final long offNanos = readOnly(
            off,
            fileLength,
            prefetch(Mode.OFF, rangeSize, new SourceReadBufferPool(32 * 1024, Long.MAX_VALUE / 4))
        );
        assertThat(off.maxOpenGets.get(), equalTo(1));

        final var pool = new SourceReadBufferPool(32 * 1024, Long.MAX_VALUE / 4);
        final var parallel = blobOf(bytes, 0, rate, 0);
        final var prefetch = prefetch(Mode.PARALLEL, rangeSize, pool);
        final int connections = prefetch.plan().connections();
        final long parallelNanos = readOnly(parallel, fileLength, prefetch);
        assertThat("ranges were fetched at the same time", parallel.maxOpenGets.get(), greaterThan(1));
        assertThat(parallel.maxOpenGets.get(), org.hamcrest.Matchers.lessThanOrEqualTo(connections));
        final String timing = (double) parallelNanos < 0.8 * offNanos
            ? null
            : "parallel [" + parallelNanos + "ns] vs off [" + offNanos + "ns]";
        assertIdle(pool, parallel);
        return timing;
    }

    private long upload(RateLimitedFakeStore store, int fileLength, SourceReadPrefetch prefetch) throws IOException {
        final var context = contextOf(store, 0, fileLength, prefetch);
        final long start = System.nanoTime();
        try (var fileReader = fileReader(context)) {
            try (var in = fileReader.openInput(fileLength); var out = store.upload()) {
                final var buffer = new byte[32 * 1024];
                int n;
                while ((n = in.read(buffer)) >= 0) {
                    out.write(buffer, 0, n);
                }
            }
            fileReader.verify();
        }
        assertThat(store.uploadedBytes.get(), equalTo(fileLength));
        return System.nanoTime() - start;
    }

    private long readOnly(RateLimitedFakeStore store, int fileLength, SourceReadPrefetch prefetch) throws IOException {
        final var context = contextOf(store, 0, fileLength, prefetch);
        final long start = System.nanoTime();
        try (var fileReader = fileReader(context)) {
            assertThat(fileReader.openInput(fileLength).readAllBytes().length, equalTo(fileLength));
            fileReader.verify();
        }
        return System.nanoTime() - start;
    }

    public void testSettingsPickTheModeOfTheNextFile() {
        final var clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Set.of(
                StatelessSnapshotSettings.SOURCE_READ_AHEAD_SETTING,
                StatelessSnapshotSettings.SOURCE_READ_PARALLEL_CONNECTIONS_SETTING,
                StatelessSnapshotSettings.SOURCE_READ_RANGE_SIZE_SETTING,
                StatelessSnapshotSettings.SOURCE_READ_MAX_BUFFER_MEMORY_SETTING
            )
        );
        final var prefetch = SourceReadPrefetch.create(countingExecutor, clusterSettings);
        assertThat("everything is off by default", prefetch.plan(), equalTo(SourceReadPrefetch.Plan.SINGLE));
        assertThat("a share of the heap", prefetch.getPool().getCapBytes(), greaterThan(0L));

        clusterSettings.applySettings(Settings.builder().put(StatelessSnapshotSettings.SOURCE_READ_AHEAD_SETTING.getKey(), true).build());
        assertThat(prefetch.plan().mode(), equalTo(SourceReadPrefetch.Mode.READ_AHEAD));
        assertThat(prefetch.plan().describe(), equalTo("readahead"));
        assertThat(prefetch.plan().rangeSize(), equalTo(4 * 1024 * 1024));

        clusterSettings.applySettings(
            Settings.builder()
                .put(StatelessSnapshotSettings.SOURCE_READ_AHEAD_SETTING.getKey(), true)
                .put(StatelessSnapshotSettings.SOURCE_READ_PARALLEL_CONNECTIONS_SETTING.getKey(), 3)
                .put(StatelessSnapshotSettings.SOURCE_READ_RANGE_SIZE_SETTING.getKey(), "1mb")
                .put(StatelessSnapshotSettings.SOURCE_READ_MAX_BUFFER_MEMORY_SETTING.getKey(), "10mb")
                .build()
        );
        assertThat("the parallel read includes the overlap, so it wins", prefetch.plan().mode(), equalTo(SourceReadPrefetch.Mode.PARALLEL));
        assertThat(prefetch.plan().describe(), equalTo("parallel 3"));
        assertThat(prefetch.plan().rangeSize(), equalTo(1024 * 1024));
        assertThat(prefetch.getPool().getCapBytes(), equalTo(ByteSizeValue.ofMb(10).getBytes()));

        final var inFlight = prefetch.getPool().tryAcquire(1);
        assertNotNull(inFlight);
        clusterSettings.applySettings(Settings.EMPTY);
        inFlight.release(); // a read that was still going when the settings were switched off gives its pages back
        assertThat("and they are not kept", prefetch.getPool().getFreePages(), equalTo(0L));
        assertThat(prefetch.plan(), equalTo(SourceReadPrefetch.Plan.SINGLE));
        assertThat("the pages that were kept are dropped", prefetch.getPool().getFreePages(), equalTo(0L));

        // and they are kept again once a mode is on
        clusterSettings.applySettings(Settings.builder().put(StatelessSnapshotSettings.SOURCE_READ_AHEAD_SETTING.getKey(), true).build());
        prefetch.getPool().tryAcquire(1).release();
        assertThat(prefetch.getPool().getFreePages(), equalTo(1L));
    }

    public void testPageIsNotHumongousAndTheCapDefaultsToAShareOfTheHeap() {
        assertThat(StatelessSnapshotSettings.SOURCE_READ_MAX_BUFFER_MEMORY_SETTING.get(Settings.EMPTY).getBytes(), greaterThan(0L));
        assertThat(
            StatelessSnapshotSettings.SOURCE_READ_MAX_BUFFER_MEMORY_SETTING.get(Settings.EMPTY).getBytes(),
            lessThan(Runtime.getRuntime().maxMemory() / 10)
        );
        assertThat(StatelessSnapshotSettings.SOURCE_READ_PARALLEL_CONNECTIONS_SETTING.get(Settings.EMPTY), equalTo(1));
        assertFalse(StatelessSnapshotSettings.SOURCE_READ_AHEAD_SETTING.get(Settings.EMPTY));
    }

    /**
     * The line that is logged for a file when its stream is closed, with how the read was done and how long the reader waited, and the
     * pause of the rate limiter that the repository tells the file reader about.
     */
    public void testFileLineIsLoggedAtDebug() throws Exception {
        final int rangeSize = 100;
        final int fileLength = 10 * rangeSize + CodecUtil.footerLength();
        final var bytes = fileOfLength(fileLength);
        final var logger = LogManager.getLogger(TIMING_LOGGER);
        final var originalLevel = logger.getLevel();
        Loggers.setLevel(logger, Level.DEBUG);
        try (var mockLog = MockLog.capture(TIMING_LOGGER)) {
            for (Mode mode : Mode.values()) {
                final String expectedMode = switch (mode) {
                    case OFF -> "single";
                    case READ_AHEAD -> "readahead";
                    case PARALLEL -> "parallel \\d";
                };
                mockLog.addExpectation(
                    new MockLog.PatternSeenEventExpectation(
                        "file line in mode " + mode,
                        TIMING_LOGGER,
                        Level.DEBUG,
                        "file \\[file\\] blob \\["
                            + BLOB_NAME
                            + "\\] from \\[0\\]: mode \\["
                            + expectedMode
                            + "\\] length \\["
                            + fileLength
                            + "\\] "
                            + "delivered \\["
                            + fileLength
                            + "\\] bytes in \\[\\d+\\] ms, consumer waited for data \\[\\d+\\] ms, "
                            + "limiter paused \\[5\\] ms, \\[\\d+\\] fetches took \\[\\d+\\] ms in total with at most \\[\\d\\] in flight, "
                            + "\\[\\d+\\] ranges read directly \\(\\[\\d+\\] of them because the buffer memory was used up\\), "
                            + "\\[0\\] other readers of the blob when it started"
                    )
                );
                final var store = blobOf(bytes, 0, 0, 0);
                final var pool = new SourceReadBufferPool(PAGE, Long.MAX_VALUE / 4);
                final var context = contextOf(store, 0, fileLength, prefetch(mode, rangeSize, pool));
                try (var fileReader = fileReader(context)) {
                    final var in = fileReader.openInput(fileLength);
                    fileReader.recordRateLimiterPause(5_000_000);
                    assertThat(in.readAllBytes().length, equalTo(fileLength));
                }
            }
            mockLog.assertAllExpectationsMatched();
        } finally {
            Loggers.setLevel(logger, originalLevel);
        }
    }

    /**
     * The tests of speed compare the times of two reads that sleep to stay under a rate, which a machine that is busy with something
     * else stretches. Each try checks, and fails on, what has to hold however slow the machine is: how many reads were open, and whether
     * they overlapped. Only when the times do not compare as they should is the try made again, twice, before that is a failure.
     *
     * @param attempt the try, which returns null if the times compared as they should, and what they were if they did not
     */
    private void assertTimingWithRetries(int attempts, CheckedSupplier<String, Exception> attempt) throws Exception {
        String last = null;
        for (int i = 1; i <= attempts; i++) {
            last = attempt.get();
            if (last == null) {
                if (i > 1) {
                    logger.info("the times compared as they should in try [{}] of [{}]", i, attempts);
                }
                return;
            }
        }
        fail("the times did not compare as they should in [" + attempts + "] tries, the last was " + last);
    }
}
