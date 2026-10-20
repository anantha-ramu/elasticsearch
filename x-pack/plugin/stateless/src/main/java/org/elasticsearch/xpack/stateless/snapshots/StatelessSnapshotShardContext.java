/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.BufferedChecksum;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotFailedException;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.ShardSnapshotResult;
import org.elasticsearch.repositories.SnapshotIndexCommit;
import org.elasticsearch.repositories.SnapshotShardContext;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.xpack.stateless.commits.BlobLocation;

import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import static org.elasticsearch.repositories.SnapshotShardContextHelper.withSnapshotIndexCommitRef;

/**
 * A {@link SnapshotShardContext} implementation that reads shard data from an object store instead of a local shard.
 */
public class StatelessSnapshotShardContext extends SnapshotShardContext {

    private static final Logger logger = LogManager.getLogger(StatelessSnapshotShardContext.class);

    private final ShardId shardId;
    @Nullable // when snapshot runs on a different node from the primary
    private final SnapshotIndexCommit snapshotIndexCommit;
    private final Map<String, BlobLocation> fileToBlobLocations;
    private final BiFunction<ShardId, Long, BlobContainer> blobContainerFunc;
    @Nullable // when snapshot runs on a different node from the primary
    private final BooleanSupplier isShardRelocated;
    @Nullable // when there is no executor to read ahead on
    private final SourceReadPrefetch sourceReadPrefetch;

    public StatelessSnapshotShardContext(
        ShardId shardId,
        SnapshotId snapshotId,
        IndexId indexId,
        @Nullable String shardStateIdentifier,
        IndexShardSnapshotStatus snapshotStatus,
        IndexVersion repositoryMetaVersion,
        long snapshotStartTime,
        @Nullable SnapshotIndexCommit snapshotIndexCommit,
        Map<String, BlobLocation> fileToBlobLocations,
        BiFunction<ShardId, Long, BlobContainer> blobContainerFunc,
        @Nullable BooleanSupplier isShardRelocated,
        ActionListener<ShardSnapshotResult> listener
    ) {
        this(
            shardId,
            snapshotId,
            indexId,
            shardStateIdentifier,
            snapshotStatus,
            repositoryMetaVersion,
            snapshotStartTime,
            snapshotIndexCommit,
            fileToBlobLocations,
            blobContainerFunc,
            isShardRelocated,
            null,
            listener
        );
    }

    StatelessSnapshotShardContext(
        ShardId shardId,
        SnapshotId snapshotId,
        IndexId indexId,
        @Nullable String shardStateIdentifier,
        IndexShardSnapshotStatus snapshotStatus,
        IndexVersion repositoryMetaVersion,
        long snapshotStartTime,
        @Nullable SnapshotIndexCommit snapshotIndexCommit,
        Map<String, BlobLocation> fileToBlobLocations,
        BiFunction<ShardId, Long, BlobContainer> blobContainerFunc,
        @Nullable BooleanSupplier isShardRelocated,
        @Nullable SourceReadPrefetch sourceReadPrefetch,
        ActionListener<ShardSnapshotResult> listener
    ) {
        super(snapshotId, indexId, shardStateIdentifier, snapshotStatus, repositoryMetaVersion, snapshotStartTime, listener);
        this.shardId = shardId;
        this.snapshotIndexCommit = snapshotIndexCommit;
        this.fileToBlobLocations = fileToBlobLocations;
        this.blobContainerFunc = blobContainerFunc;
        this.isShardRelocated = isShardRelocated;
        this.sourceReadPrefetch = sourceReadPrefetch;
    }

    @Override
    public ShardId shardId() {
        return shardId;
    }

    @Override
    public boolean isSearchableSnapshot() {
        return false;
    }

    @Override
    public Store.MetadataSnapshot metadataSnapshot() {
        try (
            var ignore = maybeWithSnapshotIndexCommitRef();
            var dir = new MetadataSnapshotBlobStoreDirectory(fileToBlobLocations, blobContainerFunc, shardId)
        ) {
            return Store.MetadataSnapshot.loadFromIndexCommit(null, dir, logger);
        } catch (IOException e) {
            throw new IndexShardSnapshotFailedException(shardId, "failed to get store file metadata", e);
        }
    }

    @Override
    public Collection<String> fileNames() {
        return fileToBlobLocations.keySet();
    }

    @Override
    public boolean assertFileContentsMatchHash(BlobStoreIndexShardSnapshot.FileInfo fileInfo) {
        try (
            var fileReader = fileReader(fileInfo.physicalName(), fileInfo.metadata());
            var indexInput = new Store.VerifyingIndexInput(
                new InputStreamIndexInput(fileInfo.physicalName(), fileReader.openInput(fileInfo.length()), fileInfo.length())
            )
        ) {
            final byte[] tmp = new byte[Math.toIntExact(fileInfo.metadata().length())];
            indexInput.readBytes(tmp, 0, tmp.length);
            assert fileInfo.metadata().hash().bytesEquals(new BytesRef(tmp));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return true;
    }

    @Override
    public void failStoreIfCorrupted(Exception e) {
        if (Lucene.isCorruptionException(e)) {
            logger.warn(
                Strings.format(
                    "Encountered corruption exception while reading snapshot data for shard %s, snapshot [%s]",
                    shardId,
                    snapshotId()
                ),
                e
            );
            // We don't fail the shard since the read is from the object store and not from the local shard
        }
    }

    @Override
    public FileReader fileReader(String file, StoreFileMetadata metadata) throws IOException {
        final Releasable commitRefReleasable = maybeWithSnapshotIndexCommitRef();
        final BlobLocation blobLocation = fileToBlobLocations.get(file);
        assert blobLocation != null : "Blob location for file [" + file + "] not found in " + fileToBlobLocations;
        final BlobContainer blobContainer = blobContainerFunc.apply(shardId, blobLocation.primaryTerm());
        return new BlobStoreFileReader(
            file,
            blobContainer,
            blobLocation,
            commitRefReleasable,
            isShardRelocated,
            sourceReadPrefetch,
            status()::ensureNotAborted
        );
    }

    private Releasable maybeWithSnapshotIndexCommitRef() {
        return snapshotIndexCommit != null
            ? withSnapshotIndexCommitRef(shardId, snapshotId(), snapshotIndexCommit, status(), isShardRelocated)
            : null;
    }

    static class BlobStoreFileReader implements SnapshotShardContext.FileReader {
        private final String fileName;
        @Nullable // when snapshot runs on a different node from the primary
        private final Releasable commitRefReleasable;
        private final BlobContainer blobContainer;
        private final BlobLocation blobLocation;
        private final FileChecksum fileChecksum;
        private long nextPositionInBlobFile; // relative to the start of the entire blob
        @Nullable // when snapshot runs on a different node from the primary
        private final BooleanSupplier isShardRelocated;
        private long readPosition; // relative to the start of the file
        private InputStream currentStream; // closed when openInput is called to create a new InputStream and on close()
        @Nullable // when the stream is read as it is opened
        private final SourceReadPrefetch sourceReadPrefetch;
        private final SourceReadPrefetch.Plan readPlan; // how this file is read, fixed when it starts
        private final Runnable abortCheck;
        @Nullable // when the read of the current stream is not logged
        private volatile SourceReadTiming.Stats currentStats;

        BlobStoreFileReader(
            String fileName,
            BlobContainer blobContainer,
            BlobLocation blobLocation,
            Releasable commitRefReleasable,
            @Nullable BooleanSupplier isShardRelocated
        ) {
            this(fileName, blobContainer, blobLocation, commitRefReleasable, isShardRelocated, null, () -> {});
        }

        BlobStoreFileReader(
            String fileName,
            BlobContainer blobContainer,
            BlobLocation blobLocation,
            Releasable commitRefReleasable,
            @Nullable BooleanSupplier isShardRelocated,
            @Nullable SourceReadPrefetch sourceReadPrefetch,
            Runnable abortCheck
        ) {
            this.fileName = fileName;
            this.blobContainer = blobContainer;
            this.blobLocation = blobLocation;
            this.commitRefReleasable = commitRefReleasable;
            this.fileChecksum = new FileChecksum(blobLocation.fileLength());
            this.nextPositionInBlobFile = blobLocation.offset();
            this.isShardRelocated = isShardRelocated;
            this.readPosition = 0;
            this.sourceReadPrefetch = sourceReadPrefetch;
            this.readPlan = sourceReadPrefetch == null ? SourceReadPrefetch.Plan.SINGLE : sourceReadPrefetch.plan();
            this.abortCheck = abortCheck;
        }

        @Override
        public InputStream openInput(long limit) throws IOException {
            assert (nextPositionInBlobFile + limit) - blobLocation.offset() <= blobLocation.fileLength()
                : "Requested limit exceeds file length, offset: "
                    + blobLocation.offset()
                    + ", position: "
                    + nextPositionInBlobFile
                    + ", limit: "
                    + limit
                    + ", file length: "
                    + blobLocation.fileLength();

            closeCurrentStream();
            assert assertPositionConsistency();

            currentStream = new ResettableBlobStoreInputStream(readPosition, limit);
            return currentStream;
        }

        @Override
        public void maybeReleaseCommitRef() {
            // When the shard relocates, the new primary will retain the snapshot commit so that it stays available in the object store.
            // This node has no need to retain the commit anymore. In fact, it should release it to allow the store to close.
            if (isShardRelocated != null && isShardRelocated.getAsBoolean()) {
                Releasables.close(commitRefReleasable);
            }
        }

        /**
         * Opens the ranged read of the blob. If the file is read ahead or in parallel, that happens here, below the checksum and the
         * position that {@link ResettableBlobStoreInputStream} keeps, which are only updated by the thread that reads the stream, with the
         * bytes in order, and above the snapshot rate limiter, which the caller of {@link #openInput} wraps around what it returns.
         */
        private InputStream openInputStreamForBlob(long bytesFromStreamStart, long limit) throws IOException {
            final long position = nextPositionInBlobFile + bytesFromStreamStart;
            final long length = limit - bytesFromStreamStart;
            final PrefetchingInputStream.RangeOpener ranges = (offset, len) -> blobContainer.readBlob(
                OperationPurpose.SNAPSHOT_DATA,
                blobLocation.blobName(),
                position + offset,
                len
            );
            if (sourceReadPrefetch == null) {
                return ranges.open(0, length);
            }
            // Before each read of the source, which a task makes on its own thread, and while the reader waits for a task, do what the
            // repository does before each read of the reader: stop if the snapshot was aborted, and let go of the commit if the shard
            // relocated. The snapshot would otherwise only notice when the reader reads again, which can be long after.
            final Runnable beforeRead = () -> {
                abortCheck.run();
                maybeReleaseCommitRef();
            };
            final String mode = SourceReadPrefetch.modeOf(readPlan, length);
            final SourceReadTiming.Stats stats = SourceReadTiming.isEnabled()
                ? new SourceReadTiming.Stats(fileName, blobLocation.blobName(), position, length, mode)
                : null;
            final InputStream stream;
            try {
                stream = sourceReadPrefetch.open(
                    readPlan,
                    length,
                    ranges,
                    beforeRead,
                    "file [" + fileName + "] blob [" + blobLocation.blobName() + "] from [" + position + "]",
                    stats
                );
            } catch (RuntimeException | Error | IOException e) {
                if (stats != null) {
                    stats.logAndRelease();
                }
                throw e;
            }
            currentStats = stats;
            // the reads of a stream that is not read ahead are the time the consumer waits for data; for one that is, it measures that
            return SourceReadTiming.maybeLog(stream, stats, stream instanceof PrefetchingInputStream == false);
        }

        /**
         * Tells the log of the current stream how long the snapshot rate limiter paused its reader.
         */
        @Override
        public void recordRateLimiterPause(long nanos) {
            final var stats = currentStats;
            if (stats != null) {
                stats.addLimiterPause(nanos);
            }
        }

        private boolean isPrefetching() {
            return sourceReadPrefetch != null && readPlan.mode() != SourceReadPrefetch.Mode.SINGLE;
        }

        private class ResettableBlobStoreInputStream extends FilterInputStream {
            private final long streamStartPosition; // relative to the start of the file
            private final long limit;
            private long markPosition; // relative to the start of the file
            private boolean closed;

            ResettableBlobStoreInputStream(long streamStartPosition, long limit) throws IOException {
                super(openInputStreamForBlob(0, limit));
                this.streamStartPosition = streamStartPosition;
                this.limit = limit;
                this.markPosition = streamStartPosition;
            }

            @Override
            public int read() throws IOException {
                ensureOpen();
                final int b = super.read();
                if (b != -1) {
                    fileChecksum.update(b, readPosition);
                    readPosition += 1;
                }
                return b;
            }

            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                ensureOpen();
                final int n = super.read(b, off, len);
                if (n != -1) {
                    fileChecksum.update(b, off, n, readPosition);
                    readPosition += n;
                }
                return n;
            }

            @Override
            public long skip(long n) throws IOException {
                assert false : "snapshot read does not skip bytes";
                throw new UnsupportedOperationException("blob store input stream for snapshot does not support skip()");
            }

            @Override
            public boolean markSupported() {
                return true;
            }

            @Override
            public void mark(int readlimit) {
                markPosition = readPosition;
            }

            /**
             * Reset closes the current input stream and reopens a new one at the marked position
             */
            @Override
            public void reset() throws IOException {
                ensureOpen();
                final var oldIn = in;
                final long bytesFromStreamStart = markPosition - streamStartPosition;
                if (isPrefetching()) {
                    // the buffers and the connections of the stream that is dropped are given back before the new one asks for its own
                    IOUtils.closeWhileHandlingException(oldIn);
                    in = openInputStreamForBlob(bytesFromStreamStart, limit);
                    readPosition = markPosition;
                } else {
                    in = openInputStreamForBlob(bytesFromStreamStart, limit);
                    readPosition = markPosition;
                    IOUtils.closeWhileHandlingException(oldIn);
                }
            }

            @Override
            public void close() throws IOException {
                if (closed == false) {
                    super.close();
                    closed = true;
                    nextPositionInBlobFile += limit;
                }
            }

            private void ensureOpen() throws IOException {
                if (closed) {
                    assert false : "stream is closed";
                    throw new IOException("stream is closed");
                }
            }
        }

        private boolean assertPositionConsistency() {
            assert readPosition + blobLocation.offset() == nextPositionInBlobFile
                : "input stream was not fully consumed before opening a new one, readPosition: "
                    + readPosition
                    + " + blobOffset: "
                    + blobLocation.offset()
                    + " vs nextPositionInBlobFile: "
                    + nextPositionInBlobFile;
            assert fileChecksum.position() == readPosition
                : "digest has not caught up with read position, digestPosition: "
                    + fileChecksum.position()
                    + " vs readPosition: "
                    + readPosition;
            return true;
        }

        @Override
        public void verify() throws IOException {
            closeCurrentStream();
            assert assertPositionConsistency();
            fileChecksum.verify(this.toString());
        }

        @Override
        public String toString() {
            return "BlobStoreFileReader{fileName='" + fileName + "', blobLocation=" + blobLocation + "}";
        }

        private void closeCurrentStream() throws IOException {
            if (currentStream != null) {
                currentStream.close();
            }
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(commitRefReleasable, this::closeCurrentStream);
        }
    }

    /**
     * Tracks the running CRC32 digest and stored checksum for a file being read from the blob store.
     * Bytes are fed via {@link #update} at their file positions. On mark/reset, the same positions
     * may be fed again; already-digested bytes are silently skipped.
     */
    static class FileChecksum {
        private final Checksum digest = new BufferedChecksum(new CRC32());
        private final long checksumPosition;
        private final byte[] checksum = new byte[Long.BYTES];
        private long position;

        FileChecksum(long fileLength) {
            this.checksumPosition = fileLength - Long.BYTES;
        }

        void update(int b, long readPosition) {
            assert readPosition <= position : "gap in checksum: readPosition=" + readPosition + " > position=" + position;
            if (readPosition >= position) {
                if (readPosition < checksumPosition) {
                    digest.update(b);
                } else {
                    checksum[(int) (readPosition - checksumPosition)] = (byte) b;
                }
                position = readPosition + 1;
            }
        }

        void update(byte[] b, int off, int n, long readPosition) {
            assert readPosition <= position : "gap in checksum: readPosition=" + readPosition + " > position=" + position;
            if (readPosition + n <= position) {
                return;
            }
            final int skip = (int) Math.max(0, position - readPosition);
            int bufPos = off + skip;
            long filePos = readPosition + skip;
            int remaining = n - skip;

            if (filePos < checksumPosition) {
                final int bytesToDigest = Math.toIntExact(Math.min(remaining, checksumPosition - filePos));
                digest.update(b, bufPos, bytesToDigest);
                bufPos += bytesToDigest;
                remaining -= bytesToDigest;
                filePos += bytesToDigest;
            }
            if (remaining > 0) {
                assert filePos >= checksumPosition && filePos - checksumPosition + remaining <= Long.BYTES;
                System.arraycopy(b, bufPos, checksum, (int) (filePos - checksumPosition), remaining);
            }
            position = readPosition + n;
        }

        long position() {
            return position;
        }

        void verify(String description) throws IOException {
            final var computedChecksum = digest.getValue();
            final var storedChecksum = CodecUtil.readBELong(new ByteArrayDataInput(checksum));
            if (computedChecksum != storedChecksum) {
                throw new CorruptIndexException(
                    "verification failed : calculated="
                        + Store.digestToString(computedChecksum)
                        + " stored="
                        + Store.digestToString(storedChecksum),
                    description
                );
            }
        }
    }

    // This class is only used in assertions, see assertFileContentsMatchHash
    private static class InputStreamIndexInput extends IndexInput {

        private final InputStream inputStream;
        private final long length;
        private long filePointer = 0L;

        InputStreamIndexInput(String resourceDescription, InputStream inputStream, long length) {
            super(resourceDescription);
            assert Assertions.ENABLED : "this class is only used for assertions";
            this.inputStream = inputStream;
            this.length = length;
        }

        @Override
        public byte readByte() throws IOException {
            final var read = inputStream.read();
            if (read == -1) {
                throw new EOFException();
            }
            filePointer += 1;
            return (byte) read;
        }

        @Override
        public void readBytes(byte[] b, int offset, int len) throws IOException {
            while (len > 0) {
                final var read = inputStream.read(b, offset, len);
                if (read == -1) {
                    throw new EOFException();
                }
                filePointer += read;
                len -= read;
                offset += read;
            }
        }

        @Override
        public void close() throws IOException {}

        @Override
        public long getFilePointer() {
            return filePointer;
        }

        @Override
        public long length() {
            return length;
        }

        @Override
        public void seek(long pos) throws IOException {
            if (filePointer == pos) {
                return;
            }
            throw new UnsupportedOperationException("seek is not supported");
        }

        @Override
        public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
            throw new UnsupportedOperationException("slice is not supported");
        }
    }
}
