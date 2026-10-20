/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.repositories.gcs;

import com.google.cloud.WriteChannel;
import com.google.cloud.storage.BlobInfo;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.core.SuppressForbidden;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.TimeUnit;

/**
 * DEBUG timing of an upload through the SDK write channel, so that the SDK path can be compared with the streaming upload (see
 * {@link GcsStreamingUpload}) piece by piece. The SDK buffers a chunk (16MiB unless configured) and sends it, and waits for the answer,
 * inside the write that fills the chunk, during which nothing is read from the source.
 *
 * Only used when the logger is enabled for DEBUG; the upload does not create or call any of this otherwise.
 *
 * For each piece, a line with the purpose of the upload (the same blob store serves snapshot data and, in stateless, commit uploads), the
 * bytes flushed, the time the flushing write took, and the time spent blocked in reads of the source
 * since the previous flush. The SDK does not say when it flushes, so a write is taken for a flush when it crosses a multiple of the chunk
 * size or when it took long, as copying into the chunk buffer takes microseconds. The close of the channel sends the last chunk.
 */
final class ResumableTiming {

    static final Logger logger = LogManager.getLogger("org.elasticsearch.repositories.gcs.ResumableTiming");

    /** a write that takes this long cannot have been just a copy into the chunk buffer */
    private static final long FLUSH_THRESHOLD_NANOS = TimeUnit.MILLISECONDS.toNanos(1);

    static boolean isEnabled() {
        return logger.isDebugEnabled();
    }

    private final OperationPurpose purpose;
    private final String description;
    private final long chunkSize;
    private final long startNanos = System.nanoTime();

    private long written;
    private long pieces;
    private long reads;
    private long readNanosSincePiece;
    private long readsSincePiece;
    private long readNanos;
    private long flushNanos;

    ResumableTiming(OperationPurpose purpose, BlobInfo blobInfo, long chunkSize) {
        this.purpose = purpose;
        this.description = blobInfo.getBucket() + "/" + blobInfo.getName();
        this.chunkSize = chunkSize;
    }

    InputStream wrap(InputStream in) {
        return new FilterInputStream(in) {
            @Override
            public int read() throws IOException {
                final long start = System.nanoTime();
                try {
                    return super.read();
                } finally {
                    sourceRead(System.nanoTime() - start);
                }
            }

            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                final long start = System.nanoTime();
                try {
                    return super.read(b, off, len);
                } finally {
                    sourceRead(System.nanoTime() - start);
                }
            }
        };
    }

    WritableByteChannel wrap(WritableByteChannel channel) {
        return new WritableByteChannel() {
            @SuppressForbidden(reason = "times the write of the channel that the upload writes to")
            @Override
            public int write(ByteBuffer src) throws IOException {
                final int positionBefore = src.position();
                final long start = System.nanoTime();
                final int n;
                try {
                    n = channel.write(src);
                } finally {
                    final long took = System.nanoTime() - start;
                    final long before = written;
                    // count the bytes consumed, also when the write failed after taking some
                    written += src.position() - positionBefore;
                    if (written / chunkSize > before / chunkSize || took >= FLUSH_THRESHOLD_NANOS) {
                        pieceFlushed(took, chunkSize, false);
                    }
                }
                return n;
            }

            @Override
            public boolean isOpen() {
                return channel.isOpen();
            }

            @Override
            public void close() throws IOException {
                channel.close();
            }
        };
    }

    /**
     * Closes the channel, which sends the last chunk and finalizes the blob, and logs the totals.
     */
    void close(WriteChannel channel) throws IOException {
        final long start = System.nanoTime();
        try {
            channel.close();
        } finally {
            pieceFlushed(System.nanoTime() - start, written % chunkSize, true);
            logger.debug(
                "uploaded [{}] purpose [{}] bytes [{}] pieces [{}] total_ms [{}] source_read_ms [{}] reads [{}] flush_ms [{}]",
                description,
                purpose,
                written,
                pieces,
                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos),
                TimeUnit.NANOSECONDS.toMillis(readNanos),
                reads,
                TimeUnit.NANOSECONDS.toMillis(flushNanos)
            );
        }
    }

    private void sourceRead(long nanos) {
        reads++;
        readsSincePiece++;
        readNanos += nanos;
        readNanosSincePiece += nanos;
    }

    private void pieceFlushed(long flushNanosOfPiece, long bytes, boolean last) {
        logger.debug(
            "piece [{}] of [{}] purpose [{}] {}end_offset [{}] flushed_bytes [{}] flush_ms [{}] source_read_ms [{}] reads [{}]",
            pieces,
            description,
            purpose,
            last ? "last " : "",
            written,
            bytes,
            TimeUnit.NANOSECONDS.toMillis(flushNanosOfPiece),
            TimeUnit.NANOSECONDS.toMillis(readNanosSincePiece),
            readsSincePiece
        );
        pieces++;
        flushNanos += flushNanosOfPiece;
        readNanosSincePiece = 0;
        readsSincePiece = 0;
    }
}
