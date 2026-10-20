/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.repositories.gcs;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.core.Nullable;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.TimeUnit;

/**
 * DEBUG log of each GET that reads a blob, whichever bucket and purpose: what the connection delivered, how fast, and over which
 * connection, so that the rate of a connection can be told from the rate of the file that is read over it, and a slow bucket from a slow
 * client. A line is logged when the response body has been read to its end or is closed. It covers every client that the GCS service makes,
 * so the stateless object store (the data bucket) and the repositories (a backup bucket, a restore) alike. Nothing is wrapped or measured
 * unless the logger is at DEBUG when the GET is made.
 * <p>
 * The stateless snapshot source reads log a line per file under their own logger, with the same blob and range start, to be joined.
 */
final class GcsReadTiming {

    private static final Logger logger = LogManager.getLogger("org.elasticsearch.repositories.gcs.GetTiming");

    private static final double MIB = 1024 * 1024;

    private GcsReadTiming() {}

    static boolean isEnabled() {
        return logger.isDebugEnabled();
    }

    /**
     * @param content           the body of the response
     * @param requestStartNanos when the request was started
     * @param headersNanos      how long it took from then to the response headers
     * @param connection        what is known about the connection of the request
     * @return the body, which logs a line about the GET when it ends or is closed
     */
    static InputStream timed(
        InputStream content,
        String repository,
        String purpose,
        String bucket,
        String blob,
        long start,
        long end,
        long requestStartNanos,
        long headersNanos,
        @Nullable GcsSocketObserver.Connection connection
    ) {
        return new TimedInputStream(content, repository, purpose, bucket, blob, start, end, requestStartNanos, headersNanos, connection);
    }

    private static final class TimedInputStream extends FilterInputStream {
        private final String repository;
        private final String purpose;
        private final String bucket;
        private final String blob;
        private final long start;
        private final long end;
        private final long requestStartNanos;
        private final long headersNanos;
        @Nullable
        private final GcsSocketObserver.Connection connection;
        private long bytes;
        private long readNanos;
        private long firstByteNanos = -1;
        private boolean logged;

        TimedInputStream(
            InputStream in,
            String repository,
            String purpose,
            String bucket,
            String blob,
            long start,
            long end,
            long requestStartNanos,
            long headersNanos,
            @Nullable GcsSocketObserver.Connection connection
        ) {
            super(in);
            this.repository = repository;
            this.purpose = purpose;
            this.bucket = bucket;
            this.blob = blob;
            this.start = start;
            this.end = end;
            this.requestStartNanos = requestStartNanos;
            this.headersNanos = headersNanos;
            this.connection = connection;
        }

        @Override
        public int read() throws IOException {
            final long begin = System.nanoTime();
            int b = -1;
            try {
                b = in.read();
                return b;
            } finally {
                afterRead(begin, b < 0 ? b : 1);
            }
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            final long begin = System.nanoTime();
            int n = 0;
            try {
                n = in.read(b, off, len);
                return n;
            } finally {
                afterRead(begin, n);
            }
        }

        private void afterRead(long beginNanos, int n) {
            final long now = System.nanoTime();
            readNanos += now - beginNanos;
            if (n > 0) {
                bytes += n;
                if (firstByteNanos < 0) {
                    firstByteNanos = now - requestStartNanos;
                }
            } else if (n < 0) {
                log(now);
            }
        }

        @Override
        public void close() throws IOException {
            try {
                super.close();
            } finally {
                log(System.nanoTime());
            }
        }

        private void log(long nowNanos) {
            if (logged) {
                return;
            }
            logged = true;
            final long elapsedNanos = nowNanos - requestStartNanos;
            // the time from the first byte to the end is what the data took, which includes the time the reader did not read in
            final long dataNanos = firstByteNanos < 0 ? 0 : elapsedNanos - firstByteNanos;
            logger.debug(
                "GET repository [{}] purpose [{}] blob [{}/{}] range_start [{}] range_length [{}] bytes [{}] elapsed_ms [{}] "
                    + "first_byte_ms [{}] headers_ms [{}] "
                    + "in_read_ms [{}] rate_mib_s [{}] in_read_rate_mib_s [{}] {} thread [{}]",
                repository,
                purpose,
                bucket,
                blob,
                start,
                end >= Long.MAX_VALUE / 2 ? "to_end" : Long.toString(end - start + 1),
                bytes,
                TimeUnit.NANOSECONDS.toMillis(elapsedNanos),
                firstByteNanos < 0 ? -1 : TimeUnit.NANOSECONDS.toMillis(firstByteNanos),
                TimeUnit.NANOSECONDS.toMillis(headersNanos),
                TimeUnit.NANOSECONDS.toMillis(readNanos),
                rate(bytes, dataNanos),
                rate(bytes, readNanos),
                connection == null ? "connection unknown" : connection.toString(),
                Thread.currentThread().getName()
            );
        }

        private static String rate(long bytes, long nanos) {
            return nanos <= 0 ? "n/a" : String.format(java.util.Locale.ROOT, "%.1f", bytes / MIB / (nanos / 1e9));
        }
    }
}
