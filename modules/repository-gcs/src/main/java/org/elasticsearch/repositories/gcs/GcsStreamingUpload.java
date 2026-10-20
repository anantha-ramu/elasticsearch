/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.repositories.gcs;

import com.google.api.client.http.EmptyContent;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpContent;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.json.JsonHttpContent;
import com.google.api.client.json.JsonFactory;
import com.google.api.services.storage.model.StorageObject;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.StorageException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.repositories.gcs.GcsRepositoryStatsCollector.StreamingUploadCounters;
import org.elasticsearch.repositories.gcs.GcsStreamingUploadSettings.Config;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.nio.ByteBuffer;
import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.CRC32C;

/**
 * Uploads one blob through a GCS resumable upload session, streaming the source into large parts instead of going through the SDK write
 * channel, which buffers a chunk (16MiB by default) in memory and waits for the response to every chunk before it reads more.
 *
 * The blob is sent as parts of {@link Config#getPartSize()} bytes. Each part is one PUT with a Content-Range, whose body is copied from the
 * source through a single buffer of {@link #COPY_BUFFER_SIZE} bytes. The length of the part is declared up front, which makes
 * HttpURLConnection stream the body to the socket (fixed-length streaming mode) without buffering it, so the heap used by an upload does
 * not depend on the part size.
 *
 * Failures of the destination (IOException, 408, 429, 5xx) are retried with backoff. A retry first asks the session how many bytes it has
 * committed, then rewinds the source to the start of the part ({@link InputStream#reset()}, which for snapshot sources reopens the file at
 * the mark), reads and discards up to the committed offset, and sends the rest. Exceptions thrown by the source are never retried.
 *
 * Integrity: an md5 given in the {@link BlobInfo} is sent with the session metadata, and GCS verifies it when the object is finalized, the
 * same as for the SDK path. Like the SDK, a CRC32C of the whole blob is also sent in the x-goog-hash header of the last request, where GCS
 * checks it against the object it assembled. A header precedes the body, so the CRC of the last bytes has to be known before they are sent:
 * the last 256kb or less of the blob is not streamed but read into the copy buffer first, and sent as a request of its own. The CRC is
 * computed over the bytes as they are first read from the source, whether that is for sending or for reading up to a committed offset.
 * That is one request more per blob, which {@link GcsStreamingUploadSettings#VERIFY_CRC32C_SETTING} switches off.
 *
 * Instances are single-use and not thread-safe.
 */
final class GcsStreamingUpload {

    static final Logger logger = LogManager.getLogger("org.elasticsearch.repositories.gcs.StreamingUpload");

    /**
     * The only per-upload buffer. Below the humongous object size of G1 for any region size.
     */
    static final int COPY_BUFFER_SIZE = 256 * 1024;

    private static final long PART_ALIGNMENT = GcsStreamingUploadSettings.PART_ALIGNMENT_BYTES;
    private static final String OCTET_STREAM = "application/octet-stream";
    private static final Pattern COMMITTED_RANGE = Pattern.compile("bytes=0-(\\d+)");

    private final HttpRequestFactory requestFactory;
    private final JsonFactory jsonFactory;
    private final String uploadUrl;
    private final BlobInfo blobInfo;
    private final boolean failIfAlreadyExists;
    private final InputStream source;
    private final long size;
    private final Config config;
    private final OperationStats stats;
    private final StreamingUploadCounters counters;
    private final ThreadPool threadPool;
    private final String description;
    private final byte[] buffer = new byte[COPY_BUFFER_SIZE];

    @Nullable
    private Watchdog watchdog;
    private String sessionUri;
    private boolean completed;

    /** absolute offset of the next byte that the source returns */
    private long sourcePosition;
    /** absolute offset the source was last marked at */
    private long markOffset;

    private final CRC32C crc32c = new CRC32C();
    /** the CRC32C covers the bytes of the source before this offset */
    private long hashedUpTo;
    /** the blob ends in a request of its own, sent from the copy buffer and carrying the CRC32C of the blob */
    private long streamedEnd;
    private boolean tailLoaded;

    private int failureStreak;
    private long failureStreakStartNanos;
    private final List<Exception> failures = new ArrayList<>();
    private long discardNanos;

    GcsStreamingUpload(
        HttpRequestFactory requestFactory,
        JsonFactory jsonFactory,
        String host,
        BlobInfo blobInfo,
        boolean failIfAlreadyExists,
        InputStream source,
        long size,
        Config config,
        OperationStats stats,
        StreamingUploadCounters counters,
        ThreadPool threadPool
    ) {
        assert config.isEnabled();
        assert source.markSupported();
        assert size > 0;
        this.requestFactory = requestFactory;
        this.jsonFactory = jsonFactory;
        this.uploadUrl = (host.endsWith("/") ? host.substring(0, host.length() - 1) : host)
            + "/upload/storage/v1/b/"
            + blobInfo.getBucket()
            + "/o";
        this.blobInfo = blobInfo;
        this.failIfAlreadyExists = failIfAlreadyExists;
        this.source = source;
        this.size = size;
        this.config = config;
        this.stats = stats;
        this.counters = counters;
        this.threadPool = threadPool;
        this.description = blobInfo.getBucket() + "/" + blobInfo.getName();
    }

    void run() throws IOException {
        final long startNanos = System.nanoTime();
        int parts = 0;
        try (Watchdog watchdog = Watchdog.start(config.getWriteStallTimeout(), threadPool)) {
            this.watchdog = watchdog;
            sessionUri = openSession();
            final long tailLength = config.isVerifyChecksum() ? size - ((size - 1) / PART_ALIGNMENT) * PART_ALIGNMENT : 0;
            streamedEnd = size - tailLength;
            long partStart = 0;
            while (partStart < streamedEnd) {
                final long partEnd = Math.min(streamedEnd, partStart + config.getPartSize());
                uploadPart(parts++, partStart, partEnd);
                partStart = partEnd;
            }
            if (tailLength > 0) {
                uploadPart(parts++, streamedEnd, size);
            }
            assert completed;
        } catch (Unretryable e) {
            throw e.rethrow();
        }
        counters.addFile(parts, size);
        if (logger.isDebugEnabled()) {
            logger.debug(
                "uploaded [{}] bytes [{}] in [{}] parts, total_ms [{}]",
                description,
                size,
                parts,
                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos)
            );
        }
    }

    // ---- session ----

    private String openSession() throws IOException {
        while (true) {
            Failure failure;
            try {
                final StorageObject object = new StorageObject().setBucket(blobInfo.getBucket()).setName(blobInfo.getName());
                if (blobInfo.getMd5() != null) {
                    object.setMd5Hash(blobInfo.getMd5());
                }
                if (blobInfo.getStorageClass() != null) {
                    object.setStorageClass(blobInfo.getStorageClass().toString());
                }
                final GenericUrl url = new GenericUrl(uploadUrl);
                url.set("uploadType", "resumable");
                url.set("name", blobInfo.getName());
                if (failIfAlreadyExists) {
                    url.set("ifGenerationMatch", 0L);
                }
                final HttpRequest request = requestFactory.buildPostRequest(url, new JsonHttpContent(jsonFactory, object));
                request.getHeaders().set("X-Upload-Content-Type", OCTET_STREAM);
                request.getHeaders().set("X-Upload-Content-Length", size);
                final Reply reply = execute(request, null);
                if (reply.status == 200 && reply.location != null) {
                    return reply.location;
                }
                failure = classify(reply, "opening the upload session");
            } catch (IOException e) {
                failure = new Failure("opening the upload session failed", 0, e);
            }
            retryOrThrow(failure, 0);
        }
    }

    // ---- parts ----

    private void uploadPart(int partIndex, long partStart, long partEnd) throws IOException {
        positionSource(partStart);
        final boolean tail = config.isVerifyChecksum() && partEnd == size;
        if (tail) {
            // the bytes to send are kept in the copy buffer, a retry does not need the source
            loadTail(partStart, partEnd);
        } else {
            // mark at the part start so that a retry can rewind to here; for snapshot sources this is cheap, reset reopens the file at the
            // mark
            try {
                source.mark(Math.toIntExact(partEnd - partStart));
            } catch (RuntimeException e) {
                throw new Unretryable(e);
            }
            markOffset = partStart;
        }

        long from = partStart;
        boolean needsQuery = false;
        for (int attempt = 1;; attempt++) {
            Failure failure;
            try {
                final Reply reply;
                if (needsQuery) {
                    reply = queryCommittedOffset();
                } else {
                    if (tail == false) {
                        positionSource(from);
                    }
                    reply = sendPart(partIndex, partStart, from, partEnd, attempt);
                }
                final long committed = reply.committed;
                if (reply.status == 200 || reply.status == 201) {
                    if (partEnd != size) {
                        throw new StorageException(
                            reply.status,
                            "resumable upload of [" + description + "] completed before all [" + size + "] bytes were sent"
                        );
                    }
                    completed = true;
                    return;
                } else if (reply.status == 308) {
                    if (committed < partStart || committed > partEnd) {
                        throw new StorageException(
                            500,
                            "resumable upload of ["
                                + description
                                + "] reports committed offset ["
                                + committed
                                + "] outside of the current part ["
                                + partStart
                                + ", "
                                + partEnd
                                + ")"
                        );
                    }
                    if (committed == partEnd && partEnd < size) {
                        noteProgress();
                        return;
                    }
                    if (committed < partEnd) {
                        if (needsQuery == false && committed <= from) {
                            failure = new Failure("part not committed, offset still [" + committed + "]", 308, null);
                        } else {
                            if (committed > from) {
                                noteProgress();
                                counters.addResumedBytes(committed - from);
                            }
                            if (needsQuery == false) {
                                // a failure was already counted as a retry, a partial commit is only counted here
                                counters.addPartialCommit();
                                counters.addRetry();
                            }
                            logger.info(
                                "resuming upload of [{}] part [{}] [{}, {}) from committed offset [{}] ({})",
                                description,
                                partIndex,
                                partStart,
                                partEnd,
                                committed,
                                needsQuery ? "after a failure" : "partial commit"
                            );
                            from = committed;
                            needsQuery = false;
                            continue;
                        }
                    } else {
                        // everything is committed but the session did not finalize, ask again
                        failure = new Failure("all bytes committed but upload not finalized", 308, null);
                    }
                } else {
                    failure = classify(reply, "uploading part [" + partIndex + "]");
                }
            } catch (Unretryable e) {
                throw e;
            } catch (IOException e) {
                failure = new Failure("uploading part [" + partIndex + "] failed", 0, e);
            }
            retryOrThrow(failure, partIndex);
            needsQuery = true;
        }
    }

    private Reply sendPart(int partIndex, long partStart, long from, long partEnd, int attempt) throws IOException {
        final boolean tail = config.isVerifyChecksum() && partEnd == size;
        final PartContent content = new PartContent(partEnd - from, tail ? Math.toIntExact(from - streamedEnd) : -1);
        final HttpRequest request = requestFactory.buildPutRequest(new GenericUrl(sessionUri), content);
        request.getHeaders().setContentRange("bytes " + from + "-" + (partEnd - 1) + "/" + size);
        if (tail) {
            assert tailLoaded && hashedUpTo == size : "the checksum covers [0, " + hashedUpTo + ") of [" + size + "]";
            request.getHeaders()
                .set(
                    "x-goog-hash",
                    "crc32c=" + Base64.getEncoder().encodeToString(ByteBuffer.allocate(4).putInt((int) crc32c.getValue()).array())
                );
        }
        final Reply reply = execute(request, content);
        if (logger.isDebugEnabled()) {
            logger.debug(
                "part [{}] of [{}] range [{}, {}) bytes [{}] source_read_ms [{}] socket_write_ms [{}] response_wait_ms [{}]"
                    + " discard_ms [{}] attempt [{}] status [{}] request_ms [{}] connection_rate_mib_s [{}]"
                    + " socket_write_rate_mib_s [{}] blocked_write_rate_mib_s [{}] {}",
                partIndex,
                description,
                from,
                partEnd,
                partEnd - from,
                TimeUnit.NANOSECONDS.toMillis(content.readNanos),
                TimeUnit.NANOSECONDS.toMillis(content.writeNanos),
                TimeUnit.NANOSECONDS.toMillis(reply.responseWaitNanos),
                TimeUnit.NANOSECONDS.toMillis(discardNanos),
                attempt,
                reply.status,
                TimeUnit.NANOSECONDS.toMillis(reply.requestNanos),
                // the rate of the connection: the bytes of the part over the time from the request to its response, in which the source
                // was read as well as the socket written, so it is a floor; the rate of the socket writes alone is above what the
                // connection takes when the writes do not wait for the source
                rateMibPerSecond(partEnd - from, reply.requestNanos),
                rateMibPerSecond(partEnd - from, content.writeNanos),
                rateMibPerSecond(content.blockedWriteBytes, content.blockedWriteNanos),
                reply.connection == null ? "connection unknown" : reply.connection
            );
        }
        discardNanos = 0;
        return reply;
    }

    private static String rateMibPerSecond(long bytes, long nanos) {
        return nanos <= 0 ? "n/a" : String.format(Locale.ROOT, "%.1f", bytes / (1024.0 * 1024.0) / (nanos / 1e9));
    }

    /**
     * Asks the session how much it has committed: an empty PUT with Content-Range "bytes *&#47;total".
     */
    private Reply queryCommittedOffset() throws IOException {
        final HttpRequest request = requestFactory.buildPutRequest(new GenericUrl(sessionUri), new EmptyContent());
        request.getHeaders().setContentRange("bytes */" + size);
        return execute(request, null);
    }

    private Failure classify(Reply reply, String doing) throws IOException {
        final int status = reply.status;
        final String message = doing + " for [" + description + "] got status [" + status + "]" + reply.describeBody();
        if (status == 408 || status == 429 || status >= 500) {
            return new Failure(message, status, null);
        } else if (status == 410) {
            counters.addSessionGone();
            throw new StorageException(
                410,
                message
                    + ": the resumable upload session is gone and a streamed upload cannot be restarted from the beginning"
                    + " (the source is only rewound to the start of the current part), failing the write"
            );
        } else if (status == 412 && failIfAlreadyExists) {
            throw new Unretryable(new FileAlreadyExistsException(blobInfo.getName(), null, message));
        } else {
            throw new StorageException(status, message);
        }
    }

    // ---- retry ----

    private void noteProgress() {
        failureStreak = 0;
        failures.clear();
    }

    /**
     * Counts the failure and returns after the backoff if another attempt is allowed, otherwise throws.
     */
    private void retryOrThrow(Failure failure, int partIndex) throws IOException {
        final long now = System.nanoTime();
        if (failureStreak == 0) {
            failureStreakStartNanos = now;
        }
        failureStreak++;
        if (failure.cause != null) {
            failures.add(failure.cause);
        }
        final boolean interrupted = Thread.currentThread().isInterrupted();
        final boolean exhausted = failureStreak >= config.getMaxAttempts()
            || now - failureStreakStartNanos >= config.getMaxRetryTime().nanos();
        if (exhausted || interrupted) {
            final String message = failure.message + (interrupted
                ? ", interrupted"
                : ", giving up after ["
                    + failureStreak
                    + "] consecutive failures over ["
                    + TimeValue.timeValueNanos(now - failureStreakStartNanos)
                    + "]");
            final Exception thrown;
            if (failure.status == 0 || failure.status == 308) {
                thrown = new IOException(message, failure.cause);
            } else {
                thrown = new StorageException(failure.status, message);
            }
            for (Exception earlier : failures) {
                if (earlier != failure.cause) {
                    thrown.addSuppressed(earlier);
                }
            }
            if (thrown instanceof IOException io) {
                throw io;
            }
            throw (StorageException) thrown;
        }
        final long backoffNanos = backoffNanos(failureStreak);
        logger.info(
            "retrying upload of [{}] part [{}] after attempt [{}] failed: {}, backing off [{}ms]",
            description,
            partIndex,
            failureStreak,
            failure.message + (failure.cause != null ? " (" + failure.cause + ")" : ""),
            TimeUnit.NANOSECONDS.toMillis(backoffNanos)
        );
        counters.addRetry();
        sleep(backoffNanos);
    }

    private long backoffNanos(int failureNumber) {
        final long initial = config.getInitialBackoff().nanos();
        final long max = config.getMaxBackoff().nanos();
        final long exponential = initial << Math.min(failureNumber - 1, 20);
        final long capped = Math.min(max, exponential < 0 ? Long.MAX_VALUE : exponential);
        // jitter in [50%, 100%] of the delay
        return (long) (capped * (0.5 + Randomness.get().nextDouble() * 0.5));
    }

    private static void sleep(long nanos) throws IOException {
        try {
            TimeUnit.NANOSECONDS.sleep(nanos);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new java.io.InterruptedIOException("interrupted while backing off");
        }
    }

    // ---- source ----

    /**
     * Moves the source to the given absolute offset. Going forwards just means reading on, going backwards rewinds to the last mark and
     * reads and discards up to the target, since the source cannot skip.
     */
    private void positionSource(long target) {
        if (sourcePosition == target) {
            return;
        }
        final long startNanos = System.nanoTime();
        try {
            if (sourcePosition > target) {
                assert markOffset <= target : "mark at [" + markOffset + "] is after the target [" + target + "]";
                source.reset();
                sourcePosition = markOffset;
            }
            while (sourcePosition < target) {
                final int read = readSource(0, (int) Math.min(buffer.length, target - sourcePosition));
                if (read < 0) {
                    throw new EOFException("source ended at [" + sourcePosition + "] before the expected offset [" + target + "]");
                }
            }
        } catch (IOException | RuntimeException e) {
            throw new Unretryable(e);
        } finally {
            discardNanos += System.nanoTime() - startNanos;
        }
    }

    /**
     * Reads from the source into the copy buffer and keeps the position and the CRC32C up to date. The bytes that a rewound source gives
     * again have been hashed already and are skipped.
     */
    private int readSource(int offset, int length) throws IOException {
        final int read = source.read(buffer, offset, length);
        if (read > 0) {
            final long end = sourcePosition + read;
            if (config.isVerifyChecksum() && end > hashedUpTo) {
                assert sourcePosition <= hashedUpTo : "gap in the checksum: read at [" + sourcePosition + "], hashed [" + hashedUpTo + "]";
                final int alreadyHashed = (int) Math.max(0, hashedUpTo - sourcePosition);
                crc32c.update(buffer, offset + alreadyHashed, read - alreadyHashed);
                hashedUpTo = end;
            }
            sourcePosition = end;
        }
        return read;
    }

    /**
     * Reads the last bytes of the blob into the copy buffer, which completes the CRC32C of the blob.
     */
    private void loadTail(long tailStart, long end) {
        assert end - tailStart <= buffer.length;
        try {
            int loaded = 0;
            while (loaded < end - tailStart) {
                final int read = readSource(loaded, (int) (end - tailStart) - loaded);
                if (read < 0) {
                    throw new EOFException(
                        "source ended at [" + sourcePosition + "], [" + (end - sourcePosition) + "] bytes short of the blob"
                    );
                }
                loaded += read;
            }
        } catch (IOException | RuntimeException e) {
            throw new Unretryable(e);
        }
        tailLoaded = true;
    }

    // ---- HTTP ----

    /**
     * Executes the request and releases the response; the connection of a failed request is disconnected so no half-sent request lingers.
     */
    private Reply execute(HttpRequest request, @Nullable PartContent content) throws IOException {
        request.setResponseInterceptor(response -> GcsRepositoryStatsCollector.recordResponse(stats, response.getStatusCode()));
        HttpResponse response = null;
        try (GcsConnectionTracker.Slot connection = GcsConnectionTracker.begin()) {
            if (content != null) {
                content.connection = connection;
            }
            try {
                final boolean observed = content != null && logger.isDebugEnabled();
                final long requestStartNanos = System.nanoTime();
                if (observed) {
                    GcsSocketObserver.beginRequest();
                }
                response = request.execute();
                final long responseNanos = System.nanoTime();
                final int status = response.getStatusCode();
                final String range = response.getHeaders().getRange();
                final String location = response.getHeaders().getLocation();
                final String body = status >= 300 && status != 308 ? response.parseAsString() : null;
                return new Reply(
                    status,
                    status == 308 ? parseCommitted(range) : -1,
                    location,
                    body,
                    content != null && content.writeEndNanos != 0 ? responseNanos - content.writeEndNanos : 0,
                    responseNanos - requestStartNanos,
                    observed ? GcsSocketObserver.endRequest() : null
                );
            } catch (IOException | RuntimeException e) {
                final HttpURLConnection httpConnection = connection.getConnection();
                if (httpConnection != null) {
                    httpConnection.disconnect();
                }
                if (watchdog != null && watchdog.takeFired()) {
                    counters.addWriteStall();
                    if (e instanceof IOException io) {
                        io.addSuppressed(
                            new IOException("socket write made no progress for [" + config.getWriteStallTimeout() + "], disconnected")
                        );
                    }
                }
                throw e;
            }
        } finally {
            if (watchdog != null) {
                watchdog.detach();
            }
            if (response != null) {
                try {
                    response.ignore();
                } catch (IOException e) {
                    logger.debug("failed to release response", e);
                }
            }
        }
    }

    /**
     * @return the committed offset from the "Range: bytes=0-N" header of a 308, 0 if there is no such header (nothing committed yet)
     */
    private static long parseCommitted(@Nullable String range) throws IOException {
        if (range == null) {
            return 0;
        }
        final Matcher matcher = COMMITTED_RANGE.matcher(range);
        if (matcher.matches() == false) {
            throw new IOException("unexpected Range header [" + range + "]");
        }
        return Long.parseLong(matcher.group(1)) + 1;
    }

    private record Reply(
        int status,
        long committed,
        @Nullable String location,
        @Nullable String body,
        long responseWaitNanos,
        long requestNanos,
        @Nullable GcsSocketObserver.Connection connection
    ) {
        String describeBody() {
            return body == null || body.isEmpty() ? "" : " body [" + (body.length() > 500 ? body.substring(0, 500) + "..." : body) + "]";
        }
    }

    private record Failure(String message, int status, @Nullable Exception cause) {}

    /**
     * Streams a range of the source into the request body, in {@link #COPY_BUFFER_SIZE} pieces. The length is declared so that
     * HttpURLConnection does not buffer the body. For the last request the bytes come from the copy buffer, where they were loaded to
     * compute the checksum.
     */
    private final class PartContent implements HttpContent {
        private final long length;
        private final int bufferOffset;
        private GcsConnectionTracker.Slot connection;
        private long readNanos;
        private long writeNanos;
        private long blockedWriteNanos; // of the writes that waited, which took at least a millisecond
        private long blockedWriteBytes;
        private long writeEndNanos;

        /**
         * @param bufferOffset where in the copy buffer the body starts, or -1 to read the body from the source
         */
        PartContent(long length, int bufferOffset) {
            this.length = length;
            this.bufferOffset = bufferOffset;
        }

        @Override
        public long getLength() {
            return length;
        }

        @Override
        public String getType() {
            return OCTET_STREAM;
        }

        @Override
        public boolean retrySupported() {
            // a retry has to go through the query, rewind and resend logic of the upload
            return false;
        }

        @Override
        public void writeTo(OutputStream out) throws IOException {
            if (watchdog != null) {
                watchdog.attach(connection.getConnection());
            }
            if (bufferOffset >= 0) {
                write(out, bufferOffset, (int) length);
                writeEndNanos = System.nanoTime();
                return;
            }
            long remaining = length;
            while (remaining > 0) {
                final int wanted = (int) Math.min(buffer.length, remaining);
                final long readStart = System.nanoTime();
                final int read;
                try {
                    read = readSource(0, wanted);
                    if (read < 0) {
                        throw new EOFException("source ended at [" + sourcePosition + "], [" + remaining + "] bytes short of the part");
                    }
                } catch (IOException | RuntimeException e) {
                    throw new Unretryable(e);
                }
                readNanos += System.nanoTime() - readStart;
                write(out, 0, read);
                remaining -= read;
            }
            writeEndNanos = System.nanoTime();
        }

        private void write(OutputStream out, int offset, int length) throws IOException {
            final long writeStart = System.nanoTime();
            if (watchdog != null) {
                watchdog.writeStarting();
            }
            out.write(buffer, offset, length);
            if (watchdog != null) {
                watchdog.writeFinished();
            }
            final long took = System.nanoTime() - writeStart;
            writeNanos += took;
            if (took >= 1_000_000) {
                // a write that waited for the socket to take the bytes: over these the bytes per second are what the connection takes
                blockedWriteNanos += took;
                blockedWriteBytes += length;
            }
        }
    }

    /**
     * Marks an exception that must not be retried, so that it passes through the HTTP client and the retry logic and is rethrown as it
     * was: those thrown by the source, which are never failures of the destination, and the outcomes that are final, such as the blob
     * existing already (a FileAlreadyExistsException is an IOException, which is otherwise retried).
     */
    static final class Unretryable extends RuntimeException {
        Unretryable(Exception cause) {
            super(cause);
        }

        IOException rethrow() throws IOException {
            if (getCause() instanceof IOException io) {
                throw io;
            }
            throw (RuntimeException) getCause();
        }
    }

    /**
     * HttpURLConnection has no write timeout, so a socket write to a peer that stopped reading blocks until TCP gives up, minutes later.
     * The watchdog disconnects a request whose socket write has been blocked for longer than the stall timeout, which fails the write
     * with an IOException and sends the part into the retry. Time spent reading the source, which may legitimately block for a long time
     * on the snapshot rate limiter, does not count.
     *
     * It is a periodic task of the thread pool, the upload does not start a thread of its own. The scheduler thread only hands each poll
     * to the generic executor, so run() and the disconnect() in it are on a generic thread. A disconnect that blocks therefore ties up one
     * generic thread (and this watchdog's later polls, which are rescheduled after run() returns), not the timers of the scheduler.
     *
     * Switched off by default, see {@link GcsStreamingUploadSettings#WRITE_STALL_TIMEOUT_SETTING}: over TLS disconnect() blocks on the lock
     * of the stalled writer. TODO SO_LINGER on the sockets of the streaming transport, the disconnect forked off the polling thread, and a
     * stall test over HTTPS.
     */
    private static final class Watchdog implements AutoCloseable, Runnable {
        private static final long IDLE = Long.MIN_VALUE;

        private final long stallNanos;
        private Scheduler.Cancellable task;
        private volatile long writeStartNanos = IDLE;
        private volatile HttpURLConnection connection;
        private volatile boolean fired;

        private Watchdog(long stallNanos) {
            this.stallNanos = stallNanos;
        }

        @Nullable
        static Watchdog start(TimeValue stallTimeout, ThreadPool threadPool) {
            if (stallTimeout.nanos() <= 0) {
                return null;
            }
            final Watchdog watchdog = new Watchdog(stallTimeout.nanos());
            final long pollNanos = Math.max(
                TimeUnit.MILLISECONDS.toNanos(1),
                Math.min(TimeUnit.SECONDS.toNanos(1), watchdog.stallNanos / 4)
            );
            watchdog.task = threadPool.scheduleWithFixedDelay(watchdog, TimeValue.timeValueNanos(pollNanos), threadPool.generic());
            return watchdog;
        }

        void attach(@Nullable HttpURLConnection connection) {
            this.fired = false;
            this.writeStartNanos = IDLE;
            this.connection = connection;
        }

        void detach() {
            this.connection = null;
            this.writeStartNanos = IDLE;
        }

        void writeStarting() {
            writeStartNanos = System.nanoTime();
        }

        void writeFinished() {
            writeStartNanos = IDLE;
        }

        boolean takeFired() {
            final boolean wasFired = fired;
            fired = false;
            return wasFired;
        }

        @Override
        public void run() {
            final long started = writeStartNanos;
            final HttpURLConnection current = connection;
            if (started != IDLE && current != null && System.nanoTime() - started >= stallNanos && writeStartNanos == started) {
                fired = true;
                connection = null;
                current.disconnect();
            }
        }

        @Override
        public void close() {
            task.cancel();
        }
    }
}
