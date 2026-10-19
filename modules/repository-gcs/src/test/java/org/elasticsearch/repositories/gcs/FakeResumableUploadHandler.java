/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.repositories.gcs;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.rest.RestStatus;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.CRC32C;

/**
 * A GCS server that only knows resumable uploads, with scripted failures. The repository's own fixture, GoogleCloudStorageHttpHandler,
 * has no way to commit part of a request and then fail it, which is what the retry and resume logic needs to be exercised against. It
 * also records protocol violations (parts that are not a multiple of 256kb, gaps, chunked bodies) for the tests to assert on.
 */
@SuppressForbidden(reason = "use a http server")
final class FakeResumableUploadHandler implements HttpHandler {

    private static final Pattern CONTENT_RANGE = Pattern.compile("bytes (?:(\\d+)-(\\d+)|\\*)/(\\d+|\\*)");

    enum Type {
        OPEN,
        PUT,
        QUERY
    }

    /**
     * One request as seen by the server.
     */
    record Call(int index, Type type, String name, long from, long end, long total, long committedBefore) {
        long length() {
            return end - from;
        }
    }

    /**
     * What the server does with a request: commit the first {@code commit} bytes of the body (-1: all, 0: none), then do {@code action}.
     */
    record Fault(long commit, Action action, int status, CountDownLatch release) {
        enum Action {
            /** answer normally */
            NONE,
            /** answer with the given status */
            STATUS,
            /** close the connection without answering */
            DROP,
            /** do not answer until released (client read timeout) */
            HANG,
            /** do not read the body, and do not answer until released */
            STOP_READING
        }

        static final Fault NONE = new Fault(-1, Action.NONE, 0, null);

        static Fault status(int status) {
            return new Fault(0, Action.STATUS, status, null);
        }

        static Fault commitThenStatus(long commit, int status) {
            return new Fault(commit, Action.STATUS, status, null);
        }

        static Fault commitThenDrop(long commit) {
            return new Fault(commit, Action.DROP, 0, null);
        }

        static Fault commitThenHang(long commit, CountDownLatch release) {
            return new Fault(commit, Action.HANG, 0, release);
        }

        static Fault stopReading(CountDownLatch release) {
            return new Fault(0, Action.STOP_READING, 0, release);
        }

        /** commit part of the body and answer normally, which gives a 308 whose Range is less than what was sent */
        static Fault partialCommit(long commit) {
            return new Fault(commit, Action.NONE, 0, null);
        }
    }

    private static final class Session {
        final String id = UUIDs.randomBase64UUID();
        final String name;
        final Long ifGenerationMatch;
        final ByteArrayOutputStream data = new ByteArrayOutputStream();
        final AtomicLong length = new AtomicLong();
        boolean finalized;

        Session(String name, Long ifGenerationMatch) {
            this.name = name;
            this.ifGenerationMatch = ifGenerationMatch;
        }
    }

    private final String bucket;
    private final Map<String, Session> sessions = new ConcurrentHashMap<>();
    private final Map<String, byte[]> blobs = new ConcurrentHashMap<>();
    private final Map<String, Long> blobLengths = new ConcurrentHashMap<>();
    private final List<Call> calls = new CopyOnWriteArrayList<>();
    private final List<String> openBodies = new CopyOnWriteArrayList<>();
    private final List<String> openQueries = new CopyOnWriteArrayList<>();
    private final List<String> protocolErrors = Collections.synchronizedList(new ArrayList<>());
    private final List<Map<String, String>> putHeaders = new CopyOnWriteArrayList<>();
    private final AtomicInteger callIndex = new AtomicInteger();

    /** decides what to do with each call */
    volatile Function<Call, Fault> faults = call -> Fault.NONE;
    /** keep the bytes of the uploads, switch off for uploads too big to hold */
    volatile boolean keepData = true;
    /** told how many bytes of the current PUT body have been read so far */
    volatile LongConsumer bodyProgress = bytes -> {};

    FakeResumableUploadHandler(String bucket) {
        this.bucket = bucket;
    }

    void putBlob(String name, byte[] contents) {
        blobs.put(name, contents);
        blobLengths.put(name, (long) contents.length);
    }

    boolean hasBlob(String name) {
        return blobLengths.containsKey(name);
    }

    byte[] getBlob(String name) {
        return blobs.get(name);
    }

    long getBlobLength(String name) {
        return blobLengths.get(name);
    }

    List<Call> getCalls() {
        return calls;
    }

    List<Call> getCalls(Type type) {
        return calls.stream().filter(c -> c.type == type).toList();
    }

    List<String> getOpenBodies() {
        return openBodies;
    }

    List<String> getOpenQueries() {
        return openQueries;
    }

    List<String> getProtocolErrors() {
        return protocolErrors;
    }

    List<Map<String, String>> getPutHeaders() {
        return putHeaders;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        try {
            final String path = exchange.getRequestURI().getRawPath();
            final String query = exchange.getRequestURI().getRawQuery();
            if (path.equals("/upload/storage/v1/b/" + bucket + "/o") == false || query == null) {
                protocolErrors.add("unexpected request " + exchange.getRequestMethod() + " " + exchange.getRequestURI());
                exchange.sendResponseHeaders(RestStatus.NOT_FOUND.getStatus(), -1);
                return;
            }
            if (exchange.getRequestMethod().equals("POST")) {
                open(exchange, query);
            } else if (exchange.getRequestMethod().equals("PUT")) {
                put(exchange, query);
            } else {
                protocolErrors.add("unexpected method " + exchange.getRequestMethod());
                exchange.sendResponseHeaders(RestStatus.METHOD_NOT_ALLOWED.getStatus(), -1);
            }
        } catch (RuntimeException | Error e) {
            protocolErrors.add("handler failed: " + e);
            throw e;
        } finally {
            exchange.close();
        }
    }

    private static String param(String query, String name) {
        for (String pair : query.split("&")) {
            if (pair.startsWith(name + "=")) {
                return java.net.URLDecoder.decode(pair.substring(name.length() + 1), StandardCharsets.UTF_8);
            }
        }
        return null;
    }

    private void open(HttpExchange exchange, String query) throws IOException {
        final String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
        openBodies.add(body);
        openQueries.add(query);
        if (exchange.getRequestHeaders().getFirst("X-Upload-Content-Length") == null) {
            protocolErrors.add("no X-Upload-Content-Length on open");
        }
        final String name = param(query, "name");
        final String ifGenerationMatch = param(query, "ifGenerationMatch");
        final Call call = new Call(callIndex.getAndIncrement(), Type.OPEN, name, 0, 0, 0, 0);
        calls.add(call);
        final Fault fault = faults.apply(call);
        if (fault.action == Fault.Action.STATUS) {
            exchange.sendResponseHeaders(fault.status, -1);
            return;
        }
        if ("0".equals(ifGenerationMatch) && hasBlob(name)) {
            exchange.sendResponseHeaders(RestStatus.PRECONDITION_FAILED.getStatus(), -1);
            return;
        }
        final Session session = new Session(name, ifGenerationMatch == null ? null : Long.parseLong(ifGenerationMatch));
        sessions.put(session.id, session);
        final String host = "http://" + exchange.getLocalAddress().getHostString() + ":" + exchange.getLocalAddress().getPort();
        exchange.getResponseHeaders()
            .add("Location", host + "/upload/storage/v1/b/" + bucket + "/o?uploadType=resumable&upload_id=" + session.id);
        exchange.sendResponseHeaders(RestStatus.OK.getStatus(), -1);
    }

    private void put(HttpExchange exchange, String query) throws IOException {
        final Session session = sessions.get(param(query, "upload_id"));
        if (session == null) {
            protocolErrors.add("unknown session");
            exchange.sendResponseHeaders(RestStatus.NOT_FOUND.getStatus(), -1);
            return;
        }
        final String contentRange = exchange.getRequestHeaders().getFirst("Content-Range");
        final Matcher matcher = contentRange == null ? null : CONTENT_RANGE.matcher(contentRange);
        if (matcher == null || matcher.matches() == false) {
            protocolErrors.add("bad Content-Range [" + contentRange + "]");
            exchange.sendResponseHeaders(RestStatus.BAD_REQUEST.getStatus(), -1);
            return;
        }
        final boolean isQuery = matcher.group(1) == null;
        final long from = isQuery ? 0 : Long.parseLong(matcher.group(1));
        final long end = isQuery ? 0 : Long.parseLong(matcher.group(2)) + 1;
        final long total = matcher.group(3).equals("*") ? -1 : Long.parseLong(matcher.group(3));
        final String contentLength = exchange.getRequestHeaders().getFirst("Content-Length");
        if (isQuery == false) {
            final Map<String, String> headers = new java.util.HashMap<>();
            exchange.getRequestHeaders().forEach((k, v) -> headers.put(k.toLowerCase(java.util.Locale.ROOT), String.join(",", v)));
            headers.put("x-test-blob", session.name);
            putHeaders.add(headers);
            if (exchange.getRequestHeaders().getFirst("Transfer-Encoding") != null) {
                protocolErrors.add("part sent with Transfer-Encoding [" + exchange.getRequestHeaders().getFirst("Transfer-Encoding") + "]");
            }
            if (contentLength == null || Long.parseLong(contentLength) != end - from) {
                protocolErrors.add("Content-Length [" + contentLength + "] does not match range [" + contentRange + "]");
            }
            if (end != total && (end - from) % (256 * 1024) != 0) {
                protocolErrors.add("non-final part [" + contentRange + "] is not a multiple of 256kb");
            }
        }
        final Call call = new Call(
            callIndex.getAndIncrement(),
            isQuery ? Type.QUERY : Type.PUT,
            session.name,
            from,
            end,
            total,
            session.length.get()
        );
        calls.add(call);
        final Fault fault = faults.apply(call);

        if (fault.action == Fault.Action.STOP_READING) {
            awaitRelease(fault.release);
            return;
        }
        if (isQuery == false) {
            final long toCommit = fault.commit < 0 ? end - from : Math.min(fault.commit, end - from);
            readBody(exchange.getRequestBody(), session, from, toCommit, end - from);
        }
        switch (fault.action) {
            case NONE -> {
            }
            case STATUS -> {
                exchange.sendResponseHeaders(fault.status, -1);
                return;
            }
            case DROP -> {
                // the server did its work, only the answer is lost
                finalizeIfComplete(session, total, exchange.getRequestHeaders().getFirst("x-goog-hash"));
                return;
            }
            case HANG -> {
                awaitRelease(fault.release);
                return;
            }
            case STOP_READING -> throw new AssertionError();
        }
        respond(exchange, session, total);
    }

    private static void awaitRelease(CountDownLatch release) {
        try {
            release.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void readBody(InputStream body, Session session, long from, long toCommit, long length) throws IOException {
        final long committed = session.length.get();
        if (from > committed) {
            protocolErrors.add("gap: part starts at [" + from + "] but only [" + committed + "] bytes are committed");
        }
        final byte[] chunk = new byte[64 * 1024];
        long read = 0;
        while (read < length) {
            final int n = body.read(chunk, 0, (int) Math.min(chunk.length, length - read));
            if (n < 0) {
                protocolErrors.add("body ended after [" + read + "] of [" + length + "] bytes");
                return;
            }
            // commit what is new and within what the fault lets through, skip the overlap with what is committed already
            final long chunkStart = from + read;
            final long committedNow = session.length.get();
            final int firstNew = (int) Math.max(0, committedNow - chunkStart);
            final int limit = (int) Math.min(n, toCommit - read);
            if (limit > firstNew && chunkStart + firstNew == committedNow) {
                if (keepData) {
                    session.data.write(chunk, firstNew, limit - firstNew);
                }
                session.length.addAndGet(limit - firstNew);
            }
            read += n;
            bodyProgress.accept(read);
        }
    }

    /**
     * @return 0, or the status to answer with if the object cannot be finalized: because it exists and the session requires that it does
     *         not, or because the checksum of the request is not that of the object
     */
    private int finalizeIfComplete(Session session, long total, String hashHeader) {
        final long committed = session.length.get();
        if (session.finalized == false && total == committed) {
            if (Long.valueOf(0).equals(session.ifGenerationMatch) && hasBlob(session.name)) {
                return RestStatus.PRECONDITION_FAILED.getStatus();
            }
            if (hashHeader != null && keepData) {
                final CRC32C crc = new CRC32C();
                crc.update(session.data.toByteArray());
                final String expected = "crc32c="
                    + Base64.getEncoder().encodeToString(ByteBuffer.allocate(4).putInt((int) crc.getValue()).array());
                if (expected.equals(hashHeader) == false) {
                    protocolErrors.add("checksum mismatch: got [" + hashHeader + "], object has [" + expected + "]");
                    return RestStatus.BAD_REQUEST.getStatus();
                }
            }
            session.finalized = true;
            blobLengths.put(session.name, committed);
            if (keepData) {
                blobs.put(session.name, session.data.toByteArray());
            }
        }
        return 0;
    }

    private void respond(HttpExchange exchange, Session session, long total) throws IOException {
        final long committed = session.length.get();
        final int refused = finalizeIfComplete(session, total, exchange.getRequestHeaders().getFirst("x-goog-hash"));
        if (refused != 0) {
            exchange.sendResponseHeaders(refused, -1);
            return;
        }
        if (session.finalized) {
            // the SDK insists on the metadata of the object in the answer
            final byte[] json = ("{\"name\":\"" + session.name + "\",\"size\":\"" + committed + "\"}").getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(RestStatus.OK.getStatus(), json.length);
            exchange.getResponseBody().write(json);
        } else {
            if (committed > 0) {
                exchange.getResponseHeaders().add("Range", "bytes=0-" + (committed - 1));
            }
            exchange.sendResponseHeaders(308, -1);
        }
    }

    @Override
    public String toString() {
        return "FakeResumableUploadHandler" + Arrays.toString(calls.toArray());
    }
}
