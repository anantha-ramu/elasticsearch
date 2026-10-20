/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.repositories.gcs;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.hamcrest.Matchers.equalTo;

public class GcsReadTimingTests extends ESTestCase {

    private static final String LOGGER_NAME = "org.elasticsearch.repositories.gcs.GetTiming";

    private static <T> T withDebug(java.util.concurrent.Callable<T> action) throws Exception {
        final var logger = LogManager.getLogger(LOGGER_NAME);
        final var original = logger.getLevel();
        Loggers.setLevel(logger, Level.DEBUG);
        try {
            return action.call();
        } finally {
            Loggers.setLevel(logger, original);
        }
    }

    public void testIsOnWhenTheLoggerOfTheSourceReadsIsAtDebug() throws Exception {
        final var logger = LogManager.getLogger(LOGGER_NAME);
        final var original = logger.getLevel();
        Loggers.setLevel(logger, Level.INFO);
        try {
            assertFalse(GcsReadTiming.isEnabled());
        } finally {
            Loggers.setLevel(logger, original);
        }
        assertTrue(withDebug(GcsReadTiming::isEnabled));
    }

    public void testLogsOneLineWhenTheBodyIsRead() throws Exception {
        final int length = between(1, 10_000);
        final var connection = new GcsSocketObserver.Connection(7, "/10.0.0.1:443", true, 131072);
        withDebug(() -> {
            try (var mockLog = MockLog.capture(LOGGER_NAME)) {
                mockLog.addExpectation(
                    new MockLog.SeenEventExpectation(
                        "the GET",
                        LOGGER_NAME,
                        Level.DEBUG,
                        "GET repository [repo] purpose [SnapshotData] blob [bucket/blob-1] range_start [100] range_length ["
                            + length
                            + "] bytes ["
                            + length
                            + "] elapsed_ms [*] first_byte_ms [*] headers_ms [*] in_read_ms [*] rate_mib_s [*] "
                            + "in_read_rate_mib_s [*] "
                            + "blocked_ms [*] blocked_bytes [*] blocked_rate_mib_s [*] "
                            + "remote [/10.0.0.1:443] connection [7] reused so_rcvbuf [131072] thread [*]"
                    )
                );
                final InputStream in = GcsReadTiming.timed(
                    new ByteArrayInputStream(randomByteArrayOfLength(length)),
                    "repo",
                    "SnapshotData",
                    "bucket",
                    "blob-1",
                    100,
                    99 + length,
                    System.nanoTime(),
                    1_000_000,
                    connection
                );
                assertThat(in.readAllBytes().length, equalTo(length));
                in.close();
                mockLog.assertAllExpectationsMatched();
            }
            return null;
        });
    }

    public void testLogsWhenClosedBeforeTheEndAndOnlyOnce() throws Exception {
        withDebug(() -> {
            try (var mockLog = MockLog.capture(LOGGER_NAME)) {
                mockLog.addExpectation(
                    new MockLog.SeenEventExpectation(
                        "the GET",
                        LOGGER_NAME,
                        Level.DEBUG,
                        "GET repository [repo] purpose [data] blob [bucket/blob-2] range_start [0] range_length [to_end] bytes [10] "
                            + "* connection unknown thread [*]"
                    )
                );
                final InputStream in = GcsReadTiming.timed(
                    new ByteArrayInputStream(randomByteArrayOfLength(100)),
                    "repo",
                    "data",
                    "bucket",
                    "blob-2",
                    0,
                    Long.MAX_VALUE - 1,
                    System.nanoTime(),
                    0,
                    null
                );
                assertThat(in.readNBytes(10).length, equalTo(10));
                in.close();
                in.close();
                // reading to the end after a close does not log a second time
                try {
                    in.read();
                } catch (IOException e) {
                    // closed streams may throw
                }
                mockLog.assertAllExpectationsMatched();
            }
            return null;
        });
    }

    /**
     * A body that delivers at about 10 MiB/s, read by a consumer that stops for much longer than a read between its reads, as the upload
     * does while it writes: the rate over the wall time falls with the pauses, and the rate of the reads that waited stays that of the
     * body, which is the one to look at for the speed of the connection.
     */
    public void testBlockedRateIsTheRateOfTheBodyWhateverTheConsumerDoes() throws Exception {
        final var line = new java.util.concurrent.atomic.AtomicReference<String>();
        final var expectation = new MockLog.LoggingExpectation() {
            @Override
            public void match(org.apache.logging.log4j.core.LogEvent event) {
                if (event.getMessage().getFormattedMessage().startsWith("GET repository")) {
                    line.set(event.getMessage().getFormattedMessage());
                }
            }

            @Override
            public void assertMatched() {
                assertNotNull(line.get());
            }
        };
        final int chunk = 256 * 1024;
        final int chunks = 5;
        // 25 ms for a chunk is 10 MiB/s
        final InputStream slow = new java.io.InputStream() {
            private int served;

            @Override
            public int read() {
                throw new AssertionError("not used");
            }

            @Override
            public int read(byte[] b, int off, int len) {
                if (served >= chunk * chunks) {
                    return -1;
                }
                safeSleep(25);
                final int n = Math.min(Math.min(len, chunk), chunk * chunks - served);
                served += n;
                return n;
            }
        };
        withDebug(() -> {
            try (var mockLog = MockLog.capture(LOGGER_NAME)) {
                mockLog.addExpectation(expectation);
                final InputStream in = GcsReadTiming.timed(
                    slow,
                    "repo",
                    "SnapshotData",
                    "bucket",
                    "blob",
                    0,
                    chunk * chunks - 1L,
                    System.nanoTime(),
                    0,
                    null
                );
                final var buffer = new byte[chunk];
                while (in.read(buffer) >= 0) {
                    safeSleep(100); // the consumer is busy with something else, as the upload is with its writes
                }
                in.close();
                mockLog.assertAllExpectationsMatched();
            }
            return null;
        });
        final var m = java.util.regex.Pattern.compile("rate_mib_s \\[([\\d.]+)\\].* blocked_rate_mib_s \\[([\\d.]+)\\]")
            .matcher(line.get());
        assertTrue(line.get(), m.find());
        final double wallRate = Double.parseDouble(m.group(1));
        final double blockedRate = Double.parseDouble(m.group(2));
        assertThat("blocked rate is near the 10 MiB/s of the body: " + line.get(), blockedRate, org.hamcrest.Matchers.closeTo(10.0, 4.0));
        assertThat(
            "the rate over the wall time includes the pauses: " + line.get(),
            wallRate,
            org.hamcrest.Matchers.lessThan(blockedRate * 0.6)
        );
    }
}
