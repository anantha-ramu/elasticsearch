/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.repositories.gcs;

import org.elasticsearch.test.ESTestCase;

import java.io.OutputStream;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

public class GcsBlockedRateTests extends ESTestCase {

    public void testCallsThatDidNotWaitAreLeftOut() {
        final var rate = new GcsBlockedRate();
        assertThat(rate.describe(), equalTo("n/a"));
        rate.record(GcsBlockedRate.BLOCKED_NANOS - 1, 1000);
        rate.record(GcsBlockedRate.BLOCKED_NANOS, 0); // nothing came back
        assertThat(rate.getBlockedBytes(), equalTo(0L));
        assertThat(rate.describe(), equalTo("n/a"));
        rate.record(GcsBlockedRate.BLOCKED_NANOS, 1024 * 1024);
        assertThat(rate.getBlockedBytes(), equalTo(1024L * 1024));
        assertThat(rate.getBlockedNanos(), equalTo(GcsBlockedRate.BLOCKED_NANOS));
    }

    /**
     * The writes of the upload are timed in the same way, here to a connection that takes 16 KiB at a known rate and a writer that stops
     * between writes. The estimate follows the rate of the connection, at a slow rate and at a fast one, while the rate over the wall
     * time, which is what the pauses of the writer leave of it, falls.
     */
    public void testBlockedRateOfWritesTracksTheRateOfTheConnection() {
        for (double mibPerSecond : new double[] { 10, 85, 200 }) {
            final int record = 16 * 1024;
            final long recordNanos = (long) (record / (mibPerSecond * 1024 * 1024) * 1e9);
            final var blocked = new GcsBlockedRate();
            final OutputStream connection = new OutputStream() {
                private long nextFreeNanos = System.nanoTime();

                @Override
                public void write(int b) {
                    throw new AssertionError("not used");
                }

                @Override
                public void write(byte[] b, int off, int len) {
                    nextFreeNanos = Math.max(nextFreeNanos, System.nanoTime()) + recordNanos;
                    GcsReadTimingTests.waitUntil(nextFreeNanos);
                }
            };
            final long start = System.nanoTime();
            final int writes = 128;
            for (int i = 1; i <= writes; i++) {
                final long writeStart = System.nanoTime();
                try {
                    connection.write(new byte[record], 0, record);
                } catch (java.io.IOException e) {
                    throw new AssertionError(e);
                }
                blocked.record(System.nanoTime() - writeStart, record);
                if (i % 8 == 0) {
                    GcsReadTimingTests.waitUntil(System.nanoTime() + 16 * recordNanos);
                }
            }
            final double wallRate = (double) writes * record / (1024 * 1024) / ((System.nanoTime() - start) / 1e9);
            final double blockedRate = Double.parseDouble(blocked.describe());
            assertThat("blocked rate, [" + mibPerSecond + "] MiB/s", blockedRate, closeTo(mibPerSecond, 0.35 * mibPerSecond));
            assertThat("wall rate falls with the pauses", wallRate, org.hamcrest.Matchers.lessThan(blockedRate * 0.6));
        }
    }
}
