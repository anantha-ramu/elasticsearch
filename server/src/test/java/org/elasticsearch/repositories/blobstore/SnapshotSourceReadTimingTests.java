/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.blobstore;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;

public class SnapshotSourceReadTimingTests extends ESTestCase {

    public void testReadsAreLoggedPerWindow() {
        final String loggerName = SnapshotSourceReadTiming.class.getCanonicalName();
        final var logger = LogManager.getLogger(SnapshotSourceReadTiming.class);
        final Level originalLevel = logger.getLevel();
        Loggers.setLevel(logger, Level.DEBUG);
        try (var mockLog = MockLog.capture(SnapshotSourceReadTiming.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "full window",
                    loggerName,
                    Level.DEBUG,
                    "part [part.0] source read of [16777216] bytes in [16] reads took * ms (longest read * ms)"
                )
            );
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "rest of the part",
                    loggerName,
                    Level.DEBUG,
                    "part [part.0] source read of [4194304] bytes in [4] reads took * ms (longest read * ms)"
                )
            );
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "part total",
                    loggerName,
                    Level.DEBUG,
                    "part [part.0] of [20971520] bytes: upload took [1234] ms, [20] source reads"
                )
            );

            final var timing = new SnapshotSourceReadTiming("part.0");
            for (int i = 0; i < 20; i++) {
                timing.record(1024 * 1024, between(1, 1_000_000));
            }
            timing.finish(20L * 1024 * 1024, 1234);
            mockLog.assertAllExpectationsMatched();
        } finally {
            Loggers.setLevel(logger, originalLevel);
        }
    }
}
