/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.repositories.gcs;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;

import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

@SuppressForbidden(reason = "opens connections, which are not connected until used")
public class GcsConnectionTrackerTests extends ESTestCase {

    private static HttpURLConnection open(GcsConnectionTracker.TrackingConnectionFactory factory) throws IOException {
        return factory.openConnection(URI.create("http://127.0.0.1:1/").toURL());
    }

    public void testRecordsTheConnectionOfTheRequest() throws IOException {
        final var factory = new GcsConnectionTracker.TrackingConnectionFactory(null);
        try (var slot = GcsConnectionTracker.begin()) {
            assertThat(slot.getConnection(), nullValue());
            final var connection = open(factory);
            assertThat(slot.getConnection(), sameInstance(connection));
        }
    }

    public void testConnectionsOpenedWhileTheRequestIsInFlightAreNotTheRequest() throws IOException {
        // the source of an upload that is rewound makes a GET of its own while the body of the request is written
        final var factory = new GcsConnectionTracker.TrackingConnectionFactory(null);
        try (var slot = GcsConnectionTracker.begin()) {
            final var request = open(factory);
            open(factory);
            open(factory);
            assertThat(slot.getConnection(), sameInstance(request));
        }
    }

    public void testNothingIsRetainedOutsideOfATrackedRequest() throws IOException {
        final var factory = new GcsConnectionTracker.TrackingConnectionFactory(null);
        open(factory);
        final HttpURLConnection tracked;
        try (var slot = GcsConnectionTracker.begin()) {
            tracked = open(factory);
            assertThat(slot.getConnection(), sameInstance(tracked));
        }
        open(factory);
        try (var slot = GcsConnectionTracker.begin()) {
            assertThat("a new request starts without a connection", slot.getConnection(), nullValue());
        }
    }
}
