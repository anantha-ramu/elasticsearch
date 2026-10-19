/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.repositories.gcs;

import com.google.api.client.http.javanet.ConnectionFactory;
import com.google.api.client.http.javanet.DefaultConnectionFactory;

import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.Proxy;
import java.net.URL;

/**
 * Gives the streaming upload a handle on the {@link HttpURLConnection} behind a request, so that a stalled socket write can be broken by
 * disconnecting from another thread. The google-http-client does not expose the connection, but it opens it through the
 * {@link ConnectionFactory} of the transport, on the thread that executes the request.
 *
 * Tracking is only active between {@link #begin()} and {@link Slot#close()} on the calling thread, so other requests through the same
 * transport retain nothing.
 */
final class GcsConnectionTracker {

    private static final ThreadLocal<Slot> ACTIVE = new ThreadLocal<>();

    private GcsConnectionTracker() {}

    /**
     * Start tracking the connection opened by the next request executed on this thread.
     */
    static Slot begin() {
        assert ACTIVE.get() == null : "connection tracking is already active on this thread";
        final Slot slot = new Slot();
        ACTIVE.set(slot);
        return slot;
    }

    static final class Slot implements AutoCloseable {
        private volatile HttpURLConnection connection;

        /**
         * @return the connection of the request, or {@code null} if the transport does not use a {@link TrackingConnectionFactory}
         */
        @Nullable
        HttpURLConnection getConnection() {
            return connection;
        }

        @Override
        public void close() {
            ACTIVE.remove();
        }
    }

    /**
     * The connection factory of the GCS client transport; same behaviour as {@link DefaultConnectionFactory}.
     */
    static final class TrackingConnectionFactory implements ConnectionFactory {
        private final DefaultConnectionFactory delegate;

        TrackingConnectionFactory(@Nullable Proxy proxy) {
            this.delegate = new DefaultConnectionFactory(proxy);
        }

        @Override
        public HttpURLConnection openConnection(URL url) throws IOException {
            final HttpURLConnection connection = delegate.openConnection(url);
            final Slot slot = ACTIVE.get();
            // The first connection opened after begin() is the one of the request. Others opened on the thread while that request is in
            // flight, such as the GET the source of the upload makes when it is rewound, are not.
            if (slot != null && slot.connection == null) {
                slot.connection = connection;
            }
            return connection;
        }
    }
}
