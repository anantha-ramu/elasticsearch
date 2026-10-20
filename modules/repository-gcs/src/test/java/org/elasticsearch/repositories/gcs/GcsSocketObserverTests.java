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
import com.sun.net.httpserver.HttpsServer;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.http.PemHttpsConfigurator;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.cert.X509Certificate;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * The observer is made of a socket that stands in for the JDK's TLS socket, so these tests go through a real TLS server, as the GCS client
 * does, and check that requests work through it and what it reports.
 */
@SuppressForbidden(reason = "a local HTTPS server, and connections to it, to test the sockets that the code under test wraps")
public class GcsSocketObserverTests extends ESTestCase {

    private HttpsServer server;
    private SSLSocketFactory trustingFactory;
    private byte[] body;

    @Before
    public void startServer() throws Exception {
        body = randomByteArrayOfLength(between(1, 10_000));
        final Path cert = copyResource("cert1.crt");
        final Path key = copyResource("cert1-pkcs8.key");
        server = HttpsServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.setHttpsConfigurator(new PemHttpsConfigurator(cert, key, new char[0]));
        server.createContext("/", this::handle);
        server.start();
        trustingFactory = trustAll();
    }

    @After
    public void stopServer() {
        server.stop(0);
    }

    private Path copyResource(String name) throws IOException {
        final Path path = createTempDir().resolve(name);
        try (InputStream in = GcsSocketObserverTests.class.getResourceAsStream(name)) {
            assertThat("test resource " + name, in, notNullValue());
            Files.copy(in, path);
        }
        return path;
    }

    private void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            exchange.getRequestBody().readAllBytes();
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(body);
            }
        }
    }

    /** The test server has a certificate of its own, which the observed factory is to make connections to as the factory it wraps does. */
    private static SSLSocketFactory trustAll() throws GeneralSecurityException {
        final var context = SSLContext.getInstance("TLS");
        context.init(null, new TrustManager[] { new X509TrustManager() {
            @Override
            public void checkClientTrusted(X509Certificate[] chain, String authType) {}

            @Override
            public void checkServerTrusted(X509Certificate[] chain, String authType) {}

            @Override
            public X509Certificate[] getAcceptedIssuers() {
                return new X509Certificate[0];
            }
        } }, null);
        return context.getSocketFactory();
    }

    private HttpsURLConnection connection(SSLSocketFactory factory) throws IOException {
        final var url = new URL(
            "https://" + InetAddress.getLoopbackAddress().getHostAddress() + ":" + server.getAddress().getPort() + "/blob"
        );
        final var connection = (HttpsURLConnection) url.openConnection();
        connection.setSSLSocketFactory(factory);
        connection.setHostnameVerifier((host, session) -> true);
        return connection;
    }

    private void get(SSLSocketFactory factory) throws IOException {
        final var connection = connection(factory);
        try (InputStream in = connection.getInputStream()) {
            assertArrayEquals(body, in.readAllBytes());
        }
    }

    private void put(SSLSocketFactory factory) throws IOException {
        final var connection = connection(factory);
        connection.setRequestMethod("PUT");
        connection.setDoOutput(true);
        connection.setFixedLengthStreamingMode(body.length);
        try (OutputStream out = connection.getOutputStream()) {
            out.write(body);
        }
        try (InputStream in = connection.getInputStream()) {
            assertArrayEquals(body, in.readAllBytes());
        }
    }

    public void testReportsTheConnectionAndThatAnotherRequestReusedIt() throws IOException {
        final var factory = GcsSocketObserver.observing(trustingFactory, () -> true);

        GcsSocketObserver.beginRequest();
        get(factory);
        final var first = GcsSocketObserver.endRequest();
        assertThat(first, notNullValue());
        assertFalse("a connection that is made for a request is new", first.reused());
        assertThat(first.remote(), containsString(":" + server.getAddress().getPort()));
        assertThat(first.receiveBuffer(), greaterThan(0));
        assertThat(first.toString(), containsString("new"));

        GcsSocketObserver.beginRequest();
        get(factory);
        final var second = GcsSocketObserver.endRequest();
        assertThat(second, notNullValue());
        assertTrue("the connection was kept alive and used again", second.reused());
        assertThat(second.id(), equalTo(first.id()));
        assertThat(second.remote(), equalTo(first.remote()));
        assertThat(second.toString(), containsString("reused"));
    }

    public void testRequestWithABodyIsObservedToo() throws IOException {
        final var factory = GcsSocketObserver.observing(trustingFactory, () -> true);
        GcsSocketObserver.beginRequest();
        put(factory);
        final var connection = GcsSocketObserver.endRequest();
        assertThat(connection, notNullValue());
        assertFalse(connection.reused());
        assertThat(connection.remote(), containsString(":" + server.getAddress().getPort()));
    }

    public void testNothingIsObservedWhileObservingIsOff() throws IOException {
        final var factory = GcsSocketObserver.observing(trustingFactory, () -> false);
        GcsSocketObserver.beginRequest();
        get(factory);
        assertThat("the sockets are the JDK's own", GcsSocketObserver.endRequest(), nullValue());
    }

    public void testConnectionsOfDifferentThreadsAreNotMixedUp() throws Exception {
        final var factory = GcsSocketObserver.observing(trustingFactory, () -> true);
        final int threads = between(2, 4);
        final var ids = new java.util.concurrent.ConcurrentHashMap<Long, Integer>();
        runInParallel(threads, i -> {
            try {
                for (int r = 0; r < 3; r++) {
                    GcsSocketObserver.beginRequest();
                    get(factory);
                    final var connection = GcsSocketObserver.endRequest();
                    assertThat(connection, notNullValue());
                    ids.merge(connection.id(), 1, Integer::sum);
                }
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        });
        // every request was seen on a connection, and no more were made than requests
        assertThat(ids.values().stream().mapToInt(Integer::intValue).sum(), equalTo(threads * 3));
        assertThat(ids.size(), not(equalTo(0)));
    }

    public void testAnObservationIsTakenOnce() throws IOException {
        final var factory = GcsSocketObserver.observing(trustingFactory, () -> true);
        GcsSocketObserver.beginRequest();
        get(factory);
        assertThat(GcsSocketObserver.endRequest(), notNullValue());
        // ended once, so nothing is left to be taken for the next request
        assertThat(GcsSocketObserver.endRequest(), nullValue());
    }

    /**
     * What QA does and the tests above do not: the certificate is checked against a CA, the default hostname verifier runs on the name in
     * the URL, and the name is sent in the handshake, all of which are done with the socket that the factory returns.
     */
    public void testTrustAndHostnameVerificationRunThroughTheObservedSocket() throws Exception {
        final var created = new java.util.concurrent.CopyOnWriteArrayList<String>();
        final var recording = new RecordingFactory(trustCa(), created);
        final var factory = GcsSocketObserver.observing(recording, () -> true);
        final var url = new URL("https://localhost:" + server.getAddress().getPort() + "/blob");

        for (int i = 0; i < 2; i++) {
            GcsSocketObserver.beginRequest();
            final var connection = (HttpsURLConnection) url.openConnection();
            connection.setSSLSocketFactory(factory); // the default hostname verifier stays, and checks the name in the URL
            try (InputStream in = connection.getInputStream()) {
                assertArrayEquals(body, in.readAllBytes());
            }
            final var observed = GcsSocketObserver.endRequest();
            assertThat("created with " + created, observed, notNullValue());
            assertThat(observed.reused(), equalTo(i == 1));
        }

        // this JDK asks the factory for an unconnected socket and connects it itself, so the wrapper has to do what the socket does for
        // connect, SNI and the checks of the certificate and name, which is what the requests above went through
        assertThat(created.get(0), equalTo("unconnected"));

        // a certificate that the CA does not vouch for is still refused through the wrapper
        final var untrusting = GcsSocketObserver.observing(defaultTrust(), () -> true);
        final var refused = (HttpsURLConnection) url.openConnection();
        refused.setSSLSocketFactory(untrusting);
        expectThrows(javax.net.ssl.SSLHandshakeException.class, refused::getInputStream);
    }

    private static SSLSocketFactory defaultTrust() throws Exception {
        final var context = SSLContext.getInstance("TLS");
        context.init(null, null, null); // the JDK's own trust, which does not have the CA of the test server
        return context.getSocketFactory();
    }

    private static SSLSocketFactory trustCa() throws Exception {
        final var keyStore = java.security.KeyStore.getInstance("PKCS12");
        keyStore.load(null, null);
        try (InputStream in = GcsSocketObserverTests.class.getResourceAsStream("ca1.crt")) {
            keyStore.setCertificateEntry("ca", java.security.cert.CertificateFactory.getInstance("X.509").generateCertificate(in));
        }
        final var trustManagers = javax.net.ssl.TrustManagerFactory.getInstance(javax.net.ssl.TrustManagerFactory.getDefaultAlgorithm());
        trustManagers.init(keyStore);
        final var context = SSLContext.getInstance("TLS");
        context.init(null, trustManagers.getTrustManagers(), null);
        return context.getSocketFactory();
    }

    /** Hands out the sockets of the factory it wraps, and notes which way it was asked for them. */
    private static final class RecordingFactory extends SSLSocketFactory {
        private final SSLSocketFactory delegate;
        private final java.util.List<String> created;

        RecordingFactory(SSLSocketFactory delegate, java.util.List<String> created) {
            this.delegate = delegate;
            this.created = created;
        }

        @Override
        public String[] getDefaultCipherSuites() {
            return delegate.getDefaultCipherSuites();
        }

        @Override
        public String[] getSupportedCipherSuites() {
            return delegate.getSupportedCipherSuites();
        }

        @Override
        public java.net.Socket createSocket(java.net.Socket s, String host, int port, boolean autoClose) throws IOException {
            created.add("layered over a connected socket, host " + host);
            return delegate.createSocket(s, host, port, autoClose);
        }

        @Override
        public java.net.Socket createSocket() throws IOException {
            created.add("unconnected");
            return delegate.createSocket();
        }

        @Override
        public java.net.Socket createSocket(String host, int port) throws IOException {
            created.add("host and port");
            return delegate.createSocket(host, port);
        }

        @Override
        public java.net.Socket createSocket(String host, int port, InetAddress localHost, int localPort) throws IOException {
            created.add("host, port and local address");
            return delegate.createSocket(host, port, localHost, localPort);
        }

        @Override
        public java.net.Socket createSocket(InetAddress host, int port) throws IOException {
            created.add("address and port");
            return delegate.createSocket(host, port);
        }

        @Override
        public java.net.Socket createSocket(InetAddress address, int port, InetAddress localAddress, int localPort) throws IOException {
            created.add("address, port and local address");
            return delegate.createSocket(address, port, localAddress, localPort);
        }
    }
}
