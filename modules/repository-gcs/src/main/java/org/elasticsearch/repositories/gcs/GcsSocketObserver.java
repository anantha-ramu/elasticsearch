/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.repositories.gcs;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.SuppressForbidden;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.SocketOption;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;

import javax.net.ssl.HandshakeCompletedListener;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

/**
 * Finds out which connection a request of the GCS client used, for the DEBUG logs of reads and uploads: the remote address, whether the
 * connection was new or reused from the keep-alive cache, and the receive buffer of the socket. {@link java.net.HttpURLConnection} does not
 * say any of these, and the socket is the JDK's, so the only public hook is the {@link SSLSocketFactory} of the transport, which sees each
 * socket that is made, and wraps it in one that notes the thread that writes to it. A request writes its head on the thread that executes
 * it, so after the request that thread's last written socket is the one the request used, and a socket that an observed request used before
 * is a reuse (requests that nobody observes, such as listings, are not counted, so a first observed request may be reported as new on a
 * connection that was in use already).
 * <p>
 * It is installed once per transport, as the one factory of the transport, so that the keep-alive cache, which is keyed by it, is shared as
 * it is without it. It wraps a socket only while {@link #isObserving()} says so, which is when one of the loggers that make use of it is at
 * DEBUG; a socket made at any other time is the JDK's own. The connections of GCS are TLS ones. Requests over plain HTTP, as in the tests
 * against a local fixture, are not observed.
 */
final class GcsSocketObserver {

    private static final ThreadLocal<ObservedSocket> LAST_WRITTEN = new ThreadLocal<>();
    private static final AtomicLong SOCKET_IDS = new AtomicLong();

    private GcsSocketObserver() {}

    static boolean isObserving() {
        return GcsReadTiming.isEnabled() || GcsStreamingUpload.logger.isDebugEnabled();
    }

    /**
     * @return a factory that makes the sockets of {@code delegate}, and observes them while {@link #isObserving()}
     */
    static SSLSocketFactory observing(SSLSocketFactory delegate) {
        return new ObservingFactory(delegate, GcsSocketObserver::isObserving);
    }

    // visible for testing
    static SSLSocketFactory observing(SSLSocketFactory delegate, BooleanSupplier observing) {
        return new ObservingFactory(delegate, observing);
    }

    /**
     * Forgets the socket that this thread wrote to last, so that the observation of the request that is about to start does not take one
     * that was used for another.
     */
    static void beginRequest() {
        LAST_WRITTEN.remove();
    }

    /**
     * What the request that was executed on this thread since {@link #beginRequest()} is known to have used.
     *
     * @return the connection, or {@code null} if it is not known: the request was not over a TLS connection, or the socket was made when
     * nothing was observed
     */
    @Nullable
    static Connection endRequest() {
        final ObservedSocket socket = LAST_WRITTEN.get();
        LAST_WRITTEN.remove();
        if (socket == null) {
            return null;
        }
        final boolean reused = socket.requests.getAndIncrement() > 0;
        return new Connection(socket.id, remoteOf(socket.delegate), reused, receiveBufferOf(socket.delegate));
    }

    private static String remoteOf(Socket socket) {
        final SocketAddress address = socket.getRemoteSocketAddress();
        return address == null ? "unknown" : address.toString();
    }

    private static int receiveBufferOf(Socket socket) {
        try {
            return socket.getReceiveBufferSize();
        } catch (SocketException e) {
            return -1;
        }
    }

    /**
     * The connection of a request.
     *
     * @param id            of the socket, so that requests over the same connection can be told
     * @param remote        the address and port at the other end
     * @param reused        whether the connection had served a request before
     * @param receiveBuffer SO_RCVBUF at the end of the request, in bytes, -1 if it could not be read
     */
    record Connection(long id, String remote, boolean reused, int receiveBuffer) {
        @Override
        public String toString() {
            return "remote [" + remote + "] connection [" + id + "] " + (reused ? "reused" : "new") + " so_rcvbuf [" + receiveBuffer + "]";
        }
    }

    private static final class ObservingFactory extends SSLSocketFactory {
        private final SSLSocketFactory delegate;
        private final BooleanSupplier observing;

        ObservingFactory(SSLSocketFactory delegate, BooleanSupplier observing) {
            this.delegate = delegate;
            this.observing = observing;
        }

        private Socket wrap(Socket socket) {
            if (socket instanceof SSLSocket ssl && observing.getAsBoolean()) {
                return new ObservedSocket(ssl);
            }
            return socket;
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
        public Socket createSocket(Socket s, String host, int port, boolean autoClose) throws IOException {
            return wrap(delegate.createSocket(s, host, port, autoClose));
        }

        @Override
        public Socket createSocket(Socket s, InputStream consumed, boolean autoClose) throws IOException {
            return wrap(delegate.createSocket(s, consumed, autoClose));
        }

        @Override
        public Socket createSocket() throws IOException {
            return wrap(delegate.createSocket());
        }

        @Override
        public Socket createSocket(String host, int port) throws IOException {
            return wrap(delegate.createSocket(host, port));
        }

        @Override
        public Socket createSocket(String host, int port, InetAddress localHost, int localPort) throws IOException {
            return wrap(delegate.createSocket(host, port, localHost, localPort));
        }

        @Override
        public Socket createSocket(InetAddress host, int port) throws IOException {
            return wrap(delegate.createSocket(host, port));
        }

        @Override
        public Socket createSocket(InetAddress address, int port, InetAddress localAddress, int localPort) throws IOException {
            return wrap(delegate.createSocket(address, port, localAddress, localPort));
        }
    }

    /**
     * A TLS socket that does what the socket it wraps does, and notes that the thread that writes to it is using it.
     */
    @SuppressForbidden(reason = "delegates to the socket it wraps, which the JDK has connected, or the caller connects")
    private static final class ObservedSocket extends SSLSocket {
        private final SSLSocket delegate;
        private final long id = SOCKET_IDS.incrementAndGet();
        private final AtomicInteger requests = new AtomicInteger();
        private volatile OutputStream out;

        ObservedSocket(SSLSocket delegate) {
            this.delegate = delegate;
        }

        @Override
        public OutputStream getOutputStream() throws IOException {
            OutputStream stream = out;
            if (stream == null) {
                final OutputStream wrapped = delegate.getOutputStream();
                stream = new OutputStream() {
                    @Override
                    public void write(int b) throws IOException {
                        LAST_WRITTEN.set(ObservedSocket.this);
                        wrapped.write(b);
                    }

                    @Override
                    public void write(byte[] b, int off, int len) throws IOException {
                        LAST_WRITTEN.set(ObservedSocket.this);
                        wrapped.write(b, off, len);
                    }

                    @Override
                    public void flush() throws IOException {
                        wrapped.flush();
                    }

                    @Override
                    public void close() throws IOException {
                        wrapped.close();
                    }
                };
                out = stream;
            }
            return stream;
        }

        // ---- everything below is the wrapped socket ----

        @Override
        public InputStream getInputStream() throws IOException {
            return delegate.getInputStream();
        }

        @Override
        public void connect(SocketAddress endpoint) throws IOException {
            delegate.connect(endpoint);
        }

        @Override
        public void connect(SocketAddress endpoint, int timeout) throws IOException {
            delegate.connect(endpoint, timeout);
        }

        @Override
        public void bind(SocketAddress bindpoint) throws IOException {
            delegate.bind(bindpoint);
        }

        @Override
        public InetAddress getInetAddress() {
            return delegate.getInetAddress();
        }

        @Override
        public InetAddress getLocalAddress() {
            return delegate.getLocalAddress();
        }

        @Override
        public int getPort() {
            return delegate.getPort();
        }

        @Override
        public int getLocalPort() {
            return delegate.getLocalPort();
        }

        @Override
        public SocketAddress getRemoteSocketAddress() {
            return delegate.getRemoteSocketAddress();
        }

        @Override
        public SocketAddress getLocalSocketAddress() {
            return delegate.getLocalSocketAddress();
        }

        @Override
        public SocketChannel getChannel() {
            return delegate.getChannel();
        }

        @Override
        public void setTcpNoDelay(boolean on) throws SocketException {
            delegate.setTcpNoDelay(on);
        }

        @Override
        public boolean getTcpNoDelay() throws SocketException {
            return delegate.getTcpNoDelay();
        }

        @Override
        public void setSoLinger(boolean on, int linger) throws SocketException {
            delegate.setSoLinger(on, linger);
        }

        @Override
        public int getSoLinger() throws SocketException {
            return delegate.getSoLinger();
        }

        @Override
        public void sendUrgentData(int data) throws IOException {
            delegate.sendUrgentData(data);
        }

        @Override
        public void setOOBInline(boolean on) throws SocketException {
            delegate.setOOBInline(on);
        }

        @Override
        public boolean getOOBInline() throws SocketException {
            return delegate.getOOBInline();
        }

        @Override
        public void setSoTimeout(int timeout) throws SocketException {
            delegate.setSoTimeout(timeout);
        }

        @Override
        public int getSoTimeout() throws SocketException {
            return delegate.getSoTimeout();
        }

        @Override
        public void setSendBufferSize(int size) throws SocketException {
            delegate.setSendBufferSize(size);
        }

        @Override
        public int getSendBufferSize() throws SocketException {
            return delegate.getSendBufferSize();
        }

        @Override
        public void setReceiveBufferSize(int size) throws SocketException {
            delegate.setReceiveBufferSize(size);
        }

        @Override
        public int getReceiveBufferSize() throws SocketException {
            return delegate.getReceiveBufferSize();
        }

        @Override
        public void setKeepAlive(boolean on) throws SocketException {
            delegate.setKeepAlive(on);
        }

        @Override
        public boolean getKeepAlive() throws SocketException {
            return delegate.getKeepAlive();
        }

        @Override
        public void setTrafficClass(int tc) throws SocketException {
            delegate.setTrafficClass(tc);
        }

        @Override
        public int getTrafficClass() throws SocketException {
            return delegate.getTrafficClass();
        }

        @Override
        public void setReuseAddress(boolean on) throws SocketException {
            delegate.setReuseAddress(on);
        }

        @Override
        public boolean getReuseAddress() throws SocketException {
            return delegate.getReuseAddress();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }

        @Override
        public void shutdownInput() throws IOException {
            delegate.shutdownInput();
        }

        @Override
        public void shutdownOutput() throws IOException {
            delegate.shutdownOutput();
        }

        @Override
        public String toString() {
            return delegate.toString();
        }

        @Override
        public boolean isConnected() {
            return delegate.isConnected();
        }

        @Override
        public boolean isBound() {
            return delegate.isBound();
        }

        @Override
        public boolean isClosed() {
            return delegate.isClosed();
        }

        @Override
        public boolean isInputShutdown() {
            return delegate.isInputShutdown();
        }

        @Override
        public boolean isOutputShutdown() {
            return delegate.isOutputShutdown();
        }

        @Override
        public void setPerformancePreferences(int connectionTime, int latency, int bandwidth) {
            delegate.setPerformancePreferences(connectionTime, latency, bandwidth);
        }

        @Override
        public <T> Socket setOption(SocketOption<T> name, T value) throws IOException {
            delegate.setOption(name, value);
            return this;
        }

        @Override
        public <T> T getOption(SocketOption<T> name) throws IOException {
            return delegate.getOption(name);
        }

        @Override
        public Set<SocketOption<?>> supportedOptions() {
            return delegate.supportedOptions();
        }

        @Override
        public String[] getSupportedCipherSuites() {
            return delegate.getSupportedCipherSuites();
        }

        @Override
        public String[] getEnabledCipherSuites() {
            return delegate.getEnabledCipherSuites();
        }

        @Override
        public void setEnabledCipherSuites(String[] suites) {
            delegate.setEnabledCipherSuites(suites);
        }

        @Override
        public String[] getSupportedProtocols() {
            return delegate.getSupportedProtocols();
        }

        @Override
        public String[] getEnabledProtocols() {
            return delegate.getEnabledProtocols();
        }

        @Override
        public void setEnabledProtocols(String[] protocols) {
            delegate.setEnabledProtocols(protocols);
        }

        @Override
        public SSLSession getSession() {
            return delegate.getSession();
        }

        @Override
        public SSLSession getHandshakeSession() {
            return delegate.getHandshakeSession();
        }

        @Override
        public void addHandshakeCompletedListener(HandshakeCompletedListener listener) {
            delegate.addHandshakeCompletedListener(listener);
        }

        @Override
        public void removeHandshakeCompletedListener(HandshakeCompletedListener listener) {
            delegate.removeHandshakeCompletedListener(listener);
        }

        @Override
        public void startHandshake() throws IOException {
            delegate.startHandshake();
        }

        @Override
        public void setUseClientMode(boolean mode) {
            delegate.setUseClientMode(mode);
        }

        @Override
        public boolean getUseClientMode() {
            return delegate.getUseClientMode();
        }

        @Override
        public void setNeedClientAuth(boolean need) {
            delegate.setNeedClientAuth(need);
        }

        @Override
        public boolean getNeedClientAuth() {
            return delegate.getNeedClientAuth();
        }

        @Override
        public void setWantClientAuth(boolean want) {
            delegate.setWantClientAuth(want);
        }

        @Override
        public boolean getWantClientAuth() {
            return delegate.getWantClientAuth();
        }

        @Override
        public void setEnableSessionCreation(boolean flag) {
            delegate.setEnableSessionCreation(flag);
        }

        @Override
        public boolean getEnableSessionCreation() {
            return delegate.getEnableSessionCreation();
        }

        @Override
        public SSLParameters getSSLParameters() {
            return delegate.getSSLParameters();
        }

        @Override
        public void setSSLParameters(SSLParameters params) {
            delegate.setSSLParameters(params);
        }

        @Override
        public String getApplicationProtocol() {
            return delegate.getApplicationProtocol();
        }

        @Override
        public String getHandshakeApplicationProtocol() {
            return delegate.getHandshakeApplicationProtocol();
        }

        @Override
        public void setHandshakeApplicationProtocolSelector(BiFunction<SSLSocket, List<String>, String> selector) {
            delegate.setHandshakeApplicationProtocolSelector(selector);
        }

        @Override
        public BiFunction<SSLSocket, List<String>, String> getHandshakeApplicationProtocolSelector() {
            return delegate.getHandshakeApplicationProtocolSelector();
        }
    }
}
