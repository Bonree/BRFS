package com.bonree.brfs.common.net.http.client;

public class ClientConfig {
    private static final int DEFAULT_MAX_CONNECTION = 4;
    private int maxConnection;
    private static final int DEFAULT_MAX_CONNECTION_PER_ROUTE = 2;
    private int maxConnectionPerRoute;

    private static final int DEFAULT_BUFFER_SIZE = 8 * 1024;
    private int bufferSize;

    private long idleTimeout;

    private static final int DEFAULT_SEND_BUFFER_SIZE = 64 * 1024;
    private int socketSendBufferSize;
    private static final int DEFAULT_RECV_BUFFER_SIZE = 512 * 1024;
    private int socketRecvBufferSize;
    private static final int DEFAULT_SOCKET_TIMEOUT = 10 * 1000;
    private int socketTimeout;

    private static final int DEFAULT_CONNECT_TIMEOUT = 30 * 1000;
    private int connectTimeout;

    private static final int DEFAULT_IO_THREAD_NUM = 4;
    private int ioThreadNum;

    private static final long DEFAULT_RESPONSE_TIMEOUT = Long.MAX_VALUE;
    private long responseTimeout;

    private boolean keepAlive;

    public static ClientConfig DEFAULT = new ClientConfig();

    private ClientConfig() {
        this.maxConnection = DEFAULT_MAX_CONNECTION;
        this.maxConnectionPerRoute = DEFAULT_MAX_CONNECTION_PER_ROUTE;
        this.bufferSize = DEFAULT_BUFFER_SIZE;
        this.idleTimeout = 0;
        this.socketSendBufferSize = DEFAULT_SEND_BUFFER_SIZE;
        this.socketRecvBufferSize = DEFAULT_RECV_BUFFER_SIZE;
        this.socketTimeout = DEFAULT_SOCKET_TIMEOUT;
        this.connectTimeout = DEFAULT_CONNECT_TIMEOUT;
        this.ioThreadNum = DEFAULT_IO_THREAD_NUM;
        this.responseTimeout = DEFAULT_RESPONSE_TIMEOUT;
        this.keepAlive = false;
    }

    public int getMaxConnection() {
        return maxConnection;
    }

    public int getMaxConnectionPerRoute() {
        return maxConnectionPerRoute;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public long getIdleTimeout() {
        return idleTimeout;
    }

    public int getSocketSendBufferSize() {
        return socketSendBufferSize;
    }

    public int getSocketRecvBufferSize() {
        return socketRecvBufferSize;
    }

    public int getSocketTimeout() {
        return socketTimeout;
    }

    public int getConnectTimeout() {
        return connectTimeout;
    }

    public int getIOThreadNum() {
        return ioThreadNum;
    }

    public long getResponseTimeout() {
        return responseTimeout;
    }

    public boolean isKeepAlive() {
        return keepAlive;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private ClientConfig config = new ClientConfig();

        public Builder setMaxConnection(int maxConnection) {
            config.maxConnection = maxConnection > 0 ? maxConnection : DEFAULT_MAX_CONNECTION;
            return this;
        }

        public Builder setMaxConnectionPerRoute(int maxConnectionPerRoute) {
            config.maxConnectionPerRoute = maxConnectionPerRoute > 0 ? maxConnectionPerRoute : DEFAULT_MAX_CONNECTION_PER_ROUTE;
            return this;
        }

        public Builder setBufferSize(int bufferSize) {
            config.bufferSize = bufferSize > 0 ? bufferSize : DEFAULT_BUFFER_SIZE;
            return this;
        }

        public Builder setIdleTimeout(long idleTimeout) {
            config.idleTimeout = idleTimeout > 0 ? idleTimeout : 0;
            return this;
        }

        public Builder setSocketSendBufferSize(int sendBufferSize) {
            config.socketSendBufferSize = sendBufferSize > 0 ? sendBufferSize : DEFAULT_SEND_BUFFER_SIZE;
            return this;
        }

        public Builder setSocketRecvBufferSize(int recvBufferSize) {
            config.socketRecvBufferSize = recvBufferSize > 0 ? recvBufferSize : DEFAULT_RECV_BUFFER_SIZE;
            return this;
        }

        public Builder setSocketTimeout(int socketTimeout) {
            config.socketTimeout = socketTimeout > 0 ? socketTimeout : DEFAULT_SOCKET_TIMEOUT;
            return this;
        }

        public Builder setConnectTimeout(int connectTimeout) {
            config.connectTimeout = connectTimeout > 0 ? connectTimeout : DEFAULT_CONNECT_TIMEOUT;
            return this;
        }

        public Builder setIOThreadNum(int threadNum) {
            config.ioThreadNum = threadNum > 0 ? threadNum : DEFAULT_IO_THREAD_NUM;
            return this;
        }

        public Builder setResponseTimeout(long timeout) {
            config.responseTimeout = timeout > 0 ? timeout : DEFAULT_RESPONSE_TIMEOUT;
            return this;
        }

        public Builder setKeepAlive(boolean keepalive) {
            config.keepAlive = keepalive;
            return this;
        }

        public ClientConfig build() {
            return config;
        }
    }
}
