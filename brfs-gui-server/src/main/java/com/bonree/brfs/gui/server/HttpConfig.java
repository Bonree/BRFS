package com.bonree.brfs.gui.server;

import com.fasterxml.jackson.annotation.JsonProperty;

public class HttpConfig {
    @JsonProperty("request.timeout.second")
    private int requestTimeout = 3;
    @JsonProperty("connect.timeout.second")
    private int connectTimeout = 3;
    @JsonProperty("read.timeout.second")
    private int readTimeout = 3;
    @JsonProperty("write.timeout.second")
    private int writeTimeout = 3;

    public HttpConfig() {
    }

    public int getRequestTimeout() {
        return requestTimeout;
    }

    public void setRequestTimeout(int requestTimeout) {
        this.requestTimeout = requestTimeout;
    }

    public int getConnectTimeout() {
        return connectTimeout;
    }

    public void setConnectTimeout(int connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    public int getReadTimeout() {
        return readTimeout;
    }

    public void setReadTimeout(int readTimeout) {
        this.readTimeout = readTimeout;
    }

    public int getWriteTimeout() {
        return writeTimeout;
    }

    public void setWriteTimeout(int writeTimeout) {
        this.writeTimeout = writeTimeout;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("HttpConfig{");
        sb.append("requestTimeout=").append(requestTimeout);
        sb.append(", connectTimeout=").append(connectTimeout);
        sb.append(", readTimeout=").append(readTimeout);
        sb.append(", writeTimeout=").append(writeTimeout);
        sb.append('}');
        return sb.toString();
    }
}
