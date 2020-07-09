package com.bonree.brfs.gui.server;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;

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
        return MoreObjects.toStringHelper(this)
                          .add("requestTimeout", requestTimeout)
                          .add("connectTimeout", connectTimeout)
                          .add("readTimeout", readTimeout)
                          .add("writeTimeout", writeTimeout)
                          .toString();
    }
}
