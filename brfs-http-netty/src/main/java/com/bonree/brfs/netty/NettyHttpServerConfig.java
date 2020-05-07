/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bonree.brfs.netty;

import com.bonree.brfs.common.http.HttpServerConfig;
import com.fasterxml.jackson.annotation.JsonProperty;

public class NettyHttpServerConfig implements HttpServerConfig {
    @JsonProperty
    private String host;

    @JsonProperty
    private int port = 8100;

    @JsonProperty
    private int sslPort;

    @JsonProperty
    private int acceptWorkerNum = 2;
    @JsonProperty
    private int requestHandleWorkerNum = 6;

    @JsonProperty
    private int connectTimeoutMillies = 30000;
    @JsonProperty
    private int backlog = 2048;
    @JsonProperty
    private boolean keepAlive = true;

    @JsonProperty
    private boolean tcpNoDelay = true;

    @JsonProperty
    private int maxHttpContentLength = 65 * 1024 * 1024;

    @Override
    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    @Override
    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getSslPort() {
        return sslPort;
    }

    public void setSslPort(int sslPort) {
        this.sslPort = sslPort;
    }

    public int getAcceptWorkerNum() {
        return acceptWorkerNum;
    }

    public void setAcceptWorkerNum(int num) {
        this.acceptWorkerNum = num;
    }

    public int getRequestHandleWorkerNum() {
        return requestHandleWorkerNum;
    }

    public void setRequestHandleWorkerNum(int requestHandleWorkerNum) {
        this.requestHandleWorkerNum = requestHandleWorkerNum;
    }

    public int getConnectTimeoutMillies() {
        return connectTimeoutMillies;
    }

    public void setConnectTimeoutMillies(int connectTimeoutMillies) {
        this.connectTimeoutMillies = connectTimeoutMillies;
    }

    public int getBacklog() {
        return backlog;
    }

    public void setBacklog(int backlog) {
        this.backlog = backlog;
    }

    public boolean isKeepAlive() {
        return keepAlive;
    }

    public void setKeepAlive(boolean keepAlive) {
        this.keepAlive = keepAlive;
    }

    public boolean isTcpNoDelay() {
        return tcpNoDelay;
    }

    public void setTcpNoDelay(boolean tcpNoDelay) {
        this.tcpNoDelay = tcpNoDelay;
    }

    public int getMaxHttpContentLength() {
        return maxHttpContentLength;
    }

    public void setMaxHttpContentLength(int maxHttpContentLength) {
        this.maxHttpContentLength = maxHttpContentLength;
    }

}
