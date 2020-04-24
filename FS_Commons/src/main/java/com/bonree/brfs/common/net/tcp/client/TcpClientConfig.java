package com.bonree.brfs.common.net.tcp.client;

import java.net.SocketAddress;

public interface TcpClientConfig {
    /**
     * 远程文件服务的地址信息
     *
     * @return
     */
    SocketAddress remoteAddress();

    /**
     * 连接远程服务的超时时间
     *
     * @return
     */
    int connectTimeoutMillis();
}
