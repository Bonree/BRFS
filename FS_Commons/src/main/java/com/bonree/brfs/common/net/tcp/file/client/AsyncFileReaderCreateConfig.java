package com.bonree.brfs.common.net.tcp.file.client;

import com.bonree.brfs.common.net.tcp.client.TcpClientConfig;

public interface AsyncFileReaderCreateConfig extends TcpClientConfig {
    /**
     * 异步读取文件时等待获取数据的最大读取请求数量
     *
     * @return
     */
    int maxPendingRead();
}
