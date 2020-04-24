package com.bonree.brfs.common.net.tcp.client;

import java.util.concurrent.Executor;

public interface TcpClientGroup<V, W, T extends TcpClientConfig> {
    TcpClient<V, W> createClient(T config) throws InterruptedException;

    TcpClient<V, W> createClient(T config, Executor executor) throws InterruptedException;
}
