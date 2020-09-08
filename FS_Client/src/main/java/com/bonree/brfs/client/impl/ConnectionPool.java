package com.bonree.brfs.client.impl;

import com.bonree.brfs.common.net.tcp.client.TcpClient;
import com.bonree.brfs.common.net.tcp.client.TcpClientCloseListener;
import com.bonree.brfs.common.net.tcp.client.TcpClientGroup;
import com.bonree.brfs.common.net.tcp.file.ReadObject;
import com.bonree.brfs.common.net.tcp.file.client.AsyncFileReaderCreateConfig;
import com.bonree.brfs.common.net.tcp.file.client.FileContentPart;
import com.bonree.brfs.common.service.Service;
import java.lang.reflect.Array;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectionPool {
    private static Logger LOG = LoggerFactory.getLogger(ConnectionPool.class);
    private TcpClientGroup<ReadObject, FileContentPart, AsyncFileReaderCreateConfig> group;
    private int connectionPerRoute;
    private ConcurrentHashMap<String, TcpClient<ReadObject, FileContentPart>[]> clientCache;

    private Random random = new Random();

    public ConnectionPool(int connectionPerRoute,
                          TcpClientGroup<ReadObject, FileContentPart, AsyncFileReaderCreateConfig> group) {
        this.connectionPerRoute = connectionPerRoute;
        this.group = group;
        this.clientCache = new ConcurrentHashMap<>();
    }

    @SuppressWarnings("unchecked")
    public TcpClient<ReadObject, FileContentPart> getConnection(Service service) {
        TcpClient<ReadObject, FileContentPart>[] clients = clientCache.get(service.getServiceId());

        TcpClient<ReadObject, FileContentPart> client = null;
        int index = random.nextInt(connectionPerRoute);

        if (clients == null) {
            clientCache.putIfAbsent(service.getServiceId(),
                                    (TcpClient<ReadObject, FileContentPart>[]) Array
                                        .newInstance(TcpClient.class, connectionPerRoute));
            clients = clientCache.get(service.getServiceId());
        }

        client = clients[index];
        if (client != null) {
            return client;
        }

        synchronized (clients) {
            if (clients[index] != null) {
                return clients[index];
            }

            try {
                client = group.createClient(new AsyncFileReaderCreateConfig() {

                    @Override
                    public SocketAddress remoteAddress() {
                        return new InetSocketAddress(service.getHost(), service.getExtraPort());
                    }

                    @Override
                    public int connectTimeoutMillis() {
                        return 3000;
                    }

                    @Override
                    public int maxPendingRead() {
                        return 1000 * 100;
                    }

                });

                if (client == null) {
                    return null;
                }

                client.setClientCloseListener(new TcpClientCloseListener() {

                    @Override
                    public void clientClosed() {
                        TcpClient<ReadObject, FileContentPart>[] clientArray = clientCache.get(service.getServiceId());
                        synchronized (clientArray) {
                            clientArray[index] = null;
                        }
                    }
                });

                clients[index] = client;
                return client;
            } catch (Exception e) {
                LOG.debug("get connection from {} happen error", service.getHost(), e);
            }
        }

        return null;
    }
}
