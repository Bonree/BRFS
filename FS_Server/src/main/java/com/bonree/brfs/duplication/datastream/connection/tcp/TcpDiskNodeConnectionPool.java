package com.bonree.brfs.duplication.datastream.connection.tcp;

import com.bonree.brfs.common.net.tcp.BaseMessage;
import com.bonree.brfs.common.net.tcp.BaseResponse;
import com.bonree.brfs.common.net.tcp.client.TcpClient;
import com.bonree.brfs.common.net.tcp.client.TcpClientConfig;
import com.bonree.brfs.common.net.tcp.client.TcpClientGroup;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnection;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnectionPool;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.Executor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TcpDiskNodeConnectionPool implements DiskNodeConnectionPool {
    private static final Logger LOG = LoggerFactory.getLogger(TcpDiskNodeConnectionPool.class);

    private Table<String, String, TcpDiskNodeConnection> connectionCache = HashBasedTable.create();

    private ServiceManager serviceManager;
    private TcpClientGroup<BaseMessage, BaseResponse, TcpClientConfig> tcpClientGroup;
    private Executor executor;

    public TcpDiskNodeConnectionPool(ServiceManager serviceManager,
                                     TcpClientGroup<BaseMessage, BaseResponse, TcpClientConfig> tcpClientGroup) {
        this(serviceManager, tcpClientGroup, null);
    }

    public TcpDiskNodeConnectionPool(ServiceManager serviceManager,
                                     TcpClientGroup<BaseMessage, BaseResponse, TcpClientConfig> tcpClientGroup,
                                     Executor executor) {
        this.serviceManager = serviceManager;
        this.tcpClientGroup = tcpClientGroup;
        this.executor = executor;
    }

    @Override
    public DiskNodeConnection getConnection(String serviceGroup, String serviceId) {
        TcpDiskNodeConnection connection = connectionCache.get(serviceGroup, serviceId);

        if (connection != null) {
            if (connection.isValid()) {
                return connection;
            }

            synchronized (connectionCache) {
                connectionCache.remove(serviceGroup, serviceId);
            }
        }

        try {
            synchronized (connectionCache) {
                Service service = serviceManager.getServiceById(serviceGroup, serviceId);
                if (service == null) {
                    return null;
                }

                TcpClient<BaseMessage, BaseResponse> client = tcpClientGroup.createClient(new TcpClientConfig() {

                    @Override
                    public SocketAddress remoteAddress() {
                        return new InetSocketAddress(service.getHost(), service.getPort());
                    }

                    @Override
                    public int connectTimeoutMillis() {
                        return 3000;
                    }
                }, executor);

                if (client == null) {
                    return null;
                }

                connection = new TcpDiskNodeConnection(client);
                connectionCache.put(serviceGroup, serviceId, connection);
            }

            return connection;
        } catch (Exception e) {
            LOG.error("connect tcp connection to disk node error", e);
        }

        return null;
    }

}
