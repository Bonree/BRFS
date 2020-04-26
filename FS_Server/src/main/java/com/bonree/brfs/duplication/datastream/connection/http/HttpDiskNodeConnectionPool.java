package com.bonree.brfs.duplication.datastream.connection.http;

import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.utils.CloseUtils;
import com.bonree.brfs.common.utils.PooledThreadFactory;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnection;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnectionPool;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpDiskNodeConnectionPool implements DiskNodeConnectionPool, Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(HttpDiskNodeConnectionPool.class);

    private static final int DEFAULT_CONNECTION_STATE_CHECK_INTERVAL = 3;
    private ScheduledExecutorService exec =
        Executors.newSingleThreadScheduledExecutor(new PooledThreadFactory("connection_checker"));
    private Table<String, String, HttpDiskNodeConnection> connectionCache = HashBasedTable.create();

    private ServiceManager serviceManager;

    public HttpDiskNodeConnectionPool(ServiceManager serviceManager) {
        this.serviceManager = serviceManager;
        exec.scheduleAtFixedRate(new ConnectionStateChecker(), 0, DEFAULT_CONNECTION_STATE_CHECK_INTERVAL, TimeUnit.SECONDS);
    }

    @Override
    public void close() {
        exec.shutdown();
        connectionCache.values().forEach(new Consumer<DiskNodeConnection>() {

            @Override
            public void accept(DiskNodeConnection connection) {
                CloseUtils.closeQuietly(connection);
            }
        });
        connectionCache.clear();
    }

    @Override
    public DiskNodeConnection getConnection(String serviceGroup, String serviceId) {
        HttpDiskNodeConnection connection = connectionCache.get(serviceGroup, serviceId);

        if (connection != null) {
            return connection;
        }

        synchronized (connectionCache) {
            Service service = serviceManager.getServiceById(serviceGroup, serviceId);
            if (service == null) {
                return null;
            }

            connection = new HttpDiskNodeConnection(service.getHost(), service.getPort());
            connection.connect();
            connectionCache.put(serviceGroup, serviceId, connection);
        }

        return connection;
    }

    private class ConnectionStateChecker implements Runnable {

        @Override
        public void run() {
            List<String> rows = new ArrayList<String>();
            synchronized (connectionCache) {
                rows.addAll(connectionCache.rowKeySet());
            }

            List<String> cols = new ArrayList<String>();
            for (String row : rows) {
                cols.clear();
                synchronized (connectionCache) {
                    cols.addAll(connectionCache.row(row).keySet());
                }

                for (String col : cols) {
                    HttpDiskNodeConnection conn = connectionCache.get(row, col);
                    if (conn != null && !conn.isValid()) {
                        LOG.info("Connection to service[{}, {}] is invalid!", row, col);

                        synchronized (connectionCache) {
                            connectionCache.remove(row, col);
                        }

                        CloseUtils.closeQuietly(conn);
                    }
                }
            }
        }

    }
}
