package com.bonree.brfs.duplication.rocksdb.connection.http;

import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.utils.CloseUtils;
import com.bonree.brfs.common.utils.PooledThreadFactory;
import com.bonree.brfs.duplication.rocksdb.connection.RegionNodeConnection;
import com.bonree.brfs.duplication.rocksdb.connection.RegionNodeConnectionPool;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/*******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date 2020/3/19 11:52
 * @Author: <a href=mailto:zhangqi@bonree.com>张奇</a>
 * @Description:
 ******************************************************************************/
public class HttpRegionNodeConnectionPool implements RegionNodeConnectionPool {
    private static final Logger LOG = LoggerFactory.getLogger(HttpRegionNodeConnectionPool.class);

    private static final int DEFAULT_CONNECTION_STATE_CHECK_INTERVAL = 5;
    private ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor(new PooledThreadFactory("region_node_connection_checker"));
    private final Table<String, String, HttpRegionNodeConnection> connectionCache = HashBasedTable.create();

    private ServiceManager serviceManager;

    public HttpRegionNodeConnectionPool(ServiceManager serviceManager) {
        this.serviceManager = serviceManager;
        exec.scheduleAtFixedRate(new ConnectionStateChecker(), 0, DEFAULT_CONNECTION_STATE_CHECK_INTERVAL, TimeUnit.SECONDS);
    }

    @Override
    public void close() {
        exec.shutdown();
        connectionCache.values().forEach(new Consumer<RegionNodeConnection>() {

            @Override
            public void accept(RegionNodeConnection connection) {
                CloseUtils.closeQuietly(connection);
            }
        });
        connectionCache.clear();
    }

    @Override
    public RegionNodeConnection getConnection(String serviceGroup, String serviceId) {
        HttpRegionNodeConnection connection = connectionCache.get(serviceGroup, serviceId);

        if (connection != null) {
            return connection;
        }

        synchronized (connectionCache) {
            Service service = serviceManager.getServiceById(serviceGroup, serviceId);
            if (service == null) {
                return null;
            }

            connection = new HttpRegionNodeConnection(service.getHost(), service.getPort());
            connection.connect();
            connectionCache.put(serviceGroup, serviceId, connection);
        }

        return connection;
    }

    private class ConnectionStateChecker implements Runnable {

        @Override
        public void run() {
            List<String> rows;
            synchronized (connectionCache) {
                rows = new ArrayList<String>(connectionCache.rowKeySet());
            }

            List<String> cols = new ArrayList<String>();
            for (String row : rows) {
                cols.clear();
                synchronized (connectionCache) {
                    cols.addAll(connectionCache.row(row).keySet());
                }

                for (String col : cols) {
                    HttpRegionNodeConnection conn = connectionCache.get(row, col);
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
