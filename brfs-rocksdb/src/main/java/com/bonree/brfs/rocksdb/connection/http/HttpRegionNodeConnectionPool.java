package com.bonree.brfs.rocksdb.connection.http;

import com.bonree.brfs.common.lifecycle.LifecycleStop;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.utils.CloseUtils;
import com.bonree.brfs.rocksdb.connection.RegionNodeConnection;
import com.bonree.brfs.rocksdb.connection.RegionNodeConnectionPool;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import java.util.function.Consumer;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private final Table<String, String, HttpRegionNodeConnection> connectionCache = HashBasedTable.create();

    private ServiceManager serviceManager;

    @Inject
    public HttpRegionNodeConnectionPool(ServiceManager serviceManager) {
        this.serviceManager = serviceManager;
    }

    @LifecycleStop
    @Override
    public void close() {
        connectionCache.values().forEach(new Consumer<RegionNodeConnection>() {

            @Override
            public void accept(RegionNodeConnection connection) {
                CloseUtils.closeQuietly(connection);
            }
        });
        connectionCache.clear();
        LOG.info("http region node connection pool close");
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

}
