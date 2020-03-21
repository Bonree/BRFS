package com.bonree.brfs.duplication.rocksdb.connection.http;

import com.bonree.brfs.common.net.http.client.ClientConfig;
import com.bonree.brfs.common.utils.CloseUtils;
import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.RegionNodeConfigs;
import com.bonree.brfs.duplication.rocksdb.client.RegionNodeClient;
import com.bonree.brfs.duplication.rocksdb.client.impl.HttpRegionNodeClient;
import com.bonree.brfs.duplication.rocksdb.connection.RegionNodeConnection;

/*******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date 2020/3/19 11:57
 * @Author: <a href=mailto:zhangqi@bonree.com>张奇</a>
 * @Description:
 ******************************************************************************/
public class HttpRegionNodeConnection implements RegionNodeConnection {

    private static final int DEFAULT_RESPONSE_TIMEOUT_MILLIS = 15 * 1000;

    private static final int MAX_CONNECTION_RER_ROUTE = Configs.getConfiguration().GetConfig(RegionNodeConfigs.CONFIG_WRITER_WORKER_NUM);

    private String address;
    private int port;
    private RegionNodeClient client;

    public HttpRegionNodeConnection(String address, int port) {
        this.address = address;
        this.port = port;
    }

    public void connect() {
        ClientConfig clientConfig = ClientConfig.builder()
                .setResponseTimeout(DEFAULT_RESPONSE_TIMEOUT_MILLIS)
                .setMaxConnectionPerRoute(MAX_CONNECTION_RER_ROUTE)
                .setMaxConnection(MAX_CONNECTION_RER_ROUTE * 3)
                .build();

        client = new HttpRegionNodeClient(address, port, clientConfig);
    }


    @Override
    public String getRemoteAddress() {
        return this.address;
    }

    @Override
    public int getRemotePort() {
        return this.port;
    }

    @Override
    public boolean isValid() {
        return client != null && client.ping();
    }

    @Override
    public RegionNodeClient getClient() {
        return client;
    }

    @Override
    public void close() {
        CloseUtils.closeQuietly(client);
    }
}
