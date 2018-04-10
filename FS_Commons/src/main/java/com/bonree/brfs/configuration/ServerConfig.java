package com.bonree.brfs.configuration;

import com.bonree.brfs.common.utils.BrStringUtils;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年4月10日 下午2:13:19
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description: 针对服务的全局配置
 ******************************************************************************/
public class ServerConfig {

    private final String clusterName;
    private final String zkNodes;
    private final String host;
    private final int port;
    private final long zkSessionTime;
    private final String dataPath;
    private final String logPath;

    public ServerConfig(String clusterName, String zkNodes, String host, int port, long zkSessionTime, String dataPath, String logPath) {
        this.clusterName = clusterName;
        this.zkNodes = zkNodes;
        this.host = host;
        this.port = port;
        this.zkSessionTime = zkSessionTime;
        this.dataPath = dataPath;
        this.logPath = logPath;
    }

    public static ServerConfig parse(Configuration config) {
        String clusterName = config.getProperty(Configuration.CLUSTER_NAME, Configuration.CLUSTER_NAME_VALUE);
        String zkNodes = config.getProperty(Configuration.ZOOKEEPER_NODES, Configuration.ZOOKEEPER_NODES_VALUE);
        String host = config.getProperty(Configuration.NETWORK_HOST, Configuration.NETWORK_HOST_VALUE);
        String portStr = config.getProperty(Configuration.NETWORK_PORT, Configuration.NETWORK_PORT_VALUE);
        String zkSessionTimeStr = config.getProperty(Configuration.ZOOKEEPER_SESSION_TIMEOUT, Configuration.ZOOKEEPER_SESSION_TIMEOUT_VALUE);
        String dataPath = config.getProperty(Configuration.PATH_DATA, Configuration.PATH_DATA_VALUE);
        String logPath = config.getProperty(Configuration.PATH_LOGS, Configuration.PATH_LOGS_VALUE);
        int port = BrStringUtils.parseNumber(portStr, Integer.class);
        long zkSessionTime = BrStringUtils.parseNumber(zkSessionTimeStr, Long.class);
        return new ServerConfig(clusterName, zkNodes, host, port, zkSessionTime, dataPath, logPath);
    }

    public String getClusterName() {
        return clusterName;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getZkNodes() {
        return zkNodes;
    }

    public long getZkSessionTime() {
        return zkSessionTime;
    }

    public String getDataPath() {
        return dataPath;
    }

    public String getLogPath() {
        return logPath;
    }

    @Override
    public String toString() {
        return "ServerConfig [clusterName=" + clusterName + ", zkNodes=" + zkNodes + ", host=" + host + ", port=" + port + ", zkSessionTime=" + zkSessionTime + ", dataPath=" + dataPath + ", logPath=" + logPath + "]";
    }

}
