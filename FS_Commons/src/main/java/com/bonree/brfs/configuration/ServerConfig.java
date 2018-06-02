package com.bonree.brfs.configuration;

import com.bonree.brfs.common.exception.ConfigParseException;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.utils.FileUtils;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年4月10日 下午2:13:19
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description: 针对服务的全局配置
 ******************************************************************************/
public class ServerConfig {

    private final String homePath;
    private final String clusterName;
    private final String zkHosts;
    private final String host;
    private final int port;
    private final int diskPort;
    private final long zkSessionTime;
    private final String dataPath;
    private final String logPath;

    private final int virtualDelay;
    private final int normalDelay;

    // 磁盘节点的默认服务组名
    public static final String DEFAULT_DISK_NODE_SERVICE_GROUP = "disk_group";
    // 副本节点的默认服务组名
    public static final String DEFAULT_DUPLICATION_SERVICE_GROUP = "duplicate_group";

    public ServerConfig(String homePath, String clusterName, String zkHosts, String host, int port, int diskPort, long zkSessionTime, String dataPath, String logPath, int virtualDelay, int normalDelay) {
        this.homePath = homePath;
        this.clusterName = clusterName;
        this.zkHosts = zkHosts;
        this.host = host;
        this.port = port;
        this.diskPort = diskPort;
        this.zkSessionTime = zkSessionTime;
        this.dataPath = dataPath;
        this.logPath = logPath;
        this.virtualDelay = virtualDelay;
        this.normalDelay = normalDelay;
    }

    public static ServerConfig parse(Configuration config, String homePath) throws ConfigParseException {
        String clusterName = config.getProperty(Configuration.CLUSTER_NAME, Configuration.CLUSTER_NAME_VALUE);
        String zkHosts = config.getProperty(Configuration.ZOOKEEPER_NODES, Configuration.ZOOKEEPER_NODES_VALUE);
        String host = config.getProperty(Configuration.NETWORK_HOST, Configuration.NETWORK_HOST_VALUE);
        String portStr = config.getProperty(Configuration.NETWORK_PORT, Configuration.NETWORK_PORT_VALUE);
        String diskPortStr = config.getProperty(Configuration.DISK_PORT, Configuration.DISK_PORT_VALUE);
        String zkSessionTimeStr = config.getProperty(Configuration.ZOOKEEPER_SESSION_TIMEOUT, Configuration.ZOOKEEPER_SESSION_TIMEOUT_VALUE);
        String dataPath = BrStringUtils.trimBasePath(config.getProperty(Configuration.PATH_DATA, Configuration.PATH_DATA_VALUE));
        String logPath = BrStringUtils.trimBasePath(config.getProperty(Configuration.PATH_LOGS, Configuration.PATH_LOGS_VALUE));
        FileUtils.createDir(dataPath, true);
        FileUtils.createDir(logPath, true);
        String recoverDelayTimeStr = config.getProperty(Configuration.GLOBAL_REPLICATION_RECOVER_AFTER_TIME, Configuration.GLOBAL_REPLICATION_RECOVER_AFTER_TIME_VALUE);
        String virtualDelayTimeStr = config.getProperty(Configuration.GLOBAL_REPLICATION_VIRTUAL_RECOVER_AFTER_TIME, Configuration.GLOBAL_REPLICATION_VIRTUAL_RECOVER_AFTER_TIME_VALUE);
        try {

            int recoverDelayTime = BrStringUtils.parseNumber(recoverDelayTimeStr, Integer.class);
            int virtualDelayTime = BrStringUtils.parseNumber(virtualDelayTimeStr, Integer.class);
            int port = BrStringUtils.parseNumber(portStr, Integer.class);
            int diskPort = BrStringUtils.parseNumber(diskPortStr, Integer.class);
            long zkSessionTime = BrStringUtils.parseNumber(zkSessionTimeStr, Long.class);
            return new ServerConfig(homePath, clusterName, zkHosts, host, port, diskPort, zkSessionTime, dataPath, logPath, virtualDelayTime, recoverDelayTime);
        } catch (NumberFormatException e) {
            throw new ConfigParseException("configuration error!!");
        }
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

    public String getZkHosts() {
        return zkHosts;
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

    public String getHomePath() {
        return homePath;
    }

    public int getDiskPort() {
        return diskPort;
    }

    public int getVirtualDelay() {
        return virtualDelay;
    }

    public int getNormalDelay() {
        return normalDelay;
    }

    @Override
    public String toString() {
        return "ServerConfig [homePath=" + homePath + ", clusterName=" + clusterName + ", zkHosts=" + zkHosts + ", host=" + host + ", port=" + port + ", diskPort=" + diskPort + ", zkSessionTime=" + zkSessionTime + ", dataPath=" + dataPath + ", logPath=" + logPath + ", virtualDelay=" + virtualDelay + ", normalDelay=" + normalDelay + "]";
    }

}
