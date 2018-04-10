package com.bonree.brfs.common;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.configuration.ServerConfig;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年4月10日 下午1:56:42
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description: brfs的zookeeper公共路径维护
 ******************************************************************************/
public class ZookeeperPaths {

    private final static Logger LOG = LoggerFactory.getLogger(ZookeeperPaths.class);

    public final static String SEPARATOR = "/";

    public final static String ROOT = "brfs";

    public final static String SERVER_ID_SEQUENCES = "server_id_sequences";

    public final static String SERVERS = "servers";

    public final static String LOCKS = "locks";

    public final static String STORAGE_NAMES = "storage_names";

    public final static String REBALANCE = "rebalance";

    public final static String ROUTE_ROLE = "route_role";

    public final static String USER = "user";

    private String baseServerIdPath;

    private String baseServersPath;

    private String baseLocksPath;

    private String baseStorageNamePath;

    private String baseRebalancePath;

    private String baseRoutePath;

    private String baseUserPath;

    public String getBaseServerIdPath() {
        return baseServerIdPath;
    }

    public void setBaseServerIdPath(String baseServerIdPath) {
        this.baseServerIdPath = baseServerIdPath;
    }

    public String getBaseServersPath() {
        return baseServersPath;
    }

    public void setBaseServersPath(String baseServersPath) {
        this.baseServersPath = baseServersPath;
    }

    public String getBaseLocksPath() {
        return baseLocksPath;
    }

    public void setBaseLocksPath(String baseLocksPath) {
        this.baseLocksPath = baseLocksPath;
    }

    public String getBaseStorageNamePath() {
        return baseStorageNamePath;
    }

    public void setBaseStorageNamePath(String baseStorageNamePath) {
        this.baseStorageNamePath = baseStorageNamePath;
    }

    public String getBaseRebalancePath() {
        return baseRebalancePath;
    }

    public void setBaseRebalancePath(String baseRebalancePath) {
        this.baseRebalancePath = baseRebalancePath;
    }

    public String getBaseRoutePath() {
        return baseRoutePath;
    }

    public void setBaseRoutePath(String baseRoutePath) {
        this.baseRoutePath = baseRoutePath;
    }

    public String getBaseUserPath() {
        return baseUserPath;
    }

    public void setBaseUserPath(String baseUserPath) {
        this.baseUserPath = baseUserPath;
    }

    public void createZkPath(ServerConfig config) {
        CuratorClient client = null;
        try {
            client = CuratorClient.getClientInstance(config.getZkNodes());
            createPathIfNotExist(client, baseLocksPath);
            createPathIfNotExist(client, baseServerIdPath);
            createPathIfNotExist(client, baseServersPath);
            createPathIfNotExist(client, baseRebalancePath);
            createPathIfNotExist(client, baseRoutePath);
            createPathIfNotExist(client, baseStorageNamePath);
            createPathIfNotExist(client, baseUserPath);
        } finally {
            client.close();
        }
    }

    public void createPathIfNotExist(CuratorClient client, String path) {
        try {
            if (!client.checkExists(path)) {
                client.createPersistent(path, true);
            }
        } catch (Exception e) {
            LOG.error("create path failed!!", e);
        }
    }

    public static ZookeeperPaths create(ServerConfig config) {
        String clusterName = config.getClusterName();
        if (StringUtils.isEmpty(clusterName)) {
            throw new IllegalStateException("clusterName is not empty!");
        }
        ZookeeperPaths zkPaths = new ZookeeperPaths();
        zkPaths.setBaseServerIdPath(SEPARATOR + ROOT + SEPARATOR + clusterName + SEPARATOR + SERVER_ID_SEQUENCES);
        zkPaths.setBaseLocksPath(SEPARATOR + ROOT + SEPARATOR + clusterName + SEPARATOR + LOCKS);
        zkPaths.setBaseServersPath(SEPARATOR + ROOT + SEPARATOR + clusterName + SEPARATOR + SERVERS);
        zkPaths.setBaseStorageNamePath(SEPARATOR + ROOT + SEPARATOR + clusterName + SEPARATOR + STORAGE_NAMES);
        zkPaths.setBaseRebalancePath(SEPARATOR + ROOT + SEPARATOR + clusterName + SEPARATOR + REBALANCE);
        zkPaths.setBaseRoutePath(SEPARATOR + ROOT + SEPARATOR + clusterName + SEPARATOR + ROUTE_ROLE);
        zkPaths.setBaseUserPath(SEPARATOR + ROOT + SEPARATOR + clusterName + SEPARATOR + USER);
        
        zkPaths.createZkPath(config);
        return zkPaths;
    }

}
