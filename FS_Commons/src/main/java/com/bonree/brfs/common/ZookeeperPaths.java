package com.bonree.brfs.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;

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

    public final static String SERVER_IDS = "server_ids";

    public final static String SERVERS = "servers";

    public final static String LOCKS = "locks";

    public final static String STORAGE_NAMES = "storage_names";

    public final static String REBALANCE = "rebalance";

    public final static String ROUTES = "routes";

    public final static String USERS = "users";

    private final String clusterName;
    private final String zkNodes;

    private String baseServerIdSeqPath;

    private String baseServerIdPath;

    private String baseServersPath;

    private String baseLocksPath;

    private String baseStorageNamePath;

    private String baseRebalancePath;

    private String baseRoutePath;

    private String baseUserPath;

    public ZookeeperPaths(final String clusterName, final String zkNodes) {
        this.clusterName = clusterName;
        this.zkNodes = zkNodes;
    }

    public String getBaseServerIdSeqPath() {
        return baseServerIdSeqPath;
    }

    public void setBaseServerIdSeqPath(String baseServerIdSeqPath) {
        this.baseServerIdSeqPath = baseServerIdSeqPath;
    }

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

    public void createZkPath() {
        CuratorClient client = null;
        try {
            client = CuratorClient.getClientInstance(zkNodes);
            createPathIfNotExist(client, baseLocksPath);
            createPathIfNotExist(client, baseServerIdSeqPath);
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

    private void createPath() {
        setBaseServerIdPath(SEPARATOR + ROOT + SEPARATOR + clusterName + SEPARATOR + SERVER_IDS);
        setBaseServerIdSeqPath(SEPARATOR + ROOT + SEPARATOR + clusterName + SEPARATOR + SERVER_ID_SEQUENCES);
        setBaseLocksPath(SEPARATOR + ROOT + SEPARATOR + clusterName + SEPARATOR + LOCKS);
        setBaseServersPath(SEPARATOR + ROOT + SEPARATOR + clusterName + SEPARATOR + SERVERS);
        setBaseStorageNamePath(SEPARATOR + ROOT + SEPARATOR + clusterName + SEPARATOR + STORAGE_NAMES);
        setBaseRebalancePath(SEPARATOR + ROOT + SEPARATOR + clusterName + SEPARATOR + REBALANCE);
        setBaseRoutePath(SEPARATOR + ROOT + SEPARATOR + clusterName + SEPARATOR + ROUTES);
        setBaseUserPath(SEPARATOR + ROOT + SEPARATOR + clusterName + SEPARATOR + USERS);
    }

    public static ZookeeperPaths create(final String clusterName, final String zkHosts) {
        BrStringUtils.checkNotEmpty(clusterName, clusterName + " is empty!!!");
        BrStringUtils.checkNotEmpty(zkHosts, zkHosts + " is empty!!!");
        ZookeeperPaths zkPaths = new ZookeeperPaths(clusterName, zkHosts);
        zkPaths.createPath();
        zkPaths.createZkPath();
        return zkPaths;
    }

}
