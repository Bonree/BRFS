package com.bonree.brfs.common;

import java.net.URI;

import org.apache.http.client.utils.URIBuilder;
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

    public final static String SEQUENCES = "sequences";

    public final static String SERVER_IDS = "server_ids";

    public final static String LOCKS = "locks";

    public final static String STORAGE_NAME = "storage_name";

    public final static String REBALANCE = "rebalance";

    public final static String ROUTES = "routes";

    public final static String USERS = "users";

    public final static String TASKS = "tasks";

    private final String clusterName;

    private final String zkHosts;

    private String baseClusterName;

    private String baseServerIdSeqPath;

    private String baseServerIdPath;

    private String baseLocksPath;

    private String baseSequencesPath;

    private String baseStorageNamePath;

    private String baseRebalancePath;

    private String baseRoutePath;

    private String baseUserPath;

    private String baseTaskPath;

    private ZookeeperPaths(final String clusterName, final String zkHosts) {
        this.clusterName = clusterName;
        this.zkHosts = zkHosts;
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

    public String getBaseTaskPath() {
        return baseTaskPath;
    }

    public void setBaseTaskPath(String baseTaskPath) {
        this.baseTaskPath = baseTaskPath;
    }

    public String getClusterName() {
        return clusterName;
    }

    public String getBaseClusterName() {
        return baseClusterName;
    }

    public String getBaseSequencesPath() {
        return baseSequencesPath;
    }

    public void setBaseSequencesPath(String baseSequencesPath) {
        this.baseSequencesPath = baseSequencesPath;
    }

    public void createZkPath() {
        CuratorClient client = null;
        try {
            client = CuratorClient.getClientInstance(zkHosts);
            createPathIfNotExist(client, baseClusterName);
            createPathIfNotExist(client, baseLocksPath);
            createPathIfNotExist(client, baseSequencesPath);
            createPathIfNotExist(client, baseServerIdSeqPath);
            createPathIfNotExist(client, baseServerIdPath);
            createPathIfNotExist(client, baseRebalancePath);
            createPathIfNotExist(client, baseRoutePath);
            createPathIfNotExist(client, baseStorageNamePath);
            createPathIfNotExist(client, baseUserPath);
            createPathIfNotExist(client, baseTaskPath);
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
        baseClusterName = SEPARATOR + ROOT + SEPARATOR + clusterName;
        setBaseServerIdPath(baseClusterName + SEPARATOR + SERVER_IDS);
        setBaseServerIdSeqPath(baseClusterName + SEPARATOR + SERVER_ID_SEQUENCES);
        setBaseLocksPath(baseClusterName + SEPARATOR + LOCKS);
        setBaseSequencesPath(baseClusterName + SEPARATOR + SEQUENCES);
        setBaseStorageNamePath(baseClusterName + SEPARATOR + STORAGE_NAME);
        setBaseRebalancePath(baseClusterName + SEPARATOR + REBALANCE);
        setBaseRoutePath(baseClusterName + SEPARATOR + ROUTES);
        setBaseUserPath(baseClusterName + SEPARATOR + USERS);
        setBaseTaskPath(baseClusterName + SEPARATOR + TASKS);
    }

    public static ZookeeperPaths create(final String clusterName, final String zkHosts) {
        BrStringUtils.checkNotEmpty(clusterName, clusterName + " is empty!!!");
        BrStringUtils.checkNotEmpty(zkHosts, zkHosts + " is empty!!!");
        ZookeeperPaths zkPaths = new ZookeeperPaths(clusterName, zkHosts);
        zkPaths.createPath();
        zkPaths.createZkPath();
        return zkPaths;
    }

    public static ZookeeperPaths getBasePath(final String clusterName,final String zkHosts) {
        BrStringUtils.checkNotEmpty(clusterName, clusterName + " is empty!!!");
        ZookeeperPaths zkPaths = new ZookeeperPaths(clusterName, zkHosts);
        zkPaths.createPath();
        return zkPaths;
    }

}
