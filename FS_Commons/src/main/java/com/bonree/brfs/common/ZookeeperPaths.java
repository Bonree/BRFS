package com.bonree.brfs.common;

import com.bonree.brfs.common.utils.BrStringUtils;
import java.util.Objects;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @date 2018年4月10日 下午1:56:42
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description: brfs的zookeeper公共路径维护
 ******************************************************************************/
public class ZookeeperPaths {

    private static final Logger LOG = LoggerFactory.getLogger(ZookeeperPaths.class);

    public static final String SEPARATOR = "/";

    public static final String ROOT = "brfs";

    /**
     * 用来生成自增的service identification
     */
    public static final String SERVER_ID_SEQUENCES = "server_id_sequences";

    public static final String SEQUENCES = "sequences";

    public static final String SERVER_IDS = "server_ids";

    public static final String LOCKS = "locks";

    public static final String STORAGE_NAME = "storage_name";

    public static final String REBALANCE = "rebalance";

    public static final String ROUTES = "routes";

    public static final String USERS = "users";

    public static final String TASKS = "tasks";

    public static final String RESOURCES = "resources";

    public static final String ROCKSDB = "rocksdb";

    public static final String DISCOVER = "discovery";
    public static final String BAS_SECOND_ID = "secondIDSet";
    public static final String BAS_NEW_ROUTE = "routeSet";
    public static final String DATA_NODE_META = "dataNodeMeta";

    private final String clusterName;

    private final CuratorFramework client;

    private String baseClusterName;

    private String baseServerIdSeqPath;

    private String baseServerIdPath;

    private String baseLocksPath;

    private String baseSequencesPath;

    private String baseRebalancePath;

    private String baseRoutePath;

    private String baseUserPath;

    private String baseTaskPath;

    private String baseResourcesPath;

    private String baseRocksDBPath;

    /**
     * discovery
     */
    private String baseDiscoveryPath;

    private String baseV2SecondIDPath;

    private String baseV2RoutePath;

    private String baseDataNodeMetaPath;

    public String getBaseDataNodeMetaPath() {
        return baseDataNodeMetaPath;
    }

    public void setBaseDataNodeMetaPath(String baseDataNodeMetaPath) {
        this.baseDataNodeMetaPath = baseDataNodeMetaPath;
    }

    public String getBaseV2SecondIDPath() {
        return baseV2SecondIDPath;
    }

    public void setBaseV2SecondIDPath(String baseV2SecondIDPath) {
        this.baseV2SecondIDPath = baseV2SecondIDPath;
    }

    public String getBaseV2RoutePath() {
        return baseV2RoutePath;
    }

    public void setBaseV2RoutePath(String baseV2RoutePath) {
        this.baseV2RoutePath = baseV2RoutePath;
    }

    private ZookeeperPaths(final String clusterName, final CuratorFramework client) {
        this.clusterName = clusterName;
        this.client = client;
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

    public String getBaseResourcesPath() {
        return baseResourcesPath;
    }

    public void setBaseResourcesPath(String baseResourcesPath) {
        this.baseResourcesPath = baseResourcesPath;
    }

    public String getBaseRocksDBPath() {
        return baseRocksDBPath;
    }

    public void setBaseRocksDBPath(String baseRocksDBPath) {
        this.baseRocksDBPath = baseRocksDBPath;
    }

    public String getBaseDiscoveryPath() {
        return baseDiscoveryPath;
    }

    public void setBaseDiscoveryPath(String baseDiscoveryPath) {
        this.baseDiscoveryPath = baseDiscoveryPath;
    }

    public void createZkPath() {
        createPathIfNotExist(client, baseClusterName);
        createPathIfNotExist(client, baseLocksPath);
        createPathIfNotExist(client, baseSequencesPath);
        createPathIfNotExist(client, baseServerIdSeqPath);
        createPathIfNotExist(client, baseServerIdPath);
        createPathIfNotExist(client, baseRebalancePath);
        createPathIfNotExist(client, baseRoutePath);
        createPathIfNotExist(client, baseUserPath);
        createPathIfNotExist(client, baseTaskPath);
        createPathIfNotExist(client, baseResourcesPath);
        createPathIfNotExist(client, baseRocksDBPath);
        createPathIfNotExist(client, baseDiscoveryPath);
        createPathIfNotExist(client, baseV2RoutePath);
        createPathIfNotExist(client, baseV2SecondIDPath);
        createPathIfNotExist(client, baseDataNodeMetaPath);
    }

    public void createPathIfNotExist(CuratorFramework curatorFramework, String path) {
        try {
            if (curatorFramework.checkExists().forPath(path) == null) {
                curatorFramework.create()
                                .creatingParentsIfNeeded()
                                .withMode(CreateMode.PERSISTENT)
                                .forPath(path);
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
        setBaseRebalancePath(baseClusterName + SEPARATOR + REBALANCE);
        setBaseRoutePath(baseClusterName + SEPARATOR + ROUTES);
        setBaseUserPath(baseClusterName + SEPARATOR + USERS);
        setBaseTaskPath(baseClusterName + SEPARATOR + TASKS);
        setBaseResourcesPath(baseClusterName + SEPARATOR + RESOURCES);
        setBaseRocksDBPath(baseClusterName + SEPARATOR + ROCKSDB);
        setBaseDiscoveryPath(baseClusterName + SEPARATOR + DISCOVER);
        setBaseV2RoutePath(baseClusterName + SEPARATOR + BAS_NEW_ROUTE);
        setBaseV2SecondIDPath(baseClusterName + SEPARATOR + BAS_SECOND_ID);
        setBaseDataNodeMetaPath(baseClusterName + SEPARATOR + DATA_NODE_META);

    }

    public static ZookeeperPaths create(final String clusterName, final CuratorFramework zkClient) {
        BrStringUtils.checkNotEmpty(clusterName, "clusterName is empty!!!");
        Objects.requireNonNull(zkClient, "zkClient is empty!!!");
        ZookeeperPaths zkPaths = new ZookeeperPaths(clusterName, zkClient);
        zkPaths.createPath();
        zkPaths.createZkPath();
        return zkPaths;
    }

    public static ZookeeperPaths getBasePath(final String clusterName, final CuratorFramework zkClient) {
        BrStringUtils.checkNotEmpty(clusterName, clusterName + " is empty!!!");
        ZookeeperPaths zkPaths = new ZookeeperPaths(clusterName, zkClient);
        zkPaths.createPath();
        return zkPaths;
    }

}
