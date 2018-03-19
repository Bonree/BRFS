package com.bonree.brfs.server.identification.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.utils.StringUtils;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.common.zookeeper.curator.locking.CuratorLocksClient;
import com.bonree.brfs.server.identification.Identification;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年3月19日 上午11:49:32
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description: 使用zookeeper实现获取单副本服务标识，多副本服务标识，虚拟服务标识
 * 为了安全性，此处的方法，不需要太高的效率，故使用synchronized字段
 ******************************************************************************/
public class ZookeeperIdentification implements Identification {

    private static final Logger LOG = LoggerFactory.getLogger(ZookeeperIdentification.class);
    private final String basePath;
    private CuratorClient client;

    private final static String SINGLE_NODE = "/single";

    private final static String MULTI_NODE = "/multi";

    private final static String VIRTUAL_NODE = "/virtual";

    private final static String LOCKS_PATH_PART = "/locks";

    private final String lockPath;

    public ZookeeperIdentification(CuratorClient client, String basePath) {
        this.client = client;
        this.basePath = StringUtils.trimBasePath(basePath);
        this.lockPath = basePath + LOCKS_PATH_PART;
    }

    public String getBasePath() {
        return basePath;
    }

    private void checkPathAndCreate(String singlePath) {
        if (!client.checkExists(singlePath)) {
            client.createPersistent(singlePath, true);
        }
    }

    @Override
    public synchronized String getSingleIdentification() {
        checkPathAndCreate(lockPath);

        String singleNode = basePath + SINGLE_NODE;
        ZookeeperIdentificationGen genExecutor = new ZookeeperIdentificationGen(singleNode);
        CuratorLocksClient<String> lockClient = new CuratorLocksClient<String>(client, lockPath, genExecutor, "genSingleIdentification");
        try {
            return SINGLE + lockClient.execute();
        } catch (Exception e) {
            LOG.error("getSingleIdentification error!", e);
        }
        return null;
    }

    @Override
    public synchronized String getMultiIndentification() {
        checkPathAndCreate(lockPath);

        String multiNode = basePath + MULTI_NODE;
        ZookeeperIdentificationGen genExecutor = new ZookeeperIdentificationGen(multiNode);
        CuratorLocksClient<String> lockClient = new CuratorLocksClient<String>(client, lockPath, genExecutor, "genMultiIdentification");
        try {
            return MULTI + lockClient.execute();
        } catch (Exception e) {
            LOG.error("getMultiIndentification error!", e);
        }
        return null;
    }

    @Override
    public synchronized String getVirtureIdentification() {

        checkPathAndCreate(lockPath);
        String virtualNode = basePath + VIRTUAL_NODE;
        ZookeeperIdentificationGen genExecutor = new ZookeeperIdentificationGen(virtualNode);
        CuratorLocksClient<String> lockClient = new CuratorLocksClient<String>(client, lockPath, genExecutor, "genVirtualIdentification");
        try {
            return VIRTUAL + lockClient.execute();
        } catch (Exception e) {
            LOG.error("getVirtureIdentification error!", e);
        }
        return null;

    }
}
