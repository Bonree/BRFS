package com.bonree.brfs.server.identification.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.common.zookeeper.curator.locking.CuratorLocksClient;
import com.bonree.brfs.common.zookeeper.curator.locking.Executor;
import com.bonree.brfs.server.identification.IncreServerID;
import com.bonree.brfs.server.identification.LevelServerIDGen;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年3月19日 上午11:49:32
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description: 使用zookeeper实现获取单副本服务标识，多副本服务标识，虚拟服务标识
 * 为了安全性，此处的方法，不需要太高的效率，故使用synchronized字段,该实例为单例模式
 ******************************************************************************/
public class SecondServerIDGenImpl implements LevelServerIDGen {

    private static final Logger LOG = LoggerFactory.getLogger(SecondServerIDGenImpl.class);

    private final String basePath;

    private final CuratorClient client;

    private final static String SECOND_NODE = "secondID";

    private final static String LOCKS_PATH_PART = "locks";

    private final static String SEPARATOR = "/";

    private final String lockPath;

    private IncreServerID<String> increServerID = new SimpleIncreServerID();

    private class SecondGen implements Executor<String> {

        private final String dataNode;

        public SecondGen(String dataNode) {
            this.dataNode = dataNode;
        }

        @Override
        public String execute(CuratorClient client) {
            return SECOND_ID + SecondServerIDGenImpl.this.increServerID.increServerID(client, dataNode);
        }

    }

    public SecondServerIDGenImpl(CuratorClient client, String basePath) {
        this.client = client;
        this.basePath = BrStringUtils.trimBasePath(basePath);
        this.lockPath = basePath + SEPARATOR + LOCKS_PATH_PART;
    }

    public String getBasePath() {
        return basePath;
    }

    @Override
    public synchronized String genLevelID() {
        String serverId = null;
        String multiNode = basePath + SEPARATOR + SECOND_NODE;
        SecondGen genExecutor = new SecondGen(multiNode);
        CuratorLocksClient<String> lockClient = new CuratorLocksClient<String>(client, lockPath, genExecutor, "genMultiIdentification");
        try {
            serverId = lockClient.execute();
        } catch (Exception e) {
            LOG.error("getMultiIndentification error!", e);
        }
        return serverId;
    }

}
