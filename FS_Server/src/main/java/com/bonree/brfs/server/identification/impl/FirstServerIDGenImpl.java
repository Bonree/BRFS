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
public class FirstServerIDGenImpl implements LevelServerIDGen {

    private static final Logger LOG = LoggerFactory.getLogger(FirstServerIDGenImpl.class);

    private final String basePath;

    private final String zkHosts;

    private final static String FIRST_NODE = "firstID";

    private final static String LOCKS_PATH_PART = "locks";

    private final static String SEPARATOR = "/";
    
    private IncreServerID<String> increServerID = new SimpleIncreServerID();

    private final String lockPath;

    private class FirstGen implements Executor<String> {

        private final String dataNode;

        public FirstGen(String dataNode) {
            this.dataNode = dataNode;
        }

        @Override
        public String execute(CuratorClient client) {
            return FIRST_ID + FirstServerIDGenImpl.this.increServerID.increServerID(client, dataNode);
        }

    }

    public FirstServerIDGenImpl(String zkHosts, String basePath) {
        this.zkHosts = zkHosts;
        this.basePath = BrStringUtils.trimBasePath(basePath);
        this.lockPath = basePath + SEPARATOR + LOCKS_PATH_PART;
    }

    public String getBasePath() {
        return basePath;
    }

    @Override
    public synchronized String genLevelID() {
        CuratorClient client = null;
        String serverId = null;
        try {
            client = CuratorClient.getClientInstance(zkHosts);
            String singleNode = basePath + SEPARATOR + FIRST_NODE;
            FirstGen genExecutor = new FirstGen(singleNode);
            CuratorLocksClient<String> lockClient = new CuratorLocksClient<String>(client, lockPath, genExecutor, "genSingleIdentification");
            try {
                serverId = lockClient.execute();
            } catch (Exception e) {
                LOG.error("getSingleIdentification error!", e);
            }
        } finally {
            if (client != null) {
                client.close();
            }
        }
        return serverId;
    }

}
