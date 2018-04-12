package com.bonree.brfs.server.sequence;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.common.zookeeper.curator.locking.CuratorLocksClient;
import com.bonree.brfs.common.zookeeper.curator.locking.Executor;
import com.bonree.brfs.server.identification.impl.ZookeeperServerIdOpt;
import com.google.common.base.Preconditions;


/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年3月27日 下午3:22:45
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description: storageIndex生成器
 ******************************************************************************/
public class StorageSequenceGenetor {

    private static final Logger LOG = LoggerFactory.getLogger(StorageSequenceGenetor.class);

    private final String basePath;

    private final String zkUrl;

    private final static String STORAGE_INDEX_NODE = "storage_index";

    private final static String LOCKS_PATH = "lock";

    private static String SEPARATOR = "/";

    public static volatile StorageSequenceGenetor storageSequenceGenetor = null;

    private StorageSequenceGenetor(String zkUrl, String basePath) {
        this.zkUrl = Preconditions.checkNotNull(zkUrl, "StorageIndexGenerator zkUrl is not null!");
        this.basePath = Preconditions.checkNotNull(basePath, "StorageIndexGenerator basePath is not null");
    }

    private class ZKIncreSequence implements Executor<Integer> {
        private final String dataNode;

        public ZKIncreSequence(String dataNode) {
            this.dataNode = dataNode;
        }

        @Override
        public Integer execute(CuratorClient client) {
            if (!client.checkExists(dataNode)) {
                client.createPersistent(dataNode, true, "0".getBytes());
            }
            byte[] bytes = client.getData(dataNode);
            String serverId = new String(bytes);
            int tmp = Integer.parseInt(new String(bytes)) + 1;
            client.setData(dataNode, String.valueOf(tmp).getBytes());
            return Integer.valueOf(serverId);
        }

    }

    public static StorageSequenceGenetor getInstance(final String zkUrl, final String basePath) {
        if (storageSequenceGenetor == null) {
            synchronized (ZookeeperServerIdOpt.class) {
                if (storageSequenceGenetor == null) {
                    storageSequenceGenetor = new StorageSequenceGenetor(zkUrl, basePath);
                }
            }
        }
        return storageSequenceGenetor;
    }

    public Integer getIncreSequence() {
        int sequence = 0;
        CuratorClient client = null;
        try {
            client = CuratorClient.getClientInstance(zkUrl);
            String node = basePath + SEPARATOR + STORAGE_INDEX_NODE;
            ZKIncreSequence genExecutor = new ZKIncreSequence(node);
            CuratorLocksClient<Integer> lockClient = new CuratorLocksClient<Integer>(client,basePath + SEPARATOR+ LOCKS_PATH, genExecutor, "genSingleIdentification");
            sequence = lockClient.execute();

        } catch (Exception e) {
            LOG.error("getSingleIdentification error!", e);
        } finally {
            if (client != null) {
                client.close();
            }
        }
        return sequence;
    }

}
