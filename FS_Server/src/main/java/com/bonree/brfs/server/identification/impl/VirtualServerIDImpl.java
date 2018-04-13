package com.bonree.brfs.server.identification.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.common.zookeeper.curator.locking.CuratorLocksClient;
import com.bonree.brfs.common.zookeeper.curator.locking.Executor;
import com.bonree.brfs.server.identification.IncreServerID;
import com.bonree.brfs.server.identification.VirtualServerID;
import com.bonree.brfs.server.identification.VirtualServerIDGen;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年4月13日 下午1:09:46
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description: virtual serverID 管理，此处可能需要进行缓存
 ******************************************************************************/
public class VirtualServerIDImpl implements VirtualServerID,VirtualServerIDGen{

    private static final Logger LOG = LoggerFactory.getLogger(VirtualServerIDImpl.class);

    private final String basePath;

    private String zkHosts;

    private final static String NORMAL_DATA = "normal";

    private final static String INVALID_DATA = "invalid";

    private final static String VIRTUAL_NODE = "virtual";

    private final static String LOCKS_PATH_PART = "locks";

    private final static String VIRTUAL_SERVER = "virtual_servers";

    private IncreServerID<String> increServerID = new SimpleIncreServerID();

    private final static String SEPARATOR = "/";

    private final String lockPath;

    private class VirtualGen implements Executor<String> {
        private final String dataNode;
        private final int storageIndex;

        public VirtualGen(String dataNode, int storageIndex) {
            this.dataNode = dataNode;
            this.storageIndex = storageIndex;
        }

        @Override
        public String execute(CuratorClient client) {
            String virtualServerId = VIRTUAL_ID + VirtualServerIDImpl.this.increServerID.increServerID(client, dataNode);
            String virtualServerNode = basePath + SEPARATOR + VIRTUAL_SERVER + SEPARATOR + storageIndex + SEPARATOR + virtualServerId;
            client.createPersistent(virtualServerNode, true, NORMAL_DATA.getBytes()); // 初始化的时候需要指定该节点为正常
            return virtualServerId;
        }
    }

    public VirtualServerIDImpl(String zkHosts, String basePath) {
        this.zkHosts = zkHosts;
        this.basePath = BrStringUtils.trimBasePath(basePath);
        this.lockPath = basePath + SEPARATOR + LOCKS_PATH_PART;
    }

    public String getBasePath() {
        return basePath;
    }

    @Override
    public synchronized String genVirtualID(int storageIndex) {
        String serverId = null;
        CuratorClient client = null;
        try {
            client = CuratorClient.getClientInstance(zkHosts);
            String virtualNode = basePath + SEPARATOR + VIRTUAL_NODE;
            VirtualGen genExecutor = new VirtualGen(virtualNode, storageIndex);
            CuratorLocksClient<String> lockClient = new CuratorLocksClient<String>(client, lockPath, genExecutor, "genVirtualIdentification");
            try {
                serverId = lockClient.execute();
            } catch (Exception e) {
                LOG.error("getVirtureIdentification error!", e);
            }
        } finally {
            if (client != null) {
                client.close();
            }
        }
        return serverId;
    }

    @Override
    public synchronized List<String> getVirtualID(int storageIndex, int count) {
        CuratorClient client = null;
        List<String> resultVirtualIds = new ArrayList<String>(count);
        try {
            client = CuratorClient.getClientInstance(zkHosts);
            String storageSIDPath = basePath + SEPARATOR + VIRTUAL_SERVER + SEPARATOR + storageIndex;
            List<String> virtualIds = client.getChildren(storageSIDPath);
            // 排除无效的虚拟ID
            virtualIds = filterVirtualInvalidId(client, storageIndex, virtualIds);
            if (virtualIds == null) {
                for (int i = 0; i < count; i++) {
                    String tmp = genVirtualID(storageIndex);
                    resultVirtualIds.add(tmp);
                }
            } else {
                if (virtualIds.size() < count) {
                    resultVirtualIds.addAll(virtualIds);
                    int distinct = count - virtualIds.size();
                    for (int i = 0; i < distinct; i++) {
                        String tmp = genVirtualID(storageIndex);
                        resultVirtualIds.add(tmp);
                    }
                } else {
                    for (int i = 0; i < count; i++) {
                        resultVirtualIds.add(virtualIds.get(i));
                    }
                }
            }
        } finally {
            if (client != null) {
                client.close();
            }
        }
        return resultVirtualIds;
    }

    @Override
    public boolean invalidVirtualIden(int storageIndex, String id) {
        String node = basePath + SEPARATOR + VIRTUAL_SERVER + SEPARATOR + storageIndex + SEPARATOR + id;
        CuratorClient client = null;
        try {
            client = CuratorClient.getClientInstance(zkHosts);
            client.setData(node, INVALID_DATA.getBytes());
            return true;
        } catch (Exception e) {
            LOG.error("set node :" + node + "  error!", e);
        } finally {
            if (client != null) {
                client.close();
            }
        }
        return false;
    }

    @Override
    public boolean deleteVirtualIden(int storageIndex, String id) {
        String node = basePath + SEPARATOR + VIRTUAL_SERVER + SEPARATOR + storageIndex + SEPARATOR + id;
        CuratorClient client = null;
        try {
            client = CuratorClient.getClientInstance(zkHosts);
            client.guaranteedDelete(node, false);
            return true;
        } catch (Exception e) {
            LOG.error("delete the node: " + node + "  error!", e);
        } finally {
            if (client != null) {
                client.close();
            }
        }
        return false;
    }

    @Override
    public List<String> listNormalVirtualID(int storageIndex) {
        String storageSIDPath = basePath + SEPARATOR + VIRTUAL_SERVER + SEPARATOR + storageIndex;
        CuratorClient client = null;
        try {
            client = CuratorClient.getClientInstance(zkHosts);
            List<String> virtualIds = client.getChildren(storageSIDPath);
            // 过滤掉正在恢复的虚拟ID。
            return filterVirtualInvalidId(client, storageIndex, virtualIds);
        } finally {
            if (client != null) {
                client.close();
            }
        }
    }

    @Override
    public List<String> listValidVirtualID(int storageIndex) {

        return null;
    }

    @Override
    public List<String> listAllVirtualID(int storageIndex) {
        String storageSIDPath = basePath + SEPARATOR + VIRTUAL_SERVER + SEPARATOR + storageIndex;
        CuratorClient client = null;
        try {
            client = CuratorClient.getClientInstance(zkHosts);
            return client.getChildren(storageSIDPath);
        } finally {
            if (client != null) {
                client.close();
            }
        }
    }

    private List<String> filterVirtualInvalidId(CuratorClient client, int storageIndex, List<String> virtualIds) {
        if (virtualIds != null && !virtualIds.isEmpty()) {
            Iterator<String> it = virtualIds.iterator();
            while (it.hasNext()) {
                String node = it.next();
                byte[] data = client.getData(basePath + SEPARATOR + VIRTUAL_SERVER + SEPARATOR + storageIndex + SEPARATOR + node);
                if (StringUtils.equals(new String(data), INVALID_DATA)) {
                    it.remove();
                }
            }
        }
        return virtualIds;
    }

}
