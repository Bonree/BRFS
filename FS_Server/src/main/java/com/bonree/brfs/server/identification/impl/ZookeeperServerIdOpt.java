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
import com.bonree.brfs.server.identification.ServerIDOpt;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年3月19日 上午11:49:32
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description: 使用zookeeper实现获取单副本服务标识，多副本服务标识，虚拟服务标识
 * 为了安全性，此处的方法，不需要太高的效率，故使用synchronized字段,该实例为单例模式
 ******************************************************************************/
public class ZookeeperServerIdOpt implements ServerIDOpt {

    private static final Logger LOG = LoggerFactory.getLogger(ZookeeperServerIdOpt.class);

    private final String basePath;

    private CuratorClient client;

    private final static String NORMAL_DATA = "normal";

    private final static String INVALID_DATA = "invalid";

    private final static String FIRST_NODE = "firstID";

    private final static String SECOND_NODE = "secondID";

    private final static String VIRTUAL_NODE = "virtual";

    private final static String LOCKS_PATH_PART = "locks";

    private final static String VIRTUAL_SERVER = "virtual_servers";

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
            String virtualServerId = VIRTUAL_ID + getServersId(client, dataNode);
            String virtualServerNode = basePath + SEPARATOR + VIRTUAL_SERVER + SEPARATOR + storageIndex + SEPARATOR + virtualServerId;
            client.createPersistent(virtualServerNode, true, NORMAL_DATA.getBytes()); // 初始化的时候需要指定该节点为正常
            return virtualServerId;
        }
    }

    private class FirstGen implements Executor<String> {

        private final String dataNode;

        public FirstGen(String dataNode) {
            this.dataNode = dataNode;
        }

        @Override
        public String execute(CuratorClient client) {
            return FIRST_ID + getServersId(client, dataNode);
        }

    }

    private class SecondGen implements Executor<String> {

        private final String dataNode;

        public SecondGen(String dataNode) {
            this.dataNode = dataNode;
        }

        @Override
        public String execute(CuratorClient client) {
            return SECOND_ID + getServersId(client, dataNode);
        }

    }

    private String getServersId(CuratorClient client, String dataNode) {
        if (!client.checkExists(dataNode)) {
            client.createPersistent(dataNode, true, "0".getBytes());
        }
        byte[] bytes = client.getData(dataNode);
        String serverId = new String(bytes);
        int tmp = Integer.parseInt(new String(bytes)) + 1;
        client.setData(dataNode, String.valueOf(tmp).getBytes());
        return serverId;
    }

    private ZookeeperServerIdOpt(String zkUrl, String basePath) {
        client = CuratorClient.getClientInstance(zkUrl);
        this.basePath = BrStringUtils.trimBasePath(basePath);
        this.lockPath = basePath + SEPARATOR + LOCKS_PATH_PART;
        checkPathAndCreate(lockPath);
        checkPathAndCreate(basePath + SEPARATOR + VIRTUAL_SERVER);
    }

    public static volatile ZookeeperServerIdOpt identificationServer = null;

    public String getBasePath() {
        return basePath;
    }

    private void checkPathAndCreate(String path) {
        if (!client.checkExists(path)) {
            client.createPersistent(path, true);
        }
    }

    public static ServerIDOpt getIdentificationServer(final String zkUrl, final String basePath) {
        if (identificationServer == null) {
            synchronized (ZookeeperServerIdOpt.class) {
                if (identificationServer == null) {
                    identificationServer = new ZookeeperServerIdOpt(zkUrl, basePath);
                }
            }
        }
        return identificationServer;
    }

    @Override
    public synchronized String genFirstIdentification() {
        String serverId = null;
        String singleNode = basePath + SEPARATOR + FIRST_NODE;
        FirstGen genExecutor = new FirstGen(singleNode);
        CuratorLocksClient<String> lockClient = new CuratorLocksClient<String>(client, lockPath, genExecutor, "genSingleIdentification");
        try {
            serverId = lockClient.execute();
        } catch (Exception e) {
            LOG.error("getSingleIdentification error!", e);
        }
        return serverId;
    }

    @Override
    public synchronized String genSecondIndentification() {
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

    @Override
    public synchronized String genVirtualIdentification(int storageIndex) {
        String serverId = null;
        String virtualNode = basePath + SEPARATOR + VIRTUAL_NODE;
        VirtualGen genExecutor = new VirtualGen(virtualNode, storageIndex);
        CuratorLocksClient<String> lockClient = new CuratorLocksClient<String>(client, lockPath, genExecutor, "genVirtualIdentification");
        try {
            serverId = lockClient.execute();
        } catch (Exception e) {
            LOG.error("getVirtureIdentification error!", e);
        }
        return serverId;
    }

    @Override
    public synchronized List<String> getVirtualIdentification(int storageIndex, int count) {
        List<String> resultVirtualIds = new ArrayList<String>(count);
        String storageSIDPath = basePath + SEPARATOR + VIRTUAL_SERVER + SEPARATOR + storageIndex;
        List<String> virtualIds = client.getChildren(storageSIDPath);
        // 排除无效的虚拟ID
        virtualIds = filterVirtualInvalidId(storageIndex, virtualIds);
        if (virtualIds == null) {
            for (int i = 0; i < count; i++) {
                String tmp = genVirtualIdentification(storageIndex);
                resultVirtualIds.add(tmp);
            }
        } else {
            if (virtualIds.size() < count) {
                resultVirtualIds.addAll(virtualIds);
                int distinct = count - virtualIds.size();
                for (int i = 0; i < distinct; i++) {
                    String tmp = genVirtualIdentification(storageIndex);
                    resultVirtualIds.add(tmp);
                }
            } else {
                for (int i = 0; i < count; i++) {
                    resultVirtualIds.add(virtualIds.get(i));
                }
            }
        }
        return resultVirtualIds;
    }

    @Override
    public boolean invalidVirtualIden(int storageIndex, String id) {
        String node = basePath + SEPARATOR + VIRTUAL_SERVER + SEPARATOR + storageIndex + SEPARATOR + id;
        try {
            client.setData(node, INVALID_DATA.getBytes());
            return true;
        } catch (Exception e) {
            LOG.error("set node :" + node + "  error!", e);
        }
        return false;
    }

    @Override
    public boolean deleteVirtualIden(int storageIndex, String id) {
        String node = basePath + SEPARATOR + VIRTUAL_SERVER + SEPARATOR + storageIndex + SEPARATOR + id;
        try {
            client.guaranteedDelete(node, false);
            return true;
        } catch (Exception e) {
            LOG.error("delete the node: " + node + "  error!", e);
        }
        return false;
    }

    @Override
    public List<String> listVirtualIdentification(int storageIndex) {
        String storageSIDPath = basePath + SEPARATOR + VIRTUAL_SERVER + SEPARATOR + storageIndex;
        List<String> virtualIds = client.getChildren(storageSIDPath);
        // 过滤掉正在恢复的虚拟ID。
        return filterVirtualInvalidId(storageIndex, virtualIds);
    }

    private List<String> filterVirtualInvalidId(int storageIndex, List<String> virtualIds) {
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
