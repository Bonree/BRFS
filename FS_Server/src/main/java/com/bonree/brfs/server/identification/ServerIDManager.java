package com.bonree.brfs.server.identification;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent.Type;
import org.jboss.netty.util.internal.ConcurrentHashMap;

import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.common.zookeeper.curator.cache.AbstractTreeCacheListener;
import com.bonree.brfs.common.zookeeper.curator.cache.CuratorCacheFactory;
import com.bonree.brfs.common.zookeeper.curator.cache.CuratorTreeCache;
import com.bonree.brfs.configuration.ServerConfig;
import com.bonree.brfs.server.identification.impl.FirstLevelServerIDImpl;
import com.bonree.brfs.server.identification.impl.VirtualServerIDImpl;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年3月27日 下午5:41:58
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description: 管理Identification
 ******************************************************************************/
public class ServerIDManager {

    private FirstLevelServerIDImpl firstLevelServerID;

    private VirtualServerID virtualServerID;

    private CuratorTreeCache secondIDCache = null;

    private final static String SINGLE_FILE = "/id/server_id";

    private Map<String, String> otherServerIDCache = null;

    private final static String SEPARATOR = ":";

    private class SecondIDCacheListener extends AbstractTreeCacheListener {

        public SecondIDCacheListener(String listenName) {
            super(listenName);
        }

        @Override
        public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
            if (event.getType() == Type.NODE_ADDED && event.getData() != null && event.getData().getData() != null) {
                String path = event.getData().getPath();
                String data = new String(event.getData().getData());
                if (!"".equals(data)) {
                    otherServerIDCache.put(extractSnIndex(path) + SEPARATOR + data, extractFirstID(path));
                }
            }
        }

        private String extractFirstID(String path) {
            String tmp = path.substring(0, path.lastIndexOf('/'));
            return tmp.substring(tmp.lastIndexOf('/') + 1, tmp.length());
        }

        private String extractSnIndex(String path) {
            return path.substring(path.lastIndexOf('/') + 1, path.length());
        }

    }

    public ServerIDManager(ServerConfig config, ZookeeperPaths zkBasePaths) {
        firstLevelServerID = new FirstLevelServerIDImpl(config.getZkHosts(), zkBasePaths.getBaseServerIdPath(), config.getHomePath() + SINGLE_FILE, zkBasePaths.getBaseServerIdSeqPath());
        firstLevelServerID.initOrLoadServerID();
        virtualServerID = new VirtualServerIDImpl(config.getZkHosts(), zkBasePaths.getBaseServerIdSeqPath());
        otherServerIDCache = new ConcurrentHashMap<>();
        loadSecondServerIDCache(config.getZkHosts(), zkBasePaths.getBaseServerIdPath());
        secondIDCache = CuratorCacheFactory.getTreeCache();
        secondIDCache.addListener(zkBasePaths.getBaseServerIdPath(), new SecondIDCacheListener("second_server_id_cache"));
    }

    private void loadSecondServerIDCache(String zkHosts, String serverIDsPath) {
        CuratorClient client = null;
        try {
            client = CuratorClient.getClientInstance(zkHosts);
            List<String> firstServerIDs = client.getChildren(serverIDsPath);
            if (firstServerIDs != null && !firstServerIDs.isEmpty()) {

                for (String firstServerID : firstServerIDs) {
                    List<String> sns = client.getChildren(serverIDsPath + "/" + firstServerID);
                    if (sns != null && !sns.isEmpty()) {
                        for (String sn : sns) {
                            byte[] secondServerID = client.getData(serverIDsPath + '/' + firstServerID + '/' + sn);
                            otherServerIDCache.put(sn + SEPARATOR + new String(secondServerID), firstServerID);
                        }
                    }
                }
            }
        } finally {
            if (client != null) {
                client.close();
            }
        }
    }

    public String getFirstServerID() {
        return firstLevelServerID.getServerID();
    }

    public String getSecondServerID(int storageIndex) {
        return firstLevelServerID.getSecondLevelServerID().getServerID(storageIndex);
    }

    public List<String> getVirtualServerID(int storageIndex, int count) {
        return virtualServerID.getVirtualID(storageIndex, count, getFirstServerID());
    }

    public boolean invalidVirtualID(int storageIndex, String id) {
        return virtualServerID.invalidVirtualIden(storageIndex, id);
    }

    public boolean deleteVirtualID(int storageIndex, String id) {
        return virtualServerID.deleteVirtualIden(storageIndex, id);
    }

    public List<String> listNormalVirtualID(int storageIndex) {
        return virtualServerID.listNormalVirtualID(storageIndex);
    }

    public List<String> listAllVirtualID(int storageIndex) {
        return virtualServerID.listAllVirtualID(storageIndex);
    }

    public List<String> listInvalidVirtualID(int storageIndex) {
        return virtualServerID.listInvalidVirtualID(storageIndex);
    }

    public String getOtherFirstID(String secondID, int snIndex) {
        return otherServerIDCache.get(snIndex + SEPARATOR + secondID);
    }

    public String getOtherSecondID(String firstID, int snIndex) {
        String secondID = null;
        for (Entry<String, String> entry : otherServerIDCache.entrySet()) {
            if (entry.getValue().equals(firstID)) {
                String[] arr = entry.getKey().split(SEPARATOR);
                String sn = arr[0];
                if (sn.equals(String.valueOf(snIndex))) {
                    secondID = arr[1];
                }
            }
        }
        return secondID;
    }

    public String getVirtualServersPath() {
        return virtualServerID.getVirtualServersPath();
    }

    public boolean registerFirstID(int storageIndex, String virtualID, String firstID) {
        return virtualServerID.registerFirstID(storageIndex, virtualID, firstID);
    }

    public static void main(String[] args) {
        String path = "/brfs/test1/server_ids/10/1";
        String aa = path.substring(0, path.lastIndexOf('/'));
        String bb = aa.substring(aa.lastIndexOf('/') + 1, aa.length());
        System.out.println(bb);
    }

}
