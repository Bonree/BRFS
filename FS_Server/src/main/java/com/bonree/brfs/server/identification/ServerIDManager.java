package com.bonree.brfs.server.identification;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent.Type;

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
public class ServerIDManager implements Closeable {

    private FirstLevelServerIDImpl firstLevelServerID;

    private VirtualServerID virtualServerID;

    private CuratorTreeCache secondIDCache = null;

    private final static String SINGLE_FILE_DIR = "/id/server_id";

    private Map<String, String> otherServerIDCache = null;

    private final static String SEPARATOR = ":";
    private CuratorClient curatorClient;

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
        curatorClient = CuratorClient.getClientInstance(config.getZkHosts());
        firstLevelServerID = new FirstLevelServerIDImpl(curatorClient, zkBasePaths.getBaseServerIdPath(), config.getHomePath() + SINGLE_FILE_DIR, zkBasePaths.getBaseServerIdSeqPath(), zkBasePaths.getBaseRoutePath());
        virtualServerID = new VirtualServerIDImpl(curatorClient, zkBasePaths.getBaseServerIdSeqPath());
        otherServerIDCache = new ConcurrentHashMap<>();
        loadSecondServerIDCache(curatorClient, zkBasePaths.getBaseServerIdPath());
        secondIDCache = CuratorCacheFactory.getTreeCache();
        secondIDCache.addListener(zkBasePaths.getBaseServerIdPath(), new SecondIDCacheListener("second_server_id_cache"));
    }

    private void loadSecondServerIDCache(CuratorClient client, String serverIDsPath) {
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
    }

    /** 概述：获取本服务的1级serverID
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public String getFirstServerID() {
        return firstLevelServerID.getServerID();
    }

    /** 概述：判断是否为新服务
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    @Deprecated
    public boolean isNewService() {
        return firstLevelServerID.isNewServer();
    }

    /** 概述：获取本服务的某个SN的2级serverID
     * @param storageIndex
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public String getSecondServerID(int storageIndex) {
        return firstLevelServerID.getSecondLevelServerID().getServerID(storageIndex);
    }

    /** 概述：删除SN的时候，需要删除相应的SN的2级server id
     * @param storageIndex
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public boolean deleteSecondServerID(int storageIndex) {
        return firstLevelServerID.getSecondLevelServerID().deleteServerID(storageIndex);
    }

    /** 概述：获取某个SN的virtual server ID
     * @param storageIndex
     * @param count
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public List<String> getVirtualServerID(int storageIndex, int count) {
        return virtualServerID.getVirtualID(storageIndex, count, getFirstServerID());
    }

    /** 概述：将一个virtual server ID置为无效
     * @param storageIndex
     * @param id
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public boolean invalidVirtualID(int storageIndex, String id) {
        return virtualServerID.invalidVirtualIden(storageIndex, id);
    }

    /** 概述：将一个virtual server ID恢复正常
     * @param storageIndex
     * @param id
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public boolean normalVirtualID(int storageIndex, String id) {
        return virtualServerID.normalVirtualIden(storageIndex, id);
    }

    /** 概述：删除一个virtual server ID
     * @param storageIndex
     * @param id
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public boolean deleteVirtualID(int storageIndex, String id) {
        return virtualServerID.deleteVirtualIden(storageIndex, id);
    }

    /** 概述：列出某个SN所有正常的virtual server ID
     * @param storageIndex
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public List<String> listNormalVirtualID(int storageIndex) {
        return virtualServerID.listNormalVirtualID(storageIndex);
    }

    /** 概述：列出某个sn所有的virtual server ID
     * @param storageIndex
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public List<String> listAllVirtualID(int storageIndex) {
        return virtualServerID.listAllVirtualID(storageIndex);
    }

    /** 概述：列出某个SN所有的无效的virtual server ID
     * @param storageIndex
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public List<String> listInvalidVirtualID(int storageIndex) {
        return virtualServerID.listInvalidVirtualID(storageIndex);
    }

    /** 概述：获取其他服务的1级serverID
     * @param secondID
     * @param snIndex
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public String getOtherFirstID(String secondID, int snIndex) {
        return otherServerIDCache.get(snIndex + SEPARATOR + secondID);
    }

    /** 概述：获取其他服务的2级serverID
     * @param firstID
     * @param snIndex
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public String getOtherSecondID(String firstID, int snIndex) {
        String secondID = null;
        while (true) {
            for (Entry<String, String> entry : otherServerIDCache.entrySet()) {
                if (entry.getValue().equals(firstID)) {
                    String[] arr = entry.getKey().split(SEPARATOR);
                    String sn = arr[0];
                    if (sn.equals(String.valueOf(snIndex))) {
                        secondID = arr[1];
                    }
                }
            }
            if (secondID != null) {
                break;
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return secondID;
    }

    /** 概述：获取virtual server路径
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public String getVirtualServersPath() {
        return virtualServerID.getVirtualServersPath();
    }

    /** 概述：将某个服务注册到virtual server中
     * @param storageIndex
     * @param virtualID
     * @param firstID
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public boolean registerFirstID(int storageIndex, String virtualID, String firstID) {
        return virtualServerID.registerFirstID(storageIndex, virtualID, firstID);
    }

    @Override
    public void close() throws IOException {
        if (curatorClient != null) {
            curatorClient.close();
        }
    }

}
