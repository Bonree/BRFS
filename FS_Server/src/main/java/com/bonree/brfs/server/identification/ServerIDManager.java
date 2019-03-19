package com.bonree.brfs.server.identification;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent.Type;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.zookeeper.curator.cache.CuratorCacheFactory;
import com.bonree.brfs.common.zookeeper.curator.cache.CuratorTreeCache;
import com.bonree.brfs.configuration.SystemProperties;
import com.bonree.brfs.server.identification.impl.FirstLevelServerIDImpl;
import com.bonree.brfs.server.identification.impl.SecondLevelServerID;
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
	private static final Logger LOG = LoggerFactory.getLogger(ServerIDManager.class);
	
    private FirstLevelServerIDImpl firstLevelServerID;
    
    private final String firstServerId;
    
    private SecondLevelServerID secondServerID;

    private VirtualServerID virtualServerID;

    private CuratorTreeCache secondIDCache = null;

    private final static String SINGLE_FILE_DIR = new File(System.getProperty(SystemProperties.PROP_SERVER_ID_DIR), "disknode_id").getAbsolutePath();

    private Map<String, String> otherServerIDCache = new ConcurrentHashMap<>();

    private final static String SEPARATOR = ":";

    private class SecondIDCacheListener implements TreeCacheListener {

        @Override
        public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
            if (event.getType() == Type.NODE_ADDED && event.getData() != null && event.getData().getData() != null) {
                String path = event.getData().getPath();
                String firstID = extractFirstID(path);
                String sn = extractSnIndex(path);
                String data = new String(event.getData().getData(), StandardCharsets.UTF_8);
                if (!"".equals(data)) {
                    LOG.info("add otherServerIDCache:{}", sn + SEPARATOR + data);
                    otherServerIDCache.put(sn + SEPARATOR + data, firstID);
                }
            } else if (event.getType() == Type.NODE_REMOVED && event.getData() != null) {
                String path = event.getData().getPath();
                String firstID = extractFirstID(path);
                String sn = extractSnIndex(path);
                String secondID = getOtherSecondID(firstID, Integer.parseInt(sn));
                LOG.info("remove otherServerIDCache:{}", sn + SEPARATOR + secondID);
                otherServerIDCache.remove(sn + SEPARATOR + secondID);
            } else if(event.getType() == Type.NODE_UPDATED && event.getData() != null && event.getData().getData() != null){
                String path = event.getData().getPath();
                String firstID = extractFirstID(path);
                String sn = extractSnIndex(path);
                String secondID = getOtherSecondID(firstID, Integer.parseInt(sn));
                LOG.info("update remove otherServerIDCache:{}", sn + SEPARATOR + secondID);
                otherServerIDCache.remove(sn + SEPARATOR + secondID);

                String data = new String(event.getData().getData(), StandardCharsets.UTF_8);
                if (!"".equals(data)) {
                    LOG.info("update add otherServerIDCache:{}", sn + SEPARATOR + data);
                    otherServerIDCache.put(sn + SEPARATOR + data, firstID);
                }
            }else {
                LOG.info("ignore  invalid event!!!");
            }

            LOG.info("otherSecondIDCache summary:{}",otherServerIDCache);
        }

        private String extractFirstID(String path) {
            String tmp = path.substring(0, path.lastIndexOf('/'));
            return tmp.substring(tmp.lastIndexOf('/') + 1, tmp.length());
        }

        private String extractSnIndex(String path) {
            return path.substring(path.lastIndexOf('/') + 1, path.length());
        }

    }

    public ServerIDManager(CuratorFramework client, ZookeeperPaths zkBasePaths) {
        firstLevelServerID = new FirstLevelServerIDImpl(client, zkBasePaths.getBaseServerIdPath(), SINGLE_FILE_DIR, zkBasePaths.getBaseServerIdSeqPath());
        virtualServerID = new VirtualServerIDImpl(client, zkBasePaths.getBaseServerIdSeqPath());
        loadSecondServerIDCache(client, zkBasePaths.getBaseServerIdPath());
        secondIDCache = CuratorCacheFactory.getTreeCache();
        secondIDCache.addListener(zkBasePaths.getBaseServerIdPath(), new SecondIDCacheListener());
        
        firstServerId = firstLevelServerID.initOrLoadServerID();
        
        secondServerID = new SecondLevelServerID(client, zkBasePaths.getBaseServerIdPath() + '/' + firstServerId, zkBasePaths.getBaseServerIdSeqPath(), zkBasePaths.getBaseRoutePath());
        secondServerID.loadServerID();
    }

    private void loadSecondServerIDCache(CuratorFramework client, String serverIDsPath) {
    	try {
    		List<String> firstServerIDs = client.getChildren().forPath(serverIDsPath);
            if (firstServerIDs != null) {
                for (String firstServerID : firstServerIDs) {
                	try {
                		List<String> sns = client.getChildren().forPath(ZKPaths.makePath(serverIDsPath, firstServerID));
                        if (sns != null) {
                            for (String sn : sns) {
                            	try {
                            		byte[] secondServerID = client.getData().forPath(ZKPaths.makePath(serverIDsPath, firstServerID, sn));
                                    otherServerIDCache.put(sn + SEPARATOR + new String(secondServerID,
                                                    StandardCharsets.UTF_8),
                                            firstServerID);
								} catch (Exception e) {
									LOG.error("get second server id error", e);
								}
                            }
                        }
					} catch (Exception e) {
						LOG.error("get sn list error", e);
					}
                }
            }
            LOG.info("load all second server ID cache:{}", otherServerIDCache);
		} catch (Exception e) {
			LOG.error("get server id list error", e);
		}
    }

    /** 概述：获取本服务的1级serverID
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public String getFirstServerID() {
        return firstServerId;
    }

    /** 概述：获取本服务的某个SN的2级serverID
     * @param storageIndex
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public String getSecondServerID(int storageIndex) {
        return secondServerID.getServerID(storageIndex);
    }

    /** 概述：删除SN的时候，需要删除相应的SN的2级server id
     * @param storageIndex
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public boolean deleteSecondServerID(int storageIndex) {
        return secondServerID.deleteServerID(storageIndex);
    }

    /** 概述：获取某个SN的virtual server ID
     * @param storageIndex
     * @param count
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public List<String> getVirtualServerID(int storageIndex, int count, List<String> diskFirstIds) {
        return virtualServerID.getVirtualID(storageIndex, count, diskFirstIds);
    }

    /** 概述：将一个virtual server ID置为无效
     * @param storageIndex
     * @param id
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public boolean invalidVirtualID(int storageIndex, String id) {
        return virtualServerID.invalidVirtualId(storageIndex, id);
    }

    /** 概述：将一个virtual server ID恢复正常
     * @param storageIndex
     * @param id
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public boolean normalVirtualID(int storageIndex, String id) {
        return virtualServerID.validVirtualId(storageIndex, id);
    }

    /** 概述：删除一个virtual server ID
     * @param storageIndex
     * @param id
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public boolean deleteVirtualID(int storageIndex, String id) {
        return virtualServerID.deleteVirtualId(storageIndex, id);
    }

    /** 概述：列出某个SN所有正常的virtual server ID
     * @param storageIndex
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public List<String> listNormalVirtualID(int storageIndex) {
        return virtualServerID.listValidVirtualIds(storageIndex);
    }

    /** 概述：列出某个SN所有的无效的virtual server ID
     * @param storageIndex
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public List<String> listInvalidVirtualID(int storageIndex) {
        return virtualServerID.listInvalidVirtualIds(storageIndex);
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
        return virtualServerID.getVirtualIdContainerPath();
    }

    /** 概述：将某个服务注册到virtual server中
     * @param storageIndex
     * @param virtualID
     * @param firstID
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public void registerFirstID(int storageIndex, String virtualID, String firstId) {
        virtualServerID.addFirstId(storageIndex, virtualID, firstId);
    }

}
