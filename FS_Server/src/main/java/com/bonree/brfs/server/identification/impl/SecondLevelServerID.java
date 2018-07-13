package com.bonree.brfs.server.identification.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.rebalance.Constants;
import com.bonree.brfs.common.rebalance.route.NormalRoute;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.server.identification.LevelServerIDGen;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年4月11日 下午3:31:55
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description: 二级serverID用于有副本的SN来使用，各个SN的平衡情况会不一致，
 * 所以每个SN都会有自己的二级ServerID。
 ******************************************************************************/
public class SecondLevelServerID {

    private static final Logger LOG = LoggerFactory.getLogger(SecondLevelServerID.class);
    private LevelServerIDGen secondServerIDOpt;

    private CuratorFramework client;

    private String selfFirstPath;

    private String baseRoutes;

    private ConcurrentHashMap<Integer, String> secondMap = new ConcurrentHashMap<Integer, String>();

    public SecondLevelServerID(CuratorFramework client, String selfFirstPath, String seqPath, String baseRoutes) {
        this.client = client;
        this.selfFirstPath = selfFirstPath;
        this.secondServerIDOpt = new SecondServerIDGenImpl(client, seqPath);
        this.baseRoutes = baseRoutes;
    }

    public void loadServerID() {
    	Map<String, String> expiredIds = scanExpiredServerIds();
    	
    	try {
			List<String> storageIdList = client.getChildren().forPath(selfFirstPath);
			
			if(storageIdList != null) {
				for(String storageId : storageIdList) {
					String idNodePath = ZKPaths.makePath(selfFirstPath, storageId);
					
					byte[] bytes = client.getData().forPath(idNodePath);
					if(bytes == null) {
						throw new IllegalStateException("server id node" + storageId + " can not get data");
					}
					
					String serverId = BrStringUtils.fromUtf8Bytes(bytes);
					if(expiredIds.remove(serverId, storageId)) {
						client.delete().forPath(idNodePath);
						continue;
					}
					
					secondMap.put(Integer.parseInt(storageId), BrStringUtils.fromUtf8Bytes(bytes));
				}
			}
		} catch (Exception e) {
			LOG.error("can not get server id list from zk", e);
			
			throw new RuntimeException(e);
		}
    }
    
    private Map<String, String> scanExpiredServerIds() {
    	Map<String, String> expiredIds = new HashMap<String, String>();
    	
    	try {
    		String normalRoutePath = ZKPaths.makePath(baseRoutes, Constants.NORMAL_ROUTE);
    		
			List<String> storageIdList = client.getChildren().forPath(normalRoutePath);
			if(storageIdList != null) {
				for(String storageId : storageIdList) {
					String idNodePath = ZKPaths.makePath(normalRoutePath, storageId);
					
					try {
						List<String> routeList = client.getChildren().forPath(idNodePath);
						if(routeList == null) {
							continue;
						}
						
						for(String route : routeList) {
							try {
								byte[] bytes = client.getData().forPath(ZKPaths.makePath(idNodePath, route));
								if(bytes == null) {
									continue;
								}
								
								NormalRoute routeNode = JsonUtils.toObject(bytes, NormalRoute.class);
								expiredIds.put(routeNode.getSecondID(), storageId);
							} catch (Exception e) {
								LOG.error("get route data of [{}] error", route, e);
							}
						}
					} catch (Exception e) {
						LOG.error("get route list[{}] error", idNodePath, e);
					}
				}
			}
		} catch(NoNodeException e) {
			LOG.info("no normal node!");
		} catch (Exception e) {
			LOG.warn("get expired id list error", e);
		}
    	
    	return expiredIds;
    }

    public String getServerID(int storageIndex) {
        String serverID = secondMap.get(storageIndex);
        if (serverID == null) {
        	String node = ZKPaths.makePath(selfFirstPath, String.valueOf(storageIndex));
        	serverID = secondServerIDOpt.genLevelID();
        	
        	String nodePath = null;
        	try {
				nodePath = client.create()
				.creatingParentContainersIfNeeded()
				.forPath(node, BrStringUtils.toUtf8Bytes(serverID));
			} catch (Exception ignore) {}
        	
        	if(nodePath == null) {
        		serverID = null;
        		try {
					byte[] bytes = client.getData().forPath(node);
					
					if(bytes != null) {
						serverID = BrStringUtils.fromUtf8Bytes(bytes);
					}
				} catch (Exception e) {
					LOG.error("get server id data error", e);
				}
        	}
        	
        	if(serverID == null) {
        		throw new RuntimeException("can not get second server id");
        	}
        	
        	secondMap.put(storageIndex, serverID);
        }
        
        return serverID;
    }

    public boolean deleteServerID(int storageIndex) {
    	String serverId = secondMap.remove(storageIndex);
    	if(serverId == null) {
    		return true;
    	}
    	
    	try {
			client.delete().quietly().forPath(ZKPaths.makePath(selfFirstPath, String.valueOf(storageIndex)));
			
			return true;
		} catch (Exception e) {
			LOG.error("can not delete second server id node", e);
		}
    	
    	return false;
    }

}
