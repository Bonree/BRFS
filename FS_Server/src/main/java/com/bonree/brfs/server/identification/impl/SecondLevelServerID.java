package com.bonree.brfs.server.identification.impl;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
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

    private Map<Integer, String> secondMap = new ConcurrentHashMap<Integer, String>();

    public SecondLevelServerID(CuratorFramework client, String selfFirstPath, String seqPath, String baseRoutes) {
        this.client = client;
        this.selfFirstPath = selfFirstPath;
        this.secondServerIDOpt = new SecondServerIDGenImpl(client, seqPath);
        this.baseRoutes = baseRoutes;
    }

    public void loadServerID() {
    	try {
    		List<String> storageIndeies = client.getChildren().forPath(selfFirstPath);
    		// 此处需要进行判断是否过期
    		for (String si : storageIndeies) {
    			String node = ZKPaths.makePath(selfFirstPath, si);
    			byte[] data = client.getData().forPath(node);
    			String serverID = BrStringUtils.fromUtf8Bytes(data);
    			if (isExpire(si, serverID)) { // 判断secondServerID是否过期，过期需要重新生成
    				serverID = secondServerIDOpt.genLevelID();
    				client.setData().forPath(node, serverID.getBytes(StandardCharsets.UTF_8));
    			}
    			secondMap.put(BrStringUtils.parseNumber(si, Integer.class), serverID);
    		}
    		LOG.info("load self second server ID cache:{}", secondMap);
    	}catch (Exception e) {
    		LOG.error("load self second server ID error!!!",e);
		}
    }
    

    private boolean isExpire(String si, String secondServerID) throws Exception {
        String normalPath = ZKPaths.makePath(baseRoutes, Constants.NORMAL_ROUTE);
        String siPath = ZKPaths.makePath(normalPath, si);
        if (client.checkExists().forPath(normalPath)!=null && client.checkExists().forPath(siPath)!=null) {
            List<String> routeNodes = client.getChildren().forPath(siPath);
            for (String routeNode : routeNodes) {
                String routePath = ZKPaths.makePath(siPath, routeNode);
                byte[] data = client.getData().forPath(routePath);
                NormalRoute normalRoute = JsonUtils.toObject(data, NormalRoute.class);
                if (normalRoute.getSecondID().equals(secondServerID)) {
                    return true;
                }
            }
        }
        return false;
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
