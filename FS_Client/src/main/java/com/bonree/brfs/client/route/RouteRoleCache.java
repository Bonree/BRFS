package com.bonree.brfs.client.route;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.rebalance.Constants;
import com.bonree.brfs.common.rebalance.route.NormalRoute;
import com.bonree.brfs.common.rebalance.route.VirtualRoute;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.common.utils.JsonUtils.JsonException;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年5月7日 下午5:07:17
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description: 
 ******************************************************************************/
public class RouteRoleCache {
	private static final Logger LOG = LoggerFactory.getLogger(RouteRoleCache.class);

	private CuratorFramework zkClient;
    private String baseRoutePath;

    private int storageIndex;

    private Map<String, NormalRoute> normalRouteDetail;

    private Map<String, VirtualRoute> virtualRouteDetail;

    public RouteRoleCache(CuratorFramework curatorClient, int storageIndex, String baseRoutePath) throws JsonException {
        this.zkClient = curatorClient;
        this.storageIndex = storageIndex;
        this.baseRoutePath = baseRoutePath;
        virtualRouteDetail = new ConcurrentHashMap<>();
        normalRouteDetail = new ConcurrentHashMap<>();
        loadRouteRole();
    }

    private void loadRouteRole() throws JsonException {
            // load virtual id
            try {
            	String virtualPath = ZKPaths.makePath(baseRoutePath, Constants.VIRTUAL_ROUTE, String.valueOf(storageIndex));
            	List<String> virtualNodes = zkClient.getChildren().forPath(virtualPath);
            	
            	if (virtualNodes != null && !virtualNodes.isEmpty()) {
                    for (String virtualNode : virtualNodes) {
                    	try {
                    		byte[] data = zkClient.getData().forPath(ZKPaths.makePath(virtualPath, virtualNode));
                    		VirtualRoute virtual = JsonUtils.toObject(data, VirtualRoute.class);
                            virtualRouteDetail.put(virtual.getVirtualID(), virtual);
						} catch (Exception e) {
							LOG.error("load virtual route[{}] error", virtualNode, e);
						}
                    }
                }
			} catch (Exception e) {
				LOG.error("get virtual nodes error", e);
			}

            // load normal id
            try {
            	String normalPath = ZKPaths.makePath(baseRoutePath, Constants.NORMAL_ROUTE, String.valueOf(storageIndex));
            	List<String> normalNodes = zkClient.getChildren().forPath(normalPath);
            	
            	if (normalNodes != null && !normalNodes.isEmpty()) {
                    for (String normalNode : normalNodes) {
                    	try {
                    		byte[] data = zkClient.getData().forPath(ZKPaths.makePath(normalPath, normalNode));
                    		NormalRoute normal = JsonUtils.toObject(data, NormalRoute.class);
                            normalRouteDetail.put(normal.getSecondID(), normal);
						} catch (Exception e) {
							LOG.error("load normal route[{}] error", normalNode, e);
						}
                    }
                }
			} catch (Exception e) {
				LOG.error("get normal nodes error", e);
			}
    }

    public int getStorageIndex() {
        return storageIndex;
    }

    public Map<String, NormalRoute> getNormalRouteCache() {
        return normalRouteDetail;
    }

    public Map<String, VirtualRoute> getVirtualRouteCache() {
        return virtualRouteDetail;
    }

    public NormalRoute getRouteRole(String secondID) {
        return normalRouteDetail.get(secondID);
    }

    public VirtualRoute getVirtualRoute(String virtualID) {
        return virtualRouteDetail.get(virtualID);
    }
}
