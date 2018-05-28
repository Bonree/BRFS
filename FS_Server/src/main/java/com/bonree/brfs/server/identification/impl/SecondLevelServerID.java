package com.bonree.brfs.server.identification.impl;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.lang3.StringUtils;

import com.bonree.brfs.common.rebalance.Constants;
import com.bonree.brfs.common.rebalance.route.NormalRoute;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.server.identification.LevelServerIDGen;
import com.google.common.base.Preconditions;

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
    private LevelServerIDGen secondServerIDOpt;

    private CuratorClient client;

    private String selfFirstPath;

    private String baseRoutes;

    private Map<Integer, String> secondMap;

    private static Lock lock = new ReentrantLock();

    public SecondLevelServerID(CuratorClient client, String selfFirstPath, String seqPath, String baseRoutes) {
        this.client = client;
        this.selfFirstPath = selfFirstPath;
        this.secondServerIDOpt = new SecondServerIDGenImpl(client, seqPath);
        this.baseRoutes = baseRoutes;
        secondMap = new ConcurrentHashMap<>();
    }

    public void loadServerID() {
        List<String> storageIndeies = client.getChildren(selfFirstPath);
        // 此处需要进行判断是否过期
        for (String si : storageIndeies) {
            String node = selfFirstPath + '/' + si;
            String serverID = new String(client.getData(node));
            if (isExpire(client, si, serverID)) { // 判断secondServerID是否过期，过期需要重新生成
                serverID = secondServerIDOpt.genLevelID();
                client.setData(node, serverID.getBytes()); // 覆盖以前的second server ID
            }
            secondMap.put(BrStringUtils.parseNumber(si, Integer.class), serverID);
        }

    }

    private boolean isExpire(CuratorClient client, String si, String secondServerID) {
        String normalPath = baseRoutes + Constants.SEPARATOR + Constants.NORMAL_ROUTE;
        String siPath = normalPath + Constants.SEPARATOR + si;
        if (client.checkExists(normalPath) && client.checkExists(siPath)) {
            List<String> routeNodes = client.getChildren(siPath);
            for (String routeNode : routeNodes) {
                String routePath = siPath + Constants.SEPARATOR + routeNode;
                byte[] data = client.getData(routePath);
                NormalRoute normalRoute = JsonUtils.toObject(data, NormalRoute.class);
                if (normalRoute.getSecondID().equals(secondServerID)) {
                    return true;
                }
            }
        }
        return false;
    }

    /** 概述：若缓存里没有，则为新建的SN，需要重新为该SN初始化second server ID
     * @param storageIndex
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public String getServerID(int storageIndex) {
        Preconditions.checkNotNull(secondMap, "Second Level Server ID is not init!!!");
        String serverID = secondMap.get(storageIndex);
        if (StringUtils.isEmpty(serverID)) { // 需要对新的SN的进行初始化
            try {
                String node = selfFirstPath + '/' + storageIndex;
                lock.lock();
                if (!client.checkExists(node)) {
                    serverID = secondServerIDOpt.genLevelID();
                    client.createPersistent(node, true, serverID.getBytes());
                } else {
                    serverID = new String(client.getData(node));
                }
                secondMap.put(storageIndex, serverID);
            } finally {
                lock.unlock();
            }
        }
        return serverID;
    }

}
