package com.bonree.brfs.server.identification.impl;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private static final Logger LOG = LoggerFactory.getLogger(SecondLevelServerID.class);
    private LevelServerIDGen secondServerIDOpt;

    private CuratorClient client;

    private String selfFirstPath;

    private String baseRoutes;

    private Map<Integer, String> secondMap;

    private final static String SECOND_LOCKS = "second_locks";

    private final static String SEPARATOR = "/";

    // private static Lock lock = new ReentrantLock();
    private InterProcessLock lock;

    public SecondLevelServerID(CuratorClient client, String selfFirstPath, String seqPath, String baseRoutes) {
        this.client = client;
        this.selfFirstPath = selfFirstPath;
        this.secondServerIDOpt = new SecondServerIDGenImpl(client, seqPath);
        this.baseRoutes = baseRoutes;
        secondMap = new ConcurrentHashMap<>();
        lock = new InterProcessMutex(client.getInnerClient(), seqPath + SEPARATOR + SECOND_LOCKS);
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
        if (StringUtils.isEmpty(serverID)) {
            try {
                if (lock.acquire(10, TimeUnit.SECONDS)) {
                    serverID = secondMap.get(storageIndex);
                    if (StringUtils.isEmpty(serverID)) {
                        String node = selfFirstPath + '/' + storageIndex;
                        if (!client.checkExists(node)) {
                            serverID = secondServerIDOpt.genLevelID();
                            client.createPersistent(node, true, serverID.getBytes());
                        } else {
                            serverID = new String(client.getData(node));
                        }
                        secondMap.put(storageIndex, serverID);
                    }
                }
            } catch (Exception e) {
                LOG.error("acquire lock error!!!", e);
            } finally {
                try {
                    lock.release();
                } catch (Exception e) {
                    LOG.error("release lock error!!!", e);
                }
            }
        }
        return serverID;
    }

}
