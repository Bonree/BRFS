package com.bonree.brfs.server.identification.impl;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.lang3.StringUtils;

import com.bonree.brfs.common.utils.BrStringUtils;
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

    private String zkHosts;

    private String selfFirstPath;

    private Map<Integer, String> secondMap;

    private static Lock lock = new ReentrantLock();

    public SecondLevelServerID(String zkHosts, String selfFirstPath, String seqPath) {
        this.zkHosts = zkHosts;
        this.selfFirstPath = selfFirstPath;
        this.secondServerIDOpt = new SecondServerIDGenImpl(zkHosts, seqPath);
        secondMap = new ConcurrentHashMap<>();
    }

    public void loadServerID() {
        CuratorClient client = null;
        try {
            client = CuratorClient.getClientInstance(zkHosts);
            List<String> storageIndeies = client.getChildren(selfFirstPath);
            // 此处需要进行判断是否过期
            for (String si : storageIndeies) {
                String node = selfFirstPath + '/' + si;
                String serverID = new String(client.getData(node));
                if (isExpire(si, serverID)) { // 判断secondServerID是否过期，过期需要重新生成
                    serverID = secondServerIDOpt.genLevelID();
                    client.setData(node, serverID.getBytes()); // 覆盖以前的second server ID
                }
                secondMap.put(BrStringUtils.parseNumber(si, Integer.class), serverID);
            }
        } finally {
            if (client != null) {
                client.close();
            }
        }

    }

    private boolean isExpire(String si, String secondServerID) {
        return false;
    }

    /** 概述：若缓存里没有，则为新建的SN，需要重新为该SN初始化second server ID
     * @param storageIndex
     * @return
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public String getServerID(int storageIndex) {
        CuratorClient client = null;
        Preconditions.checkNotNull(secondMap, "Second Level Server ID is not init!!!");
        String serverID = secondMap.get(storageIndex);
        // TODO 多线程调用可能会出现线程安全的问题
        if (StringUtils.isEmpty(serverID)) { // 需要对新的SN的进行初始化
            try {
                client = CuratorClient.getClientInstance(zkHosts);
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
                if (client != null) {
                    client.close();
                }
            }
        }
        return serverID;
    }

}
