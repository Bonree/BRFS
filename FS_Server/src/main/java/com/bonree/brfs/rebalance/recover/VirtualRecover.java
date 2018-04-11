package com.bonree.brfs.rebalance.recover;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.common.zookeeper.curator.cache.AbstractNodeCacheListener;
import com.bonree.brfs.common.zookeeper.curator.cache.CuratorCacheFactory;
import com.bonree.brfs.common.zookeeper.curator.cache.CuratorNodeCache;
import com.bonree.brfs.rebalance.DataRecover;
import com.bonree.brfs.rebalance.task.BalanceTaskSummary;
import com.bonree.brfs.rebalance.task.TaskOperation;
import com.bonree.brfs.rebalance.task.TaskStatus;
import com.bonree.brfs.server.ServerInfo;
import com.bonree.brfs.server.StorageName;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年3月23日 下午2:16:13
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description: 恢复虚拟ServerID的
 ******************************************************************************/
public class VirtualRecover implements DataRecover {

    private final static Logger LOG = LoggerFactory.getLogger(VirtualRecover.class);

    private static final String NAME_SEPARATOR = "_";

    private StorageName storageName;

    private final String listenerNode;

    private BalanceTaskSummary balanceSummary;
    TaskOperation taskOpt;

    private CuratorNodeCache nodeCache;

    private final CuratorClient client;

    private AtomicReference<TaskStatus> status = new AtomicReference<TaskStatus>(TaskStatus.INIT);

    private class RecoverListener extends AbstractNodeCacheListener {

        public RecoverListener(String listenName) {
            super(listenName);
        }

        @Override
        public void nodeChanged() throws Exception {
            byte[] data = client.getData(listenerNode);
            BalanceTaskSummary bts = JSON.parseObject(data, BalanceTaskSummary.class);
            TaskStatus stats = bts.getTaskStatus();
            // 更新缓存
            status.set(stats);
        }

    }

    public VirtualRecover(BalanceTaskSummary balanceSummary, TaskOperation taskOpt, String listenerNode, CuratorClient client) {
        this.balanceSummary = balanceSummary;
        this.listenerNode = listenerNode;
        this.client = client;
        this.taskOpt = taskOpt;
    }

    @Override
    public void recover() {
        nodeCache = CuratorCacheFactory.getNodeCache();
        nodeCache.addListener(listenerNode, new RecoverListener("recover"));
        nodeCache.startPathCache(listenerNode);
        String selfNode = null; // TODO 拼接本身NODE
        taskOpt.setTaskStatus(selfNode, DataRecover.ExecutionStatus.RECOVER);
        List<String> files = getFiles();
        String remoteServerId = balanceSummary.getInputServers().get(0);
        String fixServerId = balanceSummary.getServerId();
        LOG.info("balance virtual serverId:" + fixServerId);
        for (String fileName : files) {
            int replicaPot = 0;
            String[] metaArr = fileName.split(NAME_SEPARATOR);
            List<String> fileServerIds = new ArrayList<>();
            for (int j = 1; j < metaArr.length; j++) {
                fileServerIds.add(metaArr[j]);
            }
            if (fileServerIds.contains(fixServerId)) {
                replicaPot = fileServerIds.indexOf(fixServerId);
                if (!isExistFile(remoteServerId, fileName)) {
                    remoteCopyFile(remoteServerId, fileName, replicaPot);
                }
            }
        }
        taskOpt.setTaskStatus(selfNode, DataRecover.ExecutionStatus.FINISH);
        try {
            nodeCache.cancelListener(listenerNode);
        } catch (IOException e) {
            LOG.error("cancel listener failed!!", e);
        }
        System.out.println("恢复完成");
    }

    public List<String> getFiles() {
        return new ArrayList<String>();
    }

    public boolean isExistFile(String remoteServerId, String fileName) {
        return true;
    }

    public void remoteCopyFile(String remoteServerId, String fileName, int replicaPot) {

    }

}
