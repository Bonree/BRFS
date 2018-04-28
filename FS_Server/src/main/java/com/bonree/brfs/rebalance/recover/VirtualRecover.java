package com.bonree.brfs.rebalance.recover;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.curator.shaded.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.common.zookeeper.curator.cache.AbstractNodeCacheListener;
import com.bonree.brfs.common.zookeeper.curator.cache.CuratorCacheFactory;
import com.bonree.brfs.common.zookeeper.curator.cache.CuratorNodeCache;
import com.bonree.brfs.rebalance.Constants;
import com.bonree.brfs.rebalance.DataRecover;
import com.bonree.brfs.rebalance.task.BalanceTaskSummary;
import com.bonree.brfs.rebalance.task.TaskDetail;
import com.bonree.brfs.rebalance.task.TaskStatus;
import com.bonree.brfs.server.StorageName;
import com.bonree.brfs.server.identification.ServerIDManager;

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

    private ServerIDManager idManager;

    private final String taskNode;

    private BalanceTaskSummary balanceSummary;

    private CuratorNodeCache nodeCache;

    private final CuratorClient client;

    private boolean overFlag = false;

    private AtomicReference<TaskStatus> status = new AtomicReference<TaskStatus>(TaskStatus.INIT);

    private final BlockingQueue<FileRecoverMeta> fileRecoverQueue = new ArrayBlockingQueue<>(2000);

    private class RecoverListener extends AbstractNodeCacheListener {

        public RecoverListener(String listenName) {
            super(listenName);
        }

        @Override
        public void nodeChanged() throws Exception {
            System.out.println("node change!!!");
            byte[] data = client.getData(taskNode);
            BalanceTaskSummary bts = JSON.parseObject(data, BalanceTaskSummary.class);
            TaskStatus stats = bts.getTaskStatus();
            // 更新缓存
            status.set(stats);
            System.out.println("stats:" + stats);
        }

    }

    public VirtualRecover(BalanceTaskSummary balanceSummary, String taskNode, CuratorClient client, ServerIDManager idManager) {
        this.balanceSummary = balanceSummary;
        this.taskNode = taskNode;
        this.client = client;
        this.idManager = idManager;

        // 恢复需要对节点进行监听
        nodeCache = CuratorCacheFactory.getNodeCache();
        nodeCache.addListener(taskNode, new RecoverListener("recover"));

    }

    @Override
    public void recover() {

        boolean interrupt = false;
//        new Thread(consumerQueue()).start();

        TaskDetail detail = new TaskDetail(idManager.getFirstServerID(), ExecutionStatus.INIT, 3000, 0, 0);
        // 注册节点
        String selfNode = taskNode + Constants.SEPARATOR + idManager.getFirstServerID();
        System.out.println("create:" + selfNode + "-------------" + detail);
        registerNode(selfNode, detail);

        detail.setStatus(ExecutionStatus.RECOVER);
        System.out.println("update:" + selfNode + "-------------" + detail);
        updateDetail(selfNode, detail);

        List<String> files = getFiles();
        String storageName = getStorageNameCache(balanceSummary.getStorageIndex()).getStorageName();
        String remoteSecondId = balanceSummary.getInputServers().get(0);
        String remoteFirstID = idManager.getOtherFirstID(remoteSecondId, balanceSummary.getStorageIndex());
        String virtualID = balanceSummary.getServerId();

        LOG.info("balance virtual serverId:" + virtualID);
        for (String fileName : files) {
            int replicaPot = 0;
            String[] metaArr = fileName.split(NAME_SEPARATOR);
            List<String> fileServerIds = new ArrayList<>();
            for (int j = 1; j < metaArr.length; j++) {
                fileServerIds.add(metaArr[j]);
            }
            if (fileServerIds.contains(virtualID)) {
                replicaPot = fileServerIds.indexOf(virtualID);
                FileRecoverMeta fileMeta = new FileRecoverMeta(fileName, storageName, "", replicaPot, remoteFirstID);
                try {
                    fileRecoverQueue.put(fileMeta);
                } catch (InterruptedException e) {
                    LOG.error("put file: " + fileMeta, e);
                }
                // if (!isExistFile(remoteServerId, fileName)) {
                // remoteCopyFile(remoteServerId, fileName, replicaPot);
                // }
            }
        }

        for (int i = 0; i < 3000; i++) {
            if (TaskStatus.CANCEL.equals(status.get())) {
                interrupt = true;
                break;
            }
            System.out.println("file:" + i);
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            detail.setCurentCount(i + 1);
            detail.setProcess(detail.getCurentCount() / (double) detail.getTotalDirectories());
            updateDetail(selfNode, detail);
            System.out.println("update:" + selfNode + "-------------" + detail);
        }

        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (!interrupt) {
            detail.setStatus(ExecutionStatus.FINISH);
            System.out.println("update:" + selfNode + "-------------" + detail);
            updateDetail(selfNode, detail);
            System.out.println("virtual server id:" + virtualID + " transference over!!!");

            overFlag = true;
        }
        try {
            nodeCache.cancelListener(taskNode);
        } catch (IOException e) {
            LOG.error("cancel listener failed!!", e);
        }
        System.out.println("over!!!!!!!!!!!!!!!!!!!!!");
    }

    public List<String> getFiles() {
        return Lists.newArrayList();
    }

    public boolean isExistFile(String remoteServerId, String fileName) {
        return false;
    }

    private Runnable consumerQueue() {
        return new Runnable() {

            @Override
            public void run() {
                try {
                    FileRecoverMeta fileRecover = null;

                    while (fileRecover != null || !overFlag) {
                        fileRecover = fileRecoverQueue.poll(100, TimeUnit.MILLISECONDS);
                    }

                    System.out.println("transfer :" + fileRecover);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };
    }

    public StorageName getStorageNameCache(int storageIndex) {
        return new StorageName();
    }

    public void remoteCopyFile(String remoteServerId, String fileName, int replicaPot) {
        System.out.println("remove file:" + remoteServerId + "--" + fileName + "--" + replicaPot);
    }

    /** 概述：更新任务信息
     * @param node
     * @param status
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public void updateDetail(String node, TaskDetail detail) {
        if (client.checkExists(node)) {
            try {
                client.setData(node, JSON.toJSONString(detail).getBytes());
            } catch (Exception e) {
                LOG.error("change Task status error!", e);
            }
        }
    }

    /** 概述：注册节点
     * @param node
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public void registerNode(String node, TaskDetail detail) {
        if (!client.checkExists(node)) {
            client.createPersistent(node, false, JSON.toJSONString(detail).getBytes());
        }
    }

    public static void main(String[] args) {
        CuratorCacheFactory.init("192.168.101.86:2181");
        BalanceTaskSummary ts=JSON.parseObject("{\"changeID\":\"15248232460eacd758-ea86-49d3-994a-10a8639b6c45\",\"delayTime\":10,\"inputServers\":[\"21\"],\"outputServers\":[\"20\"],\"serverId\":\"30\",\"storageIndex\":1,\"taskStatus\":\"CANCEL\",\"taskType\":\"VIRTUAL\"}",BalanceTaskSummary.class);
        VirtualRecover v =new VirtualRecover(ts, "/brfs/wz_test1/rebalance/tasks/1/task", null, null);
        v.recover();
    }

}
