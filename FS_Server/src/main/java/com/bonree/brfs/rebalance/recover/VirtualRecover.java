package com.bonree.brfs.rebalance.recover;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.bonree.brfs.common.utils.FileUtils;
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

    private final String snDataDir;

    private ServerIDManager idManager;

    private final String taskNode;

    private final String selfNode;

    private BalanceTaskSummary balanceSummary;

    private CuratorNodeCache nodeCache;

    private final CuratorClient client;

    private final long delayTime;

    private boolean overFlag = false;

    private boolean interrupt = false;

    private TaskDetail detail;

    private int currentCount = 0;

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

    public VirtualRecover(BalanceTaskSummary balanceSummary, String taskNode, CuratorClient client, ServerIDManager idManager, String snDataDir) {
        this.balanceSummary = balanceSummary;
        this.taskNode = taskNode;
        this.client = client;
        this.idManager = idManager;
        this.snDataDir = snDataDir;
        // 恢复需要对节点进行监听
        nodeCache = CuratorCacheFactory.getNodeCache();
        nodeCache.addListener(taskNode, new RecoverListener("recover"));
        this.selfNode = taskNode + Constants.SEPARATOR + this.idManager.getFirstServerID();
        this.delayTime = balanceSummary.getDelayTime();
    }

    @Override
    public void recover() {
        try {
            for (int i = 0; i < delayTime; i++) {
                // 已注册任务，则直接退出
                if (client.checkExists(selfNode)) {
                    break;
                }
                System.out.println("remain time:" + (delayTime - i) + "s, start task!!!");
                Thread.sleep(1000);
            }
        } catch (InterruptedException e) {
            LOG.error("task back time count interrupt!!", e);
        }

        int timeFileCounts = 0;

        // List<String> replicasPaths = FileUtils.listFilePaths(snDataDir);
        // for (String replicasPath : replicasPaths) {
        // timeFileCounts += FileUtils.listFilePaths(replicasPath).size();
        // }

        Thread cosumerThread = new Thread(consumerQueue());
        cosumerThread.start();

        detail = new TaskDetail(idManager.getFirstServerID(), ExecutionStatus.INIT, timeFileCounts, 0, 0);
        // 注册节点
        System.out.println("create:" + selfNode + "-------------" + detail);

        // 无注册的话，则注册，否则不用注册
        registerNode(selfNode, detail);

        detail.setStatus(ExecutionStatus.RECOVER);
        System.out.println("update:" + selfNode + "-------------" + detail);
        updateDetail(selfNode, detail);

        String storageName = getStorageNameCache(balanceSummary.getStorageIndex()).getStorageName();
        String remoteSecondId = balanceSummary.getInputServers().get(0);
        String remoteFirstID = idManager.getOtherFirstID(remoteSecondId, balanceSummary.getStorageIndex());
        String virtualID = balanceSummary.getServerId();
        LOG.info("balance virtual serverId:" + virtualID);
        // for (String replicasPath : replicasPaths) { // 副本数
        // List<String> timeFileNames = FileUtils.listFileNames(replicasPath);
        // for (String timeFileName : timeFileNames) {// 时间文件
        // String timePath = replicasPath + FileUtils.FILE_SEPARATOR + timeFileName;
        // List<String> fileNames = FileUtils.listFileNames(timePath);
        // for (String fileName : fileNames) {
        // int replicaPot = 0;
        // String[] metaArr = fileName.split(NAME_SEPARATOR);
        // List<String> fileServerIds = new ArrayList<>();
        // for (int j = 1; j < metaArr.length; j++) {
        // fileServerIds.add(metaArr[j]);
        // }
        // if (fileServerIds.contains(virtualID)) {
        // replicaPot = fileServerIds.indexOf(virtualID);
        // FileRecoverMeta fileMeta = new FileRecoverMeta(fileName, storageName, timeFileName, replicaPot, remoteFirstID);
        // try {
        // fileRecoverQueue.put(fileMeta);
        // } catch (InterruptedException e) {
        // LOG.error("put file: " + fileMeta, e);
        // }
        // }
        // }
        // }
        // }

        // 模拟发送文件
        for (int i = 0; i < 3000; i++) {
            currentCount += 1;
            if (TaskStatus.CANCEL.equals(status.get())) {
                interrupt = true;
                break;
            }
            System.out.println("file:" + currentCount);
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            detail.setCurentCount(currentCount);
            detail.setProcess(detail.getCurentCount() / (double) detail.getTotalDirectories());
            updateDetail(selfNode, detail);
            System.out.println("update:" + selfNode + "-------------" + detail);
            try {
                Thread.sleep(10);
            } catch (InterruptedException e1) {
                e1.printStackTrace();
            }
        }

        // 所有的文件已经处理完毕，等待队列为空
        overFlag = true;

        try {
            cosumerThread.join();
        } catch (InterruptedException e) {
            LOG.error("cosumerThread error!", e);
        }

        if (!interrupt) {
            detail.setStatus(ExecutionStatus.FINISH);
            System.out.println("update:" + selfNode + "-------------" + detail);
            updateDetail(selfNode, detail);
            System.out.println("virtual server id:" + virtualID + " transference over!!!");
        }

        try {
            nodeCache.cancelListener(taskNode);
        } catch (IOException e) {
            LOG.error("cancel listener failed!!", e);
        }
        System.out.println("over!!!!!!!!!!!!!!!!!!!!!");
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
                        if (TaskStatus.CANCEL.equals(status.get())) {
                            interrupt = true;
                            break;
                        }
                        fileRecover = fileRecoverQueue.poll(100, TimeUnit.MILLISECONDS);
                        if (fileRecover != null) {
                            System.out.println("transfer :" + fileRecover);// 此处来发送文件
                            currentCount += 1;
                            detail.setCurentCount(currentCount);
                            detail.setProcess(detail.getCurentCount() / (double) detail.getTotalDirectories());
                            updateDetail(selfNode, detail);
                            System.out.println("update:" + selfNode + "-------------" + detail);
                        }

                    }
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

}
