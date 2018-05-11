package com.bonree.brfs.rebalance.recover;

import java.io.FileNotFoundException;
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
import com.bonree.brfs.common.rebalance.Constants;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.utils.FileUtils;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.common.zookeeper.curator.cache.AbstractNodeCacheListener;
import com.bonree.brfs.common.zookeeper.curator.cache.CuratorCacheFactory;
import com.bonree.brfs.common.zookeeper.curator.cache.CuratorNodeCache;
import com.bonree.brfs.rebalance.DataRecover;
import com.bonree.brfs.rebalance.LocalDiskNode;
import com.bonree.brfs.rebalance.record.SimpleRecordWriter;
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

    private final String dataDir;

    private final String storageName;

    private ServerIDManager idManager;

    private final String taskNode;

    private final String selfNode;

    private BalanceTaskSummary balanceSummary;

    private CuratorNodeCache nodeCache;

    private LocalDiskNode diskNode;

    private final CuratorClient client;

    private final long delayTime;

    private final ServiceManager serviceManager;

    private boolean overFlag = false;

    private TaskDetail detail;

    private int currentCount = 0;

    private final BlockingQueue<FileRecoverMeta> fileRecoverQueue = new ArrayBlockingQueue<>(2000);

    private AtomicReference<TaskStatus> status = null;

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

    public VirtualRecover(CuratorClient client, BalanceTaskSummary balanceSummary, String taskNode, String dataDir, String storageName, ServerIDManager idManager, ServiceManager serviceManager) {
        this.balanceSummary = balanceSummary;
        this.taskNode = taskNode;
        this.client = client;
        this.idManager = idManager;
        this.serviceManager = serviceManager;
        this.dataDir = dataDir;
        this.storageName = storageName;
        diskNode = new LocalDiskNode(dataDir);
        // 恢复需要对节点进行监听
        nodeCache = CuratorCacheFactory.getNodeCache();
        nodeCache.addListener(taskNode, new RecoverListener("recover"));
        this.selfNode = taskNode + Constants.SEPARATOR + this.idManager.getFirstServerID();
        this.delayTime = balanceSummary.getDelayTime();
        status = new AtomicReference<TaskStatus>(balanceSummary.getTaskStatus());
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

        LOG.info("begin virtual recover");

        int timeFileCounts = 0;
        String snDataDir = dataDir + FileUtils.FILE_SEPARATOR + storageName;
        List<String> replicasPaths = FileUtils.listFilePaths(snDataDir);
        for (String replicasPath : replicasPaths) {
            timeFileCounts += FileUtils.listFilePaths(replicasPath).size();
        }

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

        String remoteSecondId = balanceSummary.getInputServers().get(0);
        String remoteFirstID = idManager.getOtherFirstID(remoteSecondId, balanceSummary.getStorageIndex());
        String virtualID = balanceSummary.getServerId();
        LOG.info("balance virtual serverId:" + virtualID);
        QUIT: for (String replicasPath : replicasPaths) { // 副本数
            List<String> timeFileNames = FileUtils.listFileNames(replicasPath);
            for (String timeFileName : timeFileNames) {// 时间文件
                SimpleRecordWriter simpleWriter = null;
                try {
                    simpleWriter = new SimpleRecordWriter("");
                    String timePath = replicasPath + FileUtils.FILE_SEPARATOR + timeFileName;
                    List<String> fileNames = FileUtils.listFileNames(timePath);
                    for (String fileName : fileNames) {

                        if (status.get().equals(TaskStatus.CANCEL)) {
                            break QUIT;
                        }

                        int replicaPot = 0;
                        String[] metaArr = fileName.split(NAME_SEPARATOR);
                        List<String> fileServerIds = new ArrayList<>();
                        for (int j = 1; j < metaArr.length; j++) {
                            fileServerIds.add(metaArr[j]);
                        }
                        if (fileServerIds.contains(virtualID)) {
                            replicaPot = fileServerIds.indexOf(virtualID);
                            FileRecoverMeta fileMeta = new FileRecoverMeta(fileName, storageName, timeFileName, replicaPot, remoteFirstID, simpleWriter);
                            try {
                                fileRecoverQueue.put(fileMeta);
                            } catch (InterruptedException e) {
                                LOG.error("put file: " + fileMeta, e);
                            }
                        }
                    }
                } catch (FileNotFoundException e1) {
                    e1.printStackTrace();
                } finally {
                    if (simpleWriter != null) {
                        try {
                            simpleWriter.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }

        // 模拟发送文件
        for (int i = 0; i < 3000; i++) {
            currentCount += 1;
            if (status.get().equals(TaskStatus.CANCEL)) {
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
        // 没有中断
        if (status.get().equals(TaskStatus.RUNNING)) {
            detail.setStatus(ExecutionStatus.FINISH);
            System.out.println("update:" + selfNode + "-------------" + detail);
            updateDetail(selfNode, detail);
            System.out.println("virtual server id:" + virtualID + " transference over!!!");
            System.out.println("虚拟恢复正常完成！！！！！");
        }

        try {
            nodeCache.cancelListener(taskNode);
        } catch (IOException e) {
            LOG.error("cancel listener failed!!", e);
        }
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
                        if (status.get().equals(TaskStatus.CANCEL)) {
                            break;
                        }
                        fileRecover = fileRecoverQueue.poll(100, TimeUnit.MILLISECONDS);
                        if (fileRecover != null) {
                            System.out.println("transfer :" + fileRecover);// 此处来发送文件
                            String firstID = fileRecover.getFirstServerID();
                            Service service = serviceManager.getServiceById(Constants.DISCOVER, firstID);
                            String logicPath = fileRecover.getStorageName() + FileUtils.FILE_SEPARATOR + fileRecover.getPot() + FileUtils.FILE_SEPARATOR + fileRecover.getTime() + FileUtils.FILE_SEPARATOR + fileRecover.getFileName();
                            if (!diskNode.isExistFile(service.getHost(), service.getPort(), logicPath)) {
                                while (!sucureCopyTo(service, logicPath)) {
                                    Thread.sleep(1000);
                                }
                            }
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

    public boolean sucureCopyTo(Service service, String logicPath) {
        boolean success = true;
        try {
            diskNode.copyTo(service.getHost(), service.getPort(), logicPath, logicPath);
        } catch (IOException e) {
            success = false;
            e.printStackTrace();
        }
        return success;
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
