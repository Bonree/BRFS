package com.bonree.brfs.rebalanceV2.recover;

import com.bonree.brfs.common.rebalance.Constants;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.utils.BRFSFileUtil;
import com.bonree.brfs.common.utils.BRFSPath;
import com.bonree.brfs.common.utils.FileUtils;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.common.zookeeper.curator.cache.AbstractNodeCacheListener;
import com.bonree.brfs.common.zookeeper.curator.cache.CuratorCacheFactory;
import com.bonree.brfs.common.zookeeper.curator.cache.CuratorNodeCache;
import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.CommonConfigs;
import com.bonree.brfs.identification.IDSManager;
import com.bonree.brfs.identification.LocalPartitionInterface;
import com.bonree.brfs.partition.model.LocalPartitionInfo;
import com.bonree.brfs.rebalance.DataRecover;
import com.bonree.brfs.rebalance.task.TaskDetail;
import com.bonree.brfs.rebalance.task.TaskStatus;
import com.bonree.brfs.rebalanceV2.task.BalanceTaskSummaryV2;
import com.bonree.brfs.rebalanceV2.transfer.SimpleFileClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @date 2018年3月23日 下午2:16:13
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description: 恢复虚拟ServerID的
 ******************************************************************************/
public class VirtualRecoverV2 implements DataRecover {

    private final static Logger LOG = LoggerFactory.getLogger(VirtualRecoverV2.class);

    private static final String NAME_SEPARATOR = "_";

    private final String storageName;
    private IDSManager idManager;

    private final String taskNode;
    private final String selfNode;

    private BalanceTaskSummaryV2 balanceSummary;
    private CuratorNodeCache nodeCache;
    private SimpleFileClient fileClient;
    private final CuratorClient client;
    private final long delayTime;
    private final ServiceManager serviceManager;
    private boolean overFlag = false;
    private TaskDetail detail;
    private int currentCount = 0;
    private LocalPartitionInterface localPartitionInterface;
    private AtomicReference<TaskStatus> status;

    private final BlockingQueue<FileRecoverMetaV2> fileRecoverQueue = new ArrayBlockingQueue<>(2000);

    public VirtualRecoverV2(CuratorClient client, BalanceTaskSummaryV2 balanceSummary, String taskNode, String storageName, IDSManager idManager, ServiceManager serviceManager, LocalPartitionInterface localPartitionInterface) {
        this.balanceSummary = balanceSummary;
        this.taskNode = taskNode;
        this.client = client;
        this.idManager = idManager;
        this.serviceManager = serviceManager;
        this.storageName = storageName;
        this.localPartitionInterface = localPartitionInterface;
        this.fileClient = new SimpleFileClient();
        // 恢复需要对节点进行监听
        nodeCache = CuratorCacheFactory.getNodeCache();
        nodeCache.addListener(taskNode, new RecoverListener("recover_listener"));
        this.selfNode = taskNode + Constants.SEPARATOR + this.idManager.getFirstSever();
        this.delayTime = balanceSummary.getDelayTime();
        status = new AtomicReference<>(balanceSummary.getTaskStatus());
    }

    private class RecoverListener extends AbstractNodeCacheListener {

        public RecoverListener(String listenName) {
            super(listenName);
        }

        @Override
        public void nodeChanged() throws Exception {
            LOG.info("node change!!!");
            if (client.checkExists(taskNode)) {
                byte[] data = client.getData(taskNode);
                BalanceTaskSummaryV2 bts = JsonUtils.toObject(data, BalanceTaskSummaryV2.class);
                String newID = bts.getId();
                String oldID = balanceSummary.getId();
                if (newID.equals(oldID)) { // 是同一个任务
                    TaskStatus stats = bts.getTaskStatus();
                    // 更新缓存
                    status.set(stats);
                    LOG.info("stats:" + stats);
                } else { // 不是同一个任务
                    LOG.info("newID:{} not match oldID:{}", newID, oldID);
                    LOG.info("cancel multi recover:{}", balanceSummary);
                    status.set(TaskStatus.CANCEL);
                }
            } else {
                LOG.info("task is deleted!!,this task will cancel!");
                status.set(TaskStatus.CANCEL);
            }
        }

    }

    @Override
    public void recover() {
        LOG.info("begin virtual recover");
        // 注册节点
        LOG.info("create:" + selfNode + "-------------" + detail);
        // 无注册的话，则注册，否则不用注册
        while (true) {
            detail = registerNodeDetail(selfNode);
            if (detail != null) {
                LOG.info("register " + selfNode + " is successful!!");
                break;
            }
            LOG.error("register " + selfNode + " is error!!");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        // 主任务结束，则直接退出
        if (balanceSummary.getTaskStatus().equals(TaskStatus.FINISH)) {
            finishTask();
            return;
        }

        try {
            for (int i = 0; i < delayTime; i++) {
                if (status.get().equals(TaskStatus.CANCEL)) {
                    return;
                }
                // 倒计时完毕，则不需要倒计时
                if (!detail.getStatus().equals(ExecutionStatus.INIT)) {
                    break;
                }
                if (delayTime - i <= 10) {
                    LOG.info("remain time:" + (delayTime - i) + "s, start task!!!");
                } else {
                    if ((delayTime - i) % 10 == 0) {
                        LOG.info("remain time:" + (delayTime - i) + "s, start task!!!");
                    }
                }
                Thread.sleep(1000);
            }
        } catch (InterruptedException e) {
            LOG.error("task back time count interrupt!!", e);
        }

        detail.setStatus(ExecutionStatus.RECOVER);
        LOG.info("update:" + selfNode + "-------------" + detail);
        updateDetail(selfNode, detail);

        int timeFileCounts = 0;
        Collection<LocalPartitionInfo> localPartitionInfos = this.localPartitionInterface.getPartitions();

        for (LocalPartitionInfo partitionInfo : localPartitionInfos) {
            String partitionPath = partitionInfo.getDataDir();
            String snDataDir = partitionPath + FileUtils.FILE_SEPARATOR + storageName;

            if (!FileUtils.isExist(snDataDir)) {
                finishTask();
                return;
            }

            List<String> replicasNames = FileUtils.listFileNames(snDataDir);
            for (String replicasName : replicasNames) {
                String replicasPath = snDataDir + FileUtils.FILE_SEPARATOR + replicasName;
                timeFileCounts += FileUtils.listFileNames(replicasPath).size();
            }

            Thread cosumerThread = new Thread(consumerQueue());
            cosumerThread.start();

            detail.setTotalDirectories(timeFileCounts);
            updateDetail(selfNode, detail);

            String remoteSecondId = balanceSummary.getInputServers().get(0);
            String remoteFirstID = idManager.getFirstId(remoteSecondId, balanceSummary.getStorageIndex());
            String virtualID = balanceSummary.getServerId();

            LOG.info("balance virtual serverId:" + virtualID);
            List<BRFSPath> allPaths = BRFSFileUtil.scanFile(partitionPath, storageName);
            for (BRFSPath brfsPath : allPaths) {
                if (status.get().equals(TaskStatus.CANCEL)) {
                    break;
                }
                String perFile = partitionPath + FileUtils.FILE_SEPARATOR + brfsPath.toString();
                if (!perFile.endsWith(".rd")) {
                    String timeFileName = brfsPath.getYear() + FileUtils.FILE_SEPARATOR + brfsPath
                            .getMonth() + FileUtils.FILE_SEPARATOR + brfsPath.getDay() + FileUtils.FILE_SEPARATOR + brfsPath
                            .getHourMinSecond();
                    String fileName = brfsPath.getFileName();
                    int replicaPot = 0;
                    String[] metaArr = fileName.split(NAME_SEPARATOR);
                    List<String> fileServerIds = new ArrayList<>();
                    for (int j = 1; j < metaArr.length; j++) {
                        fileServerIds.add(metaArr[j]);
                    }
                    if (fileServerIds.contains(virtualID)) {
                        // 此处位置需要加1，副本数从1开始
                        replicaPot = fileServerIds.indexOf(virtualID) + 1;
                        FileRecoverMetaV2 fileMeta = new FileRecoverMetaV2(perFile, fileName, storageName, timeFileName, Integer
                                .parseInt(brfsPath.getIndex()), replicaPot, remoteFirstID, partitionPath);
                        try {
                            fileRecoverQueue.put(fileMeta);
                        } catch (InterruptedException e) {
                            LOG.error("put file: " + fileMeta, e);
                        }
                    }
                }
            }

            // 所有的文件已经处理完毕，等待队列为空
            overFlag = true;
            try {
                cosumerThread.join();
            } catch (InterruptedException e) {
                LOG.error("cosumerThread error!", e);
            }

            finishTask();
        }
    }

    public void finishTask() {
        // 没有取消任务
        if (!status.get().equals(TaskStatus.CANCEL)) {
            detail.setStatus(ExecutionStatus.FINISH);
            LOG.info("update:" + selfNode + "-------------" + detail);
            updateDetail(selfNode, detail);
            LOG.info("virtual server id:" + balanceSummary.getServerId() + " transference over!!!");
            LOG.info("虚拟恢复正常完成！！！！！");
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
                    FileRecoverMetaV2 fileRecover = null;
                    while (fileRecover != null || !overFlag) {
                        if (status.get().equals(TaskStatus.CANCEL)) {
                            break;
                        }
                        fileRecover = fileRecoverQueue.poll(100, TimeUnit.MILLISECONDS);
                        if (fileRecover != null) {
                            String logicPath = storageName + FileUtils.FILE_SEPARATOR + fileRecover
                                    .getReplica() + FileUtils.FILE_SEPARATOR + fileRecover.getTime();
                            String remoteDir = storageName + FileUtils.FILE_SEPARATOR + fileRecover
                                    .getPot() + FileUtils.FILE_SEPARATOR + fileRecover.getTime();
                            String localFilePath = fileRecover.getPartitionPath() + FileUtils.FILE_SEPARATOR + logicPath + FileUtils.FILE_SEPARATOR + fileRecover
                                    .getFileName();
                            boolean success;
                            LOG.info("transfer: {}", fileRecover);
                            String firstID = fileRecover.getFirstServerID();

                            while (true) {
                                Service service = serviceManager.getServiceById(Configs.getConfiguration()
                                        .GetConfig(CommonConfigs.CONFIG_DATA_SERVICE_GROUP_NAME), firstID);
                                if (service == null) {
                                    LOG.warn("first id is {}, maybe down!", firstID);
                                    Thread.sleep(1000);
                                    continue;
                                }

                                String selectedPartitionId = idManager.getPartitionId(fileRecover.getSelectedSecondId(), balanceSummary.getStorageIndex());
                                String partitionIdRecoverFileName = selectedPartitionId + ":" + fileRecover.getFileName();
                                success = secureCopyTo(service, localFilePath, remoteDir, partitionIdRecoverFileName);
                                if (success) {
                                    break;
                                }
                            }

                            currentCount += 1;
                            detail.setCurentCount(currentCount);
                            detail.setProcess(detail.getCurentCount() / (double) detail.getTotalDirectories());
                            updateDetail(selfNode, detail);
                            LOG.info("update:" + selfNode + "-------------" + detail);
                        }
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };
    }

    public boolean secureCopyTo(Service service, String localPath, String remoteDir, String fileName) {
        boolean success = true;
        try {
            if (!FileUtils.isExist(localPath + ".rd")) {

                fileClient.sendFile(service.getHost(), service.getPort() + 20, localPath, remoteDir, fileName);
            }
        } catch (Exception e) {
            success = false;
            e.printStackTrace();
        }
        return success;
    }

    /**
     * 概述：更新任务信息
     *
     * @param node
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public void updateDetail(String node, TaskDetail detail) {
        if (client.checkExists(node)) {
            try {
                client.setData(node, JsonUtils.toJsonBytes(detail));
            } catch (Exception e) {
                LOG.error("change Task status error!", e);
            }
        }
    }

    /**
     * 概述：注册节点
     *
     * @param node
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public TaskDetail registerNodeDetail(String node) {
        TaskDetail detail = null;
        try {
            if (!client.checkExists(node)) {
                detail = new TaskDetail(idManager.getFirstSever(), ExecutionStatus.INIT, 0, 0, 0);
                client.createPersistent(node, false, JsonUtils.toJsonBytes(detail));
            } else {
                byte[] data = client.getData(node);
                detail = JsonUtils.toObject(data, TaskDetail.class);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return detail;

    }
}
