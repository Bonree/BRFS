package com.bonree.brfs.rebalance.recover;

import com.bonree.brfs.common.rebalance.Constants;
import com.bonree.brfs.common.resource.vo.LocalPartitionInfo;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.utils.BRFSFileUtil;
import com.bonree.brfs.common.utils.BRFSPath;
import com.bonree.brfs.common.utils.FileUtils;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.common.zookeeper.curator.cache.CuratorCacheFactory;
import com.bonree.brfs.common.zookeeper.curator.cache.CuratorNodeCache;
import com.bonree.brfs.guice.ClusterConfig;
import com.bonree.brfs.identification.IDSManager;
import com.bonree.brfs.identification.LocalPartitionInterface;
import com.bonree.brfs.rebalance.DataRecover;
import com.bonree.brfs.rebalance.task.BalanceTaskSummary;
import com.bonree.brfs.rebalance.task.TaskDetail;
import com.bonree.brfs.rebalance.task.TaskStatus;
import com.bonree.brfs.rebalance.task.listener.TaskNodeCache;
import com.bonree.brfs.rebalance.transfer.SimpleFileClient;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @date 2018年3月23日 下午2:16:13
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description: 恢复虚拟ServerID的
 ******************************************************************************/
public class VirtualRecover implements DataRecover {

    private static final Logger LOG = LoggerFactory.getLogger(VirtualRecover.class);

    private static final String NAME_SEPARATOR = "_";

    private final String storageName;
    private IDSManager idManager;

    private final String taskNode;
    private final String selfNode;

    private BalanceTaskSummary balanceSummary;
    private CuratorNodeCache nodeCache;
    private SimpleFileClient fileClient;
    private CuratorFramework curatorFramework;
    private final long delayTime;
    private final ServiceManager serviceManager;
    private boolean overFlag = false;
    private TaskDetail detail;
    private int currentCount = 0;
    private LocalPartitionInterface localPartitionInterface;
    private AtomicInteger snDirNonExistNum = new AtomicInteger();
    private TaskNodeCache cache;
    private String baseBalancePath;
    private ClusterConfig config;

    private final BlockingQueue<FileRecoverMeta> fileRecoverQueue = new ArrayBlockingQueue<>(2000);

    public VirtualRecover(ClusterConfig config, CuratorFramework curatorFramework, BalanceTaskSummary balanceSummary,
                          String taskNode, String storageName,
                          IDSManager idManager, ServiceManager serviceManager,
                          LocalPartitionInterface localPartitionInterface, String baseBalancePath) {
        this.config = config;
        this.balanceSummary = balanceSummary;
        this.taskNode = taskNode;
        this.curatorFramework = curatorFramework;
        this.idManager = idManager;
        this.serviceManager = serviceManager;
        this.storageName = storageName;
        this.localPartitionInterface = localPartitionInterface;
        this.fileClient = new SimpleFileClient();
        // 恢复需要对节点进行监听
        nodeCache = CuratorCacheFactory.getNodeCache();
        cache = new TaskNodeCache(balanceSummary, this.curatorFramework, taskNode);
        nodeCache.addListener(taskNode, cache);
        this.selfNode = taskNode + Constants.SEPARATOR + this.idManager.getFirstSever();
        this.delayTime = balanceSummary.getDelayTime();
        this.baseBalancePath = baseBalancePath;
    }

    @Override
    public void recover() throws Exception {
        LOG.info("begin virtual recover");

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
                LOG.error("sleep 1s interrupted ", e);
            }
        }
        // 注册节点
        LOG.info("create:" + selfNode + "-------------" + detail);
        // 主任务结束，则直接退出
        if (balanceSummary.getTaskStatus().equals(TaskStatus.FINISH)) {
            finishTask();
            return;
        }

        try {
            for (int i = 0; i < delayTime; i++) {
                if (cache.getStatus().get().equals(TaskStatus.CANCEL)) {
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

        Thread cosumerThread = new Thread(consumerQueue());
        cosumerThread.start();

        int timeFileCounts = 0;
        Collection<LocalPartitionInfo> localPartitionInfos = this.localPartitionInterface.getPartitions();

        try {
            for (LocalPartitionInfo partitionInfo : localPartitionInfos) {
                String partitionPath = partitionInfo.getDataDir();
                String snDataDir = partitionPath + FileUtils.FILE_SEPARATOR + storageName;
                LOG.info("storage data dir: {}", snDataDir);

                if (!FileUtils.isExist(snDataDir)) {
                    snDirNonExistNum.incrementAndGet();
                    continue;
                }

                List<String> replicasNames = FileUtils.listFileNames(snDataDir);
                for (String replicasName : replicasNames) {
                    String replicasPath = snDataDir + FileUtils.FILE_SEPARATOR + replicasName;
                    timeFileCounts += FileUtils.listFileNames(replicasPath).size();
                }

                detail.setTotalDirectories(timeFileCounts);
                updateDetail(selfNode, detail);

                String remoteSecondId = balanceSummary.getInputServers().get(0);
                String remoteFirstId = balanceSummary.getVirtualTarget();
                String virtualId = balanceSummary.getServerId();
                LOG.info("virtualID {}, remoteSecond {}, remoteFirstId {}", virtualId, remoteSecondId, remoteFirstId);
                List<BRFSPath> allPaths = BRFSFileUtil.scanFile(partitionPath, storageName);

                for (BRFSPath brfsPath : allPaths) {
                    if (cache.getStatus().get().equals(TaskStatus.CANCEL)) {
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

                        if (fileServerIds.contains(virtualId)) {
                            // 此处位置需要加1，副本数从1开始
                            replicaPot = fileServerIds.indexOf(virtualId) + 1;
                            FileRecoverMeta fileMeta =
                                new FileRecoverMeta(perFile, fileName, remoteSecondId, timeFileName, Integer
                                    .parseInt(brfsPath.getIndex()), replicaPot, remoteFirstId, partitionPath);
                            try {
                                fileRecoverQueue.put(fileMeta);
                            } catch (InterruptedException e) {
                                LOG.error("put file: " + fileMeta, e);
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("scan virtual serverId file error", e);
        } finally {
            // 所有的文件已经处理完毕，等待队列为空
            overFlag = true;
        }

        try {
            cosumerThread.join();
        } catch (InterruptedException e) {
            LOG.error("cosumerThread error!", e);
        }

        finishTask();

        if (snDirNonExistNum.get() == localPartitionInfos.size()) {
            LOG.info("virtual finish task because of snDirNonExistNum equal localPartitionInfos size");
            finishTask();
        }
    }

    public void finishTask() throws Exception {
        // 没有取消任务
        if (!cache.getStatus().get().equals(TaskStatus.CANCEL)) {
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

    private Runnable consumerQueue() {
        return new Runnable() {

            @Override
            public void run() {
                try {
                    FileRecoverMeta fileRecover = null;
                    while (fileRecover != null || !overFlag) {
                        if (cache.getStatus().get().equals(TaskStatus.CANCEL)) {
                            break;
                        }

                        fileRecover = fileRecoverQueue.poll(100, TimeUnit.MILLISECONDS);
                        if (fileRecover != null) {
                            String logicPath = storageName + FileUtils.FILE_SEPARATOR + fileRecover
                                .getReplica() + FileUtils.FILE_SEPARATOR + fileRecover.getTime();
                            String remoteDir = storageName + FileUtils.FILE_SEPARATOR + fileRecover
                                .getPot() + FileUtils.FILE_SEPARATOR + fileRecover.getTime();
                            String localFilePath =
                                fileRecover.getPartitionPath() + FileUtils.FILE_SEPARATOR + logicPath + FileUtils.FILE_SEPARATOR
                                    + fileRecover.getFileName();
                            boolean success;
                            LOG.info("transfer: {}", fileRecover);
                            String firstId = fileRecover.getFirstServerID();

                            int retryTimes = 0;
                            int transferRetryTimes = 0;
                            while (true) {
                                Service service = serviceManager.getServiceById(config.getDataNodeGroup(), firstId);

                                if (service == null) {
                                    LOG.warn("first id is {} : {}, maybe down!", config.getDataNodeGroup(), firstId);
                                    Thread.sleep(3000);
                                    // 当执行虚拟serverId迁移任务时发生目标节点挂掉的情况，则等待5分钟若目标节点还未连接上，则取消任务
                                    if (retryTimes++ >= 100) {
                                        cache.getStatus().set(TaskStatus.CANCEL);
                                        LOG.warn("current virtual task will cancel because wait [{}] more than five minutes",
                                                 firstId);
                                        break;
                                    }
                                    continue;
                                }

                                String selectedPartitionId =
                                    idManager.getPartitionId(fileRecover.getSelectedSecondId(), balanceSummary.getStorageIndex());
                                String partitionIdRecoverFileName = selectedPartitionId + ":" + fileRecover.getFileName();
                                LOG.info("localFilePath:{}, remoteDir:{}, partitionIdRecoverFileName:{}", localFilePath,
                                         remoteDir, partitionIdRecoverFileName);
                                success = secureCopyTo(service, localFilePath, remoteDir, partitionIdRecoverFileName);
                                if (success) {
                                    break;
                                } else {
                                    if (transferRetryTimes < 3) {
                                        transferRetryTimes++;
                                    } else {
                                        // 当文件超过重试次数传输失败时，取消任务
                                        balanceSummary.setTaskStatus(TaskStatus.CANCEL);
                                        String taskPath = ZKPaths
                                            .makePath(taskNode, String.valueOf(balanceSummary.getStorageIndex()),
                                                      Constants.TASK_NODE);
                                        LOG.info("replica file transfer failed, will cancel the virtual task [{}]", taskPath);

                                        // 删除在zk上的task任务
                                        String taskHistoryPath =
                                            ZKPaths.makePath(baseBalancePath, Constants.TASKS_HISTORY_NODE);
                                        String taskHistory = ZKPaths
                                            .makePath(taskHistoryPath, String.valueOf(balanceSummary.getStorageIndex()),
                                                      balanceSummary.getChangeID());
                                        try {
                                            curatorFramework.setData()
                                                            .forPath(taskPath, JsonUtils.toJsonBytesQuietly(balanceSummary));
                                            byte[] data = curatorFramework.getData().forPath(taskPath);

                                            if (curatorFramework.checkExists().forPath(taskPath) != null) {
                                                curatorFramework.delete().deletingChildrenIfNeeded().forPath(taskPath);
                                            }
                                            curatorFramework.create().creatingParentsIfNeeded()
                                                            .forPath(taskHistory, data);
                                        } catch (Exception e) {
                                            LOG.error("clear virtual task [{}] failed", taskPath, e);
                                        }
                                        break;
                                    }
                                }
                            }

                            currentCount += 1;
                            detail.setCurentCount(currentCount);
                            detail.setProcess(detail.getCurentCount() / (double) detail.getTotalDirectories());
                            updateDetail(selfNode, detail);
                            LOG.info("update:" + selfNode + "-------------" + detail);
                        }
                    }
                } catch (Exception e) {
                    LOG.error("consumer virtual queue happen error ", e);
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
            LOG.error("copy file {}/{} to {}:{} happen error", localPath, fileName, service.getHost(), remoteDir, e);
        }
        return success;
    }

    /**
     * 概述：更新任务信息
     *
     * @param node
     *
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public void updateDetail(String node, TaskDetail detail) throws Exception {
        if (curatorFramework.checkExists().forPath(node) != null) {
            try {
                curatorFramework.setData().forPath(node, JsonUtils.toJsonBytes(detail));
            } catch (Exception e) {
                LOG.error("change Task status error!", e);
            }
        }
    }

    /**
     * 概述：注册节点
     *
     * @param node
     *
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public TaskDetail registerNodeDetail(String node) {
        TaskDetail detail = null;
        try {
            if (curatorFramework.checkExists().forPath(node) == null) {
                detail = new TaskDetail(idManager.getFirstSever(), ExecutionStatus.INIT, 0, 0, 0);
                curatorFramework.create()
                                .creatingParentsIfNeeded()
                                .withMode(CreateMode.PERSISTENT)
                                .forPath(node, JsonUtils.toJsonBytes(detail));
            } else {
                byte[] data = curatorFramework.getData().forPath(node);
                detail = JsonUtils.toObject(data, TaskDetail.class);
            }
        } catch (Exception e) {
            LOG.error("registernode {} happen error", node, e);
        }

        return detail;

    }
}
