package com.bonree.brfs.rebalanceV2.recover;

import com.bonree.brfs.common.rebalance.Constants;
import com.bonree.brfs.common.rebalance.route.NormalRoute;
import com.bonree.brfs.common.rebalance.route.VirtualRoute;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.utils.*;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.common.zookeeper.curator.cache.AbstractNodeCacheListener;
import com.bonree.brfs.common.zookeeper.curator.cache.CuratorCacheFactory;
import com.bonree.brfs.common.zookeeper.curator.cache.CuratorNodeCache;
import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.CommonConfigs;
import com.bonree.brfs.identification.LocalPartitionInterface;
import com.bonree.brfs.partition.model.LocalPartitionInfo;
import com.bonree.brfs.rebalance.DataRecover;
import com.bonree.brfs.rebalance.task.TaskDetail;
import com.bonree.brfs.rebalance.task.TaskStatus;
import com.bonree.brfs.rebalanceV2.task.BalanceTaskSummaryV2;
import com.bonree.brfs.rebalanceV2.transfer.SimpleFileClient;
import com.bonree.brfs.server.identification.ServerIDManager;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @date 2018年4月9日 下午2:18:16
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description: 副本恢复
 ******************************************************************************/
public class MultiRecoverV2 implements DataRecover {

    private Logger LOG = LoggerFactory.getLogger(MultiRecoverV2.class);

    private static final String NAME_SEPARATOR = "_";
    private String baseRoutesPath;

    // 该SN历史迁移路由信息
    private Map<String, NormalRoute> normalRoutes;
    private Map<String, VirtualRoute> virtualRoutes;

    private SimpleFileClient fileClient;
    private final String storageName;
    private final ServiceManager serviceManager;
    private ServerIDManager idManager;

    private final String taskNode;
    private final String selfNode;

    private BalanceTaskSummaryV2 balanceSummary;
    private CuratorNodeCache nodeCache;
    private final CuratorClient client;
    private final long delayTime;
    private boolean overFlag = false;
    private TaskDetail detail;
    private int currentCount = 0;
    private LocalPartitionInterface localPartitionInterface;
    private AtomicReference<TaskStatus> status;

    private BlockingQueue<FileRecoverMetaV2> fileRecoverQueue = new ArrayBlockingQueue<>(2000);


    public MultiRecoverV2(LocalPartitionInterface localPartitionInterface, BalanceTaskSummaryV2 summary, ServerIDManager idManager, ServiceManager serviceManager, String taskNode, CuratorClient client, String storageName, String baseRoutesPath) {
        this.balanceSummary = summary;
        this.idManager = idManager;
        this.serviceManager = serviceManager;
        this.taskNode = taskNode;
        this.baseRoutesPath = baseRoutesPath;
        this.client = client;
        this.storageName = storageName;
        this.fileClient = new SimpleFileClient();
        this.localPartitionInterface = localPartitionInterface;
        // 开启监控
        nodeCache = CuratorCacheFactory.getNodeCache();
        nodeCache.addListener(taskNode, new RecoverListener("recover_listener"));
        this.selfNode = taskNode + Constants.SEPARATOR + this.idManager.getFirstServerID();
        this.delayTime = balanceSummary.getDelayTime();
        status = new AtomicReference<>(summary.getTaskStatus());

        virtualRoutes = new HashMap<>();
        normalRoutes = new HashMap<>();
        loadRoutingRules();
    }


    private class RecoverListener extends AbstractNodeCacheListener {

        public RecoverListener(String listenName) {
            super(listenName);
        }

        @Override
        public void nodeChanged() throws Exception {
            LOG.info("receive update event!!!");
            if (client.checkExists(taskNode)) {
                byte[] data = client.getData(taskNode);
                BalanceTaskSummaryV2 bts = JsonUtils.toObject(data, BalanceTaskSummaryV2.class);
                String newID = bts.getId();
                String oldID = balanceSummary.getId();
                if (newID.equals(oldID)) { // 是同一个任务
                    TaskStatus stats = bts.getTaskStatus();
                    // 更新缓存
                    status.set(stats);
                    LOG.info("stats: {}", stats);
                } else { // 不是同一个任务
                    LOG.info("newID:{} not match oldID:{}", newID, oldID);
                    LOG.info("cancel multi recover:{}", balanceSummary);
                    status.set(TaskStatus.CANCEL);
                }
            } else {
                LOG.info("task is deleted, this task will cancel!");
                status.set(TaskStatus.CANCEL);
            }
        }
    }

    /**
     * @description: 加载路由规则到缓存
     */
    public void loadRoutingRules() {
        // load virtual id
        String virtualPath = baseRoutesPath + Constants.SEPARATOR + Constants.VIRTUAL_ROUTE + Constants.SEPARATOR + balanceSummary.getStorageIndex();
        List<String> virtualNodes = client.getChildren(virtualPath);
        if (client.checkExists(virtualPath)) {
            if (virtualNodes != null && !virtualNodes.isEmpty()) {
                for (String virtualNode : virtualNodes) {
                    String dataPath = virtualPath + Constants.SEPARATOR + virtualNode;
                    byte[] data = client.getData(dataPath);
                    VirtualRoute virtual = JsonUtils.toObjectQuietly(data, VirtualRoute.class);
                    virtualRoutes.put(virtual.getVirtualID(), virtual);
                }
            }
        }
        LOG.info("virtual routes: {}", virtualRoutes);

        // load normal id
        String normalPath = baseRoutesPath + Constants.SEPARATOR + Constants.NORMAL_ROUTE + Constants.SEPARATOR + balanceSummary.getStorageIndex();
        if (client.checkExists(normalPath)) {
            List<String> normalNodes = client.getChildren(normalPath);
            if (normalNodes != null && !normalNodes.isEmpty()) {
                for (String normalNode : normalNodes) {
                    String dataPath = normalPath + Constants.SEPARATOR + normalNode;
                    byte[] data = client.getData(dataPath);
                    NormalRoute normal = JsonUtils.toObjectQuietly(data, NormalRoute.class);
                    normalRoutes.put(normal.getSecondID(), normal);
                }
            }
        }
        LOG.info("normal routes: {}", normalRoutes);
    }

    @Override
    public void recover() {

        LOG.info("begin normal recover");
        // 注册节点
        LOG.info("register self node, path:{} ,detail: {}", selfNode, detail);
        // 无注册的话，则注册，否则不用注册
        while (true) {
            detail = registerNodeDetail(selfNode);
            if (detail != null) {
                LOG.info("register {} is successful!!", selfNode);
                break;
            }
            LOG.error("register {} is error!!", selfNode);
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
                // 暂时用循环控制，后期重构改成wait notify机制
                while (true) {
                    if (!status.get().equals(TaskStatus.PAUSE)) {
                        break;
                    }
                    LOG.info("sub task is pause for {}", balanceSummary);
                    Thread.sleep(1000);
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
        LOG.info("update self node, path:{}, detail:{}", selfNode, detail);
        updateDetail(selfNode, detail);

        Collection<LocalPartitionInfo> localPartitionInfos = this.localPartitionInterface.getPartitions();

        for (LocalPartitionInfo partitionInfo : localPartitionInfos) {
            String partitionPath = partitionInfo.getDataDir();
            String snDataDir = partitionPath + FileUtils.FILE_SEPARATOR + storageName;
            if (!FileUtils.isExist(snDataDir)) {
                finishTask();
                return;
            }

            List<BRFSPath> allPaths = BRFSFileUtil.scanFile(partitionPath, storageName);

            int fileCounts = allPaths.size();

            // 启动消费队列
            Thread cosumerThread = new Thread(consumerQueue());
            cosumerThread.start();

            detail.setTotalDirectories(fileCounts);
            updateDetail(selfNode, detail);

            LOG.info("deal the local server: {}", idManager.getSecondServerID(balanceSummary.getStorageIndex()));

            // 遍历副本文件
            // dealReplicas(replicasNames, snDataDir);
            for (BRFSPath brfsPath : allPaths) {
                if (status.get().equals(TaskStatus.CANCEL)) {
                    return;
                }
                String perFile = partitionPath + FileUtils.FILE_SEPARATOR + brfsPath.toString();
                String timeFile = brfsPath.getYear() + FileUtils.FILE_SEPARATOR + brfsPath
                        .getMonth() + FileUtils.FILE_SEPARATOR + brfsPath.getDay() + FileUtils.FILE_SEPARATOR + brfsPath
                        .getHourMinSecond();

                if (!perFile.endsWith(".rd")) {
                    dealFile(perFile, brfsPath.getFileName(), timeFile, Integer.parseInt(brfsPath.getIndex()), partitionPath);
                }
            }

            overFlag = true;
            LOG.info("waiting consumer...");
            try {
                cosumerThread.join();
            } catch (InterruptedException e) {
                LOG.error("consumerThread error!", e);
            }

            finishTask();
        }
    }

    public void finishTask() {
        // 没有取消任务
        if (!status.get().equals(TaskStatus.CANCEL)) {
            detail.setStatus(ExecutionStatus.FINISH);
            updateDetail(selfNode, detail);
            LOG.info("恢复正常完成！！！！！");
        }

        try {
            nodeCache.cancelListener(taskNode);
        } catch (IOException e) {
            LOG.error("cancel listener failed!!", e);
        }
    }

    private void dealFile(String perFile, String fileName, String timeFileName, int replica, String partitionPath) {
        // 对文件名进行分割处理
        String[] metaArr = fileName.split(NAME_SEPARATOR);
        // 提取出用于hash的部分
        String namePart = metaArr[0];

        // 整理2级serverID，转换虚拟serverID
        List<String> fileServerIds = new ArrayList<>();
        for (int j = 1; j < metaArr.length; j++) {
            if (metaArr[j].charAt(0) == Constants.VIRTUAL_ID) {
                VirtualRoute virtualRoute = virtualRoutes.get(metaArr[j]);
                if (virtualRoute != null) {
                    fileServerIds.add(virtualRoute.getNewSecondID());
                } else {
                    LOG.error("file {} is exception!!", perFile);
                    return;
                }
            } else {
                fileServerIds.add(metaArr[j]);
            }
        }

        // 这里要判断一个副本是否需要进行迁移
        // 挑选出的可迁移的servers
        String selectMultiId = null;
        // 可获取的server，可能包括自身
        List<String> recoverableServerList = null;
        // 排除掉自身或已有的servers
        List<String> exceptionServerIds = null;
        // 真正可选择的servers
        List<String> selectableServerList = null;

        while (RebalanceUtils.needRecover(fileServerIds, replica, getAliveMultiIds())) {
            for (String deadServer : fileServerIds) {
                if (!getAliveMultiIds().contains(deadServer)) {
                    LOG.info("deadServer: {}", deadServer);
                    int pot = fileServerIds.indexOf(deadServer);
                    if (!StringUtils.equals(deadServer, balanceSummary.getServerId())) {
                        recoverableServerList = getRecoverRoleList(deadServer);
                    } else {
                        recoverableServerList = balanceSummary.getInputServers();
                    }

                    LOG.info("recoverableServerList: {}", recoverableServerList);
                    exceptionServerIds = new ArrayList<>(fileServerIds);
                    exceptionServerIds.remove(deadServer);
                    selectableServerList = getSelectedList(recoverableServerList, exceptionServerIds);

                    int index = RebalanceUtils.hashFileName(namePart, selectableServerList.size());
                    selectMultiId = selectableServerList.get(index);
                    fileServerIds.set(pot, selectMultiId);

                    // 判断选取的新节点是否存活
                    if (isAlive(getAliveMultiIds(), selectMultiId)) {
                        String secondServerIDSelected = idManager.getSecondServerID(balanceSummary.getStorageIndex());

                        // 判断选取的新节点是否为本节点
                        if (!secondServerIDSelected.equals(selectMultiId)) {
                            String firstID = idManager.getOtherFirstID(selectMultiId, balanceSummary.getStorageIndex());
                            FileRecoverMetaV2 fileMeta = new FileRecoverMetaV2(perFile, fileName, selectMultiId, timeFileName, replica, pot + 1, firstID, partitionPath);
                            try {
                                fileRecoverQueue.put(fileMeta);
                            } catch (InterruptedException e) {
                                LOG.error("put file: " + fileMeta, e);
                            }
                        }
                    }
                }
            }
        }
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
                        } else if (status.get().equals(TaskStatus.PAUSE)) {
                            LOG.info("task pause!!!");
                            Thread.sleep(1000);
                        } else if (status.get().equals(TaskStatus.RUNNING)) {
                            fileRecover = fileRecoverQueue.poll(1, TimeUnit.SECONDS);
                            if (fileRecover != null) {
                                String localDir = storageName + FileUtils.FILE_SEPARATOR + fileRecover
                                        .getReplica() + FileUtils.FILE_SEPARATOR + fileRecover.getTime();
                                String remoteDir = storageName + FileUtils.FILE_SEPARATOR + fileRecover
                                        .getPot() + FileUtils.FILE_SEPARATOR + fileRecover.getTime();
                                String localFilePath = fileRecover.getPartitionPath() + FileUtils.FILE_SEPARATOR + localDir + FileUtils.FILE_SEPARATOR + fileRecover
                                        .getFileName();
                                boolean success;
                                while (true) {
                                    if (status.get().equals(TaskStatus.PAUSE)) {
                                        LOG.info("task pause!!!");
                                        Thread.sleep(1000);
                                        continue;
                                    }
                                    if (status.get().equals(TaskStatus.CANCEL)) {
                                        break;
                                    }
                                    Service service = serviceManager.getServiceById(Configs.getConfiguration()
                                            .GetConfig(CommonConfigs.CONFIG_DATA_SERVICE_GROUP_NAME), fileRecover
                                            .getFirstServerID());
                                    if (service == null) {
                                        LOG.warn("first id is {},maybe down!", fileRecover.getFirstServerID());
                                        Thread.sleep(1000);
                                        continue;
                                    }
                                    String partitionIdRecoverFileName = balanceSummary.getPartitionId() + ":" + fileRecover.getFileName();
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
                    }

                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };
    }

    private List<String> getRecoverRoleList(String deadSecondID) {
        return normalRoutes.get(deadSecondID).getNewSecondIDs();
    }

    private List<String> getAliveMultiIds() {
        return balanceSummary.getAliveServer();
    }

    private List<String> getSelectedList(List<String> aliveServerList, List<String> excludeServers) {
        List<String> selectedList = new ArrayList<>();
        for (String tmp : aliveServerList) {
            if (!excludeServers.contains(tmp)) {
                selectedList.add(tmp);
            }
        }
        selectedList.sort(new CompareFromName());
        return selectedList;
    }

    private boolean isAlive(List<String> aliveServers, String serverId) {
        return aliveServers.contains(serverId);
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
                LOG.error("update task detail error!", e);
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
        TaskDetail detail;
        if (!client.checkExists(node)) {
            detail = new TaskDetail(idManager.getFirstServerID(), ExecutionStatus.INIT, 0, 0, 0);
            client.createPersistent(node, false, JsonUtils.toJsonBytesQuietly(detail));
        } else {
            byte[] data = client.getData(node);
            detail = JsonUtils.toObjectQuietly(data, TaskDetail.class);
        }
        return detail;
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

}
