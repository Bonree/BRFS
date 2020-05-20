package com.bonree.brfs.rebalancev2.recover;

import com.bonree.brfs.common.rebalance.Constants;
import com.bonree.brfs.common.rebalance.route.NormalRouteInterface;
import com.bonree.brfs.common.rebalance.route.VirtualRoute;
import com.bonree.brfs.common.rebalance.route.impl.v2.NormalRouteV2;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.utils.BRFSFileUtil;
import com.bonree.brfs.common.utils.BRFSPath;
import com.bonree.brfs.common.utils.CompareFromName;
import com.bonree.brfs.common.utils.FileUtils;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.common.utils.Pair;
import com.bonree.brfs.common.utils.RebalanceUtils;
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
import com.bonree.brfs.rebalance.route.BlockAnalyzer;
import com.bonree.brfs.rebalance.route.RouteLoader;
import com.bonree.brfs.rebalance.route.impl.RouteParser;
import com.bonree.brfs.rebalance.task.TaskDetail;
import com.bonree.brfs.rebalance.task.TaskStatus;
import com.bonree.brfs.rebalancev2.task.BalanceTaskSummaryV2;
import com.bonree.brfs.rebalancev2.transfer.SimpleFileClient;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @date 2018年4月9日 下午2:18:16
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description: 副本恢复
 ******************************************************************************/
public class MultiRecoverV2 implements DataRecover {

    private Logger log = LoggerFactory.getLogger(MultiRecoverV2.class);

    private static final String NAME_SEPARATOR = "_";
    private String baseRoutesPath;

    // 该SN历史迁移路由信息
    private Map<String, NormalRouteV2> normalRoutes;
    private Map<String, VirtualRoute> virtualRoutes;

    private SimpleFileClient fileClient;
    private final String storageName;
    private final ServiceManager serviceManager;
    private IDSManager idManager;

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
    private RouteLoader routeLoader;
    private AtomicReference<TaskStatus> status;
    private AtomicInteger snDirNonExistNum = new AtomicInteger();

    private BlockingQueue<FileRecoverMetaV2> fileRecoverQueue = new ArrayBlockingQueue<>(2000);

    public MultiRecoverV2(LocalPartitionInterface localPartitionInterface, RouteLoader routeLoader, BalanceTaskSummaryV2 summary,
                          IDSManager idManager, ServiceManager serviceManager, String taskNode, CuratorClient client,
                          String storageName,
                          String baseRoutesPath) {
        this.balanceSummary = summary;
        this.idManager = idManager;
        this.serviceManager = serviceManager;
        this.taskNode = taskNode;
        this.baseRoutesPath = baseRoutesPath;
        this.client = client;
        this.storageName = storageName;
        this.fileClient = new SimpleFileClient();
        this.localPartitionInterface = localPartitionInterface;
        this.routeLoader = routeLoader;
        // 开启监控
        nodeCache = CuratorCacheFactory.getNodeCache();
        nodeCache.addListener(taskNode, new RecoverListener("recover_listener"));
        this.selfNode = taskNode + Constants.SEPARATOR + this.idManager.getFirstSever();
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
            log.info("receive update event!!!");
            if (client.checkExists(taskNode)) {
                byte[] data = client.getData(taskNode);
                BalanceTaskSummaryV2 bts = JsonUtils.toObject(data, BalanceTaskSummaryV2.class);
                String newID = bts.getId();
                String oldID = balanceSummary.getId();
                if (newID.equals(oldID)) { // 是同一个任务
                    TaskStatus stats = bts.getTaskStatus();
                    // 更新缓存
                    status.set(stats);
                    log.info("stats: {}", stats);
                } else { // 不是同一个任务
                    log.info("newID:{} not match oldID:{}", newID, oldID);
                    log.info("cancel multi recover:{}", balanceSummary);
                    status.set(TaskStatus.CANCEL);
                }
            } else {
                log.info("task is deleted, this task will cancel!");
                status.set(TaskStatus.CANCEL);
            }
        }
    }

    /**
     * @description: 加载路由规则到缓存
     */
    public void loadRoutingRules() {
        // load virtual id
        String virtualPath = baseRoutesPath + Constants.SEPARATOR + Constants.VIRTUAL_ROUTE + Constants.SEPARATOR
            + balanceSummary.getStorageIndex();
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
        log.info("virtual routes: {}", virtualRoutes);

        // load normal id
        String normalPath = baseRoutesPath + Constants.SEPARATOR + Constants.NORMAL_ROUTE + Constants.SEPARATOR
            + balanceSummary.getStorageIndex();
        if (client.checkExists(normalPath)) {
            List<String> normalNodes = client.getChildren(normalPath);
            if (normalNodes != null && !normalNodes.isEmpty()) {
                for (String normalNode : normalNodes) {
                    String dataPath = normalPath + Constants.SEPARATOR + normalNode;
                    byte[] data = client.getData(dataPath);
                    NormalRouteV2 normal = JsonUtils.toObjectQuietly(data, NormalRouteV2.class);
                    normalRoutes.put(normal.getSecondID(), normal);
                }
            }
        }
        log.info("normal routes: {}", normalRoutes);
    }

    @Override
    public void recover() {

        log.info("begin normal recover");
        // 注册节点
        log.info("register self node, path:{}", selfNode);
        // 无注册的话，则注册，否则不用注册
        while (true) {
            detail = registerNodeDetail(selfNode);
            if (detail != null) {
                log.info("register {} is successful!!", selfNode);
                break;
            }
            log.error("register {} is error!!", selfNode);
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
                    log.info("sub task is pause for {}", balanceSummary);
                    Thread.sleep(1000);
                }

                // 倒计时完毕，则不需要倒计时
                if (!detail.getStatus().equals(ExecutionStatus.INIT)) {
                    break;
                }
                if (delayTime - i <= 10) {
                    log.info("remain time:" + (delayTime - i) + "s, start task!!!");
                } else {
                    if ((delayTime - i) % 10 == 0) {
                        log.info("remain time:" + (delayTime - i) + "s, start task!!!");
                    }
                }
                Thread.sleep(1000);
            }
        } catch (InterruptedException e) {
            log.error("task back time count interrupt!!", e);
        }

        detail.setStatus(ExecutionStatus.RECOVER);
        log.info("update self node, path:{}, detail:{}", selfNode, detail);
        updateDetail(selfNode, detail);

        Collection<LocalPartitionInfo> localPartitionInfos = this.localPartitionInterface.getPartitions();
        // 启动消费队列
        Thread consumerThread = new Thread(consumerQueue());
        consumerThread.start();
        try {
            for (LocalPartitionInfo partitionInfo : localPartitionInfos) {

                String partitionPath = partitionInfo.getDataDir();
                String snDataDir = partitionPath + FileUtils.FILE_SEPARATOR + storageName;
                if (!FileUtils.isExist(snDataDir)) {
                    snDirNonExistNum.incrementAndGet();
                    continue;
                }

                List<BRFSPath> allPaths = BRFSFileUtil.scanFile(partitionPath, storageName);
                int fileCounts = allPaths.size();

                detail.setTotalDirectories(fileCounts);
                updateDetail(selfNode, detail);

                log.info("deal the local server: {}",
                         idManager.getSecondId(partitionInfo.getPartitionId(), balanceSummary.getStorageIndex()));

                RouteParser routeParser = new RouteParser(balanceSummary.getStorageIndex(), routeLoader);
                NormalRouteV2 normalRoute =
                    new NormalRouteV2(balanceSummary.getChangeID(), balanceSummary.getStorageIndex(),
                                      balanceSummary.getServerId(),
                                      balanceSummary.getNewSecondIds(), balanceSummary.getSecondFirstShip());

                // 遍历副本文件
                for (BRFSPath brfsPath : allPaths) {
                    if (status.get().equals(TaskStatus.CANCEL)) {
                        return;
                    }
                    String perFile = partitionPath + FileUtils.FILE_SEPARATOR + brfsPath.toString();
                    if (!perFile.endsWith(".rd")) {
                        log.info("prepare deal file:{}, partitionPath:{}", brfsPath.toString(), partitionPath);
                        dealFileV2(brfsPath, partitionPath, routeParser, normalRoute);
                    }
                }

            }
        } catch (Exception e) {
            log.error("scan file happen error {}", localPartitionInfos, e);
        } finally {
            this.overFlag = true;
        }
        try {
            log.info("waiting consumer...{}", this.overFlag);
            consumerThread.join();
        } catch (InterruptedException e) {
            log.error("consumerThread error!", e);
        }

        finishTask();

        if (snDirNonExistNum.get() == localPartitionInfos.size()) {
            log.info("normal finish task because of snDirNonExistNum equal localPartitionInfos size");
            finishTask();
        }
    }

    public void finishTask() {
        // 没有取消任务
        if (!status.get().equals(TaskStatus.CANCEL)) {
            detail.setStatus(ExecutionStatus.FINISH);
            updateDetail(selfNode, detail);
            log.info("恢复正常完成！！！！！");
        }

        try {
            nodeCache.cancelListener(taskNode);
        } catch (IOException e) {
            log.error("cancel listener failed!!", e);
        }
    }

    /**
     * @param perFile       文件块路径
     * @param fileName      文件块名称
     * @param timeFileName  相对的时间块
     * @param replica       所在的位置
     * @param partitionPath 本地存储根目录
     */
    private void dealFile(String perFile, String fileName, String timeFileName, int replica, String partitionPath) {
        // 对文件名进行分割处理
        String[] metaArr = fileName.split(NAME_SEPARATOR);
        // 提取出用于hash的部分
        String namePart = metaArr[0];

        // 整理2级serverID，转换虚拟serverID
        RouteParser parser = new RouteParser(0, null);

        List<String> fileServerIds = new ArrayList<>();
        for (int j = 1; j < metaArr.length; j++) {
            if (metaArr[j].charAt(0) == Constants.VIRTUAL_ID) {
                VirtualRoute virtualRoute = virtualRoutes.get(metaArr[j]);
                if (virtualRoute != null) {
                    fileServerIds.add(virtualRoute.getNewSecondID());
                } else {
                    log.error("file {} is exception!!", perFile);
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
                    log.info("deadServer: {}", deadServer);
                    int pot = fileServerIds.indexOf(deadServer);
                    if (!StringUtils.equals(deadServer, balanceSummary.getServerId())) {
                        recoverableServerList = getRecoverRoleList(deadServer);
                    } else {
                        recoverableServerList = balanceSummary.getInputServers();
                    }

                    exceptionServerIds = new ArrayList<>(fileServerIds);
                    exceptionServerIds.remove(deadServer);
                    selectableServerList = getSelectedList(recoverableServerList, exceptionServerIds);

                    int index = RebalanceUtils.hashFileName(namePart, selectableServerList.size());
                    selectMultiId = selectableServerList.get(index);
                    fileServerIds.set(pot, selectMultiId);
                    log.info("recoverableServerList: {}, selectableServerList:{}, selectMultiId:{}", recoverableServerList,
                             selectableServerList, selectMultiId);

                    // 判断选取的新节点是否存活
                    if (isAlive(getAliveMultiIds(), selectMultiId)) {
                        String secondServerIDSelected =
                            idManager.getSecondId(balanceSummary.getPartitionId(), balanceSummary.getStorageIndex());

                        // 判断选取的新节点是否为本节点
                        if (!secondServerIDSelected.equals(selectMultiId)) {
                            String firstID = idManager.getFirstId(selectMultiId, balanceSummary.getStorageIndex());
                            FileRecoverMetaV2 fileMeta =
                                new FileRecoverMetaV2(perFile, fileName, selectMultiId, timeFileName, replica, pot + 1,
                                                      firstID,
                                                      partitionPath);
                            try {
                                fileRecoverQueue.put(fileMeta);
                            } catch (InterruptedException e) {
                                log.error("put file: " + fileMeta, e);
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * @param brfsPath      本机文件块信息
     * @param partitionPath 本机磁盘id根目录
     * @param parser        路由规则解析
     * @param normal        由changgeSummary转化来的路由规则
     */
    private void dealFileV2(BRFSPath brfsPath, String partitionPath, RouteParser parser, NormalRouteInterface normal) {

        // 1.由路由规则解析出来的服务
        String[] analysisSecondIds = parser.searchVaildIds(brfsPath.getFileName());
        List<String> validSecondIds = new ArrayList<>();
        // 2.检查元素，若存在虚拟id，则不进行恢复
        for (String id : analysisSecondIds) {
            if (parser.isVirtualID(id)) {
                log.warn("current id is virtual id, will return");
                return;
            }
            validSecondIds.add(id);
        }

        List<String> aliveMultiIds = getAliveMultiIds();
        log.info("analysis second ids from route:{}, valid second ids:{}, aliveMultiIds:{}", analysisSecondIds,
                 validSecondIds,
                 aliveMultiIds);

        // 3.收集已经不可用的服务集合，若集合为空，则文件不需要恢复
        List<String> deadSecondIds =
            validSecondIds.stream().filter(x -> !aliveMultiIds.contains(x)).collect(Collectors.toList());
        if (deadSecondIds.isEmpty()) {
            log.info("dead second ids is empty!");
            return;
        }
        // 4.解析文件名 用于当前需要发布的路由规则
        Pair<String, List<String>> fileInfoPair = BlockAnalyzer.analyzingFileName(brfsPath.getFileName());
        int fileCode = BlockAnalyzer.sumName(fileInfoPair.getFirst());
        List<String> excludes = fileInfoPair.getSecond();
        // excludes.addAll(idManager.getSecondIds(idManager.getFirstSever(), balanceSummary.getStorageIndex()));
        // 排除本机二级serverId

        log.info("dead second ids:{}", deadSecondIds);
        // 5.遍历不可用的服务
        for (String deadServer : deadSecondIds) {
            log.info("deadServer: {}", deadServer);
            int pot = validSecondIds.indexOf(deadServer) + 1;
            // 5-1.若发现不可用的secondid，则可能发生新的变更，将由下次任务执行，本次不进行操作
            if (!deadServer.equals(normal.getBaseSecondId())) {
                log.warn("recovery find unable file:[{}],analysis:{} second {} route:[{}]", brfsPath.getFileName(),
                         validSecondIds, deadServer, normal);
                continue;
            }
            // 5-2.根据预发布的路由规则进行解析，
            String selectMultiId = normal.locateNormalServer(fileCode, excludes);

            log.info("alive multiIds:{}, normal route:{}", aliveMultiIds, normal);

            // 5-3.判断选取的新节点是否存活
            if (isAlive(aliveMultiIds, selectMultiId)) {
                String secondServerIDSelected =
                    idManager.getSecondId(balanceSummary.getPartitionId(), balanceSummary.getStorageIndex());
                // 5-4.判断要恢复的secondId 与选中的secondid是否一致，一致，则表明该节点已经启动，则不做处理，不一致则需要作恢复
                log.info("select second server id:{}, select multi id:{}", secondServerIDSelected, selectMultiId);

                if (!secondServerIDSelected.equals(selectMultiId)) {
                    String firstID = idManager.getFirstId(selectMultiId, balanceSummary.getStorageIndex());
                    FileRecoverMetaV2 fileMeta =
                        new FileRecoverMetaV2(partitionPath + File.separator + brfsPath.toString(), brfsPath.getFileName(),
                                              selectMultiId, getTimeDir(brfsPath), Integer.parseInt(brfsPath.getIndex()), pot,
                                              firstID, partitionPath);
                    try {
                        fileRecoverQueue.put(fileMeta);
                    } catch (InterruptedException e) {
                        log.error("put file [{}] err", fileMeta, e);
                    }
                }
            }
        }
    }

    /**
     * 获取BRFS文件块的时间目录
     *
     * @param brfsPath
     *
     * @return
     */
    private String getTimeDir(BRFSPath brfsPath) {
        return brfsPath.getYear() + FileUtils.FILE_SEPARATOR + brfsPath
            .getMonth() + FileUtils.FILE_SEPARATOR + brfsPath.getDay() + FileUtils.FILE_SEPARATOR + brfsPath
            .getHourMinSecond();
    }

    private Runnable consumerQueue() {
        return new Runnable() {
            @Override
            public void run() {
                try {
                    FileRecoverMetaV2 fileRecover = null;
                    while (fileRecover != null || !overFlag) {
                        log.info("current task status:{}", status);
                        if (status.get().equals(TaskStatus.CANCEL)) {
                            break;
                        } else if (status.get().equals(TaskStatus.PAUSE)) {
                            log.info("task pause!!!");
                            Thread.sleep(1000);
                        } else if (status.get().equals(TaskStatus.RUNNING)) {
                            fileRecover = fileRecoverQueue.poll(1, TimeUnit.SECONDS);
                            log.info("fileRecover:{}", fileRecover);
                            if (fileRecover != null) {
                                String localDir = storageName + FileUtils.FILE_SEPARATOR + fileRecover
                                    .getReplica() + FileUtils.FILE_SEPARATOR + fileRecover.getTime();
                                String remoteDir = storageName + FileUtils.FILE_SEPARATOR + fileRecover
                                    .getPot() + FileUtils.FILE_SEPARATOR + fileRecover.getTime();
                                String localFilePath = fileRecover.getPartitionPath() + FileUtils.FILE_SEPARATOR + localDir
                                    + FileUtils.FILE_SEPARATOR + fileRecover
                                    .getFileName();
                                boolean success;
                                while (true) {
                                    if (status.get().equals(TaskStatus.PAUSE)) {
                                        log.info("task pause!!!");
                                        Thread.sleep(1000);
                                        continue;
                                    }
                                    if (status.get().equals(TaskStatus.CANCEL)) {
                                        break;
                                    }
                                    Service service = serviceManager.getServiceById(
                                        Configs.getConfiguration()
                                               .getConfig(
                                                   CommonConfigs.CONFIG_DATA_SERVICE_GROUP_NAME),
                                        fileRecover
                                            .getFirstServerID());
                                    if (service == null) {
                                        log.warn("get service by first server id [{}] is null, maybe wait and try again!",
                                                 fileRecover.getFirstServerID());
                                        Thread.sleep(1000);
                                        continue;
                                    }

                                    String selectedPartitionId = idManager
                                        .getPartitionId(fileRecover.getSelectedSecondId(), balanceSummary.getStorageIndex());
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
                                log.info("update:" + selfNode + "-------------" + detail);
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
        return new ArrayList<>(normalRoutes.get(deadSecondID).getNewSecondIDs().keySet());
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
     *
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public void updateDetail(String node, TaskDetail detail) {
        if (client.checkExists(node)) {
            try {
                client.setData(node, JsonUtils.toJsonBytes(detail));
            } catch (Exception e) {
                log.error("update task detail error!", e);
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
        TaskDetail detail;
        if (!client.checkExists(node)) {
            detail = new TaskDetail(idManager.getFirstSever(), ExecutionStatus.INIT, 0, 0, 0);
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
