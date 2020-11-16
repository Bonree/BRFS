package com.bonree.brfs.rebalance.recover;

import com.bonree.brfs.common.rebalance.Constants;
import com.bonree.brfs.common.rebalance.route.NormalRouteInterface;
import com.bonree.brfs.common.rebalance.route.impl.v2.NormalRouteV2;
import com.bonree.brfs.common.resource.vo.LocalPartitionInfo;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.utils.BRFSFileUtil;
import com.bonree.brfs.common.utils.BRFSPath;
import com.bonree.brfs.common.utils.FileUtils;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.common.utils.Pair;
import com.bonree.brfs.common.zookeeper.curator.cache.CuratorCacheFactory;
import com.bonree.brfs.common.zookeeper.curator.cache.CuratorNodeCache;
import com.bonree.brfs.duplication.storageregion.StorageRegion;
import com.bonree.brfs.guice.ClusterConfig;
import com.bonree.brfs.identification.IDSManager;
import com.bonree.brfs.identification.LocalPartitionInterface;
import com.bonree.brfs.rebalance.DataRecover;
import com.bonree.brfs.rebalance.route.BlockAnalyzer;
import com.bonree.brfs.rebalance.route.RouteLoader;
import com.bonree.brfs.rebalance.route.impl.RouteParser;
import com.bonree.brfs.rebalance.task.BalanceTaskSummary;
import com.bonree.brfs.rebalance.task.TaskDetail;
import com.bonree.brfs.rebalance.task.TaskStatus;
import com.bonree.brfs.rebalance.task.listener.TaskNodeCache;
import com.bonree.brfs.rebalance.transfer.SimpleFileClient;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
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
public class MultiRecover implements DataRecover {

    private Logger log = LoggerFactory.getLogger(MultiRecover.class);

    private SimpleFileClient fileClient;
    private final StorageRegion storageRegion;
    private final ServiceManager serviceManager;
    private IDSManager idManager;

    private final String taskNode;
    private final String selfNode;

    private BalanceTaskSummary balanceSummary;
    private CuratorNodeCache nodeCache;
    private CuratorFramework curatorFramework;
    private final long delayTime;
    private boolean overFlag = false;
    private TaskDetail detail;
    private int currentCount = 0;
    private LocalPartitionInterface localPartitionInterface;
    private TaskNodeCache cache;
    private String baseBalancePath;
    private AtomicInteger snDirNonExistNum = new AtomicInteger();
    private ClusterConfig config;
    private RouteLoader routeLoader;

    private BlockingQueue<FileRecoverMeta> fileRecoverQueue = new ArrayBlockingQueue<>(2000);

    public MultiRecover(ClusterConfig config, LocalPartitionInterface localPartitionInterface, RouteLoader routeLoader,
                        BalanceTaskSummary summary,
                        IDSManager idManager, ServiceManager serviceManager, String taskNode, CuratorFramework curatorFramework,
                        StorageRegion storageRegion, String baseBalancePath) {
        this.config = config;
        this.balanceSummary = summary;
        this.idManager = idManager;
        this.serviceManager = serviceManager;
        this.taskNode = taskNode;
        this.curatorFramework = curatorFramework;
        this.storageRegion = storageRegion;
        this.fileClient = new SimpleFileClient();
        this.localPartitionInterface = localPartitionInterface;
        // 开启监控
        nodeCache = CuratorCacheFactory.getNodeCache();
        cache = new TaskNodeCache(this.balanceSummary, curatorFramework, taskNode);
        nodeCache.addListener(taskNode, cache);
        this.selfNode = taskNode + Constants.SEPARATOR + this.idManager.getFirstSever();
        this.delayTime = balanceSummary.getDelayTime();
        this.routeLoader = routeLoader;
        this.baseBalancePath = baseBalancePath;
    }

    @SuppressWarnings("checkstyle:EmptyCatchBlock")
    @Override
    public void recover() throws Exception {

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
                log.error("sleep 1s is interrupted", e);
            }
        }
        // 主任务结束，则直接退出
        if (balanceSummary.getTaskStatus().equals(TaskStatus.FINISH)) {
            finishTask();
            return;
        }

        try {
            for (int i = 0; i < delayTime; i++) {
                if (cache.getStatus().get().equals(TaskStatus.CANCEL)) {
                    log.info("change id [{}] with task path [{}] is cancel ",
                            balanceSummary.getChangeID(),
                            taskNode);
                    return;
                }
                // 暂时用循环控制，后期重构改成wait notify机制
                while (true) {
                    if (!cache.getStatus().get().equals(TaskStatus.PAUSE)) {
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
        log.info("partitions size  {}", localPartitionInfos.size());
        // 启动消费队列
        Thread consumerThread = new Thread(consumerQueue());
        consumerThread.start();
        Map<String, String> fileMap = new HashMap<>();
        fileMap.put(BRFSPath.STORAGEREGION, storageRegion.getName());
        try {
            for (LocalPartitionInfo partitionInfo : localPartitionInfos) {

                String partitionPath = partitionInfo.getDataDir();
                String snDataDir = partitionPath + FileUtils.FILE_SEPARATOR + storageRegion.getName();
                if (!FileUtils.isExist(snDataDir)) {
                    snDirNonExistNum.incrementAndGet();
                    continue;
                }

                List<BRFSPath> allPaths = BRFSFileUtil.scanFile(partitionPath, storageRegion.getName());
                int fileCounts = allPaths.size();

                detail.setTotalDirectories(fileCounts);
                updateDetail(selfNode, detail);

                log.info("deal the local server: {}",
                        idManager.getSecondId(partitionInfo.getPartitionId(), balanceSummary.getStorageIndex()));

                NormalRouteV2 normalRoute =
                        new NormalRouteV2(balanceSummary.getChangeID(), balanceSummary.getStorageIndex(),
                                balanceSummary.getServerId(),
                                balanceSummary.getNewSecondIds(), balanceSummary.getSecondFirstShip());
                RouteParser parser = new RouteParser(balanceSummary.getStorageIndex(), this.routeLoader);
                RouteParser oldParser = parser.clone();
                parser.putNormalRoute(normalRoute);
                String localSecond = idManager.getSecondId(partitionInfo.getPartitionId(), balanceSummary.getStorageIndex());
                // 遍历副本文件
                for (BRFSPath brfsPath : allPaths) {
                    if (cache.getStatus().get().equals(TaskStatus.CANCEL)) {
                        return;
                    }
                    String perFile = partitionPath + FileUtils.FILE_SEPARATOR + brfsPath.toString();
                    if (!perFile.endsWith(".rd") && !FileUtils.isExist(perFile + ".rd")) {
                        log.info("prepare deal file:{}, partitionPath:{}", brfsPath.toString(), partitionPath);
                        // dealFileV2(brfsPath, partitionPath, parser, normalRoute);
                        dealFileV3(brfsPath, partitionPath, oldParser, parser, localSecond);
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

        // 当本机没有storageregion的目录，则等待10s 保证任务下的各个节点均注册
        if (snDirNonExistNum.get() == localPartitionInfos.size()) {
            log.info("normal finish task because of snDirNonExistNum equal localPartitionInfos size");
            try {
                Thread.sleep(10000);
            } catch (InterruptedException ingore) {
                // 忽略异常
            }
        }
        finishTask();
    }

    public void finishTask() {
        // 没有取消任务
        if (!cache.getStatus().get().equals(TaskStatus.CANCEL)) {
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
     * @param brfsPath      本机文件块信息
     * @param partitionPath 本机磁盘id根目录
     * @param parser        路由规则解析
     * @param normal        由changgeSummary转化来的路由规则
     */
    private void dealFileV2(BRFSPath brfsPath, String partitionPath, BlockAnalyzer parser, NormalRouteInterface normal) {

        // 1.由路由规则解析出来的服务
        String[] analysisSecondIds = parser.searchVaildIds(brfsPath.getFileName());
        List<String> validSecondIds = new ArrayList<>();
        // 2.检查元素，若存在虚拟id，则不进行恢复
        for (String id : analysisSecondIds) {
            if (BlockAnalyzer.isVirtualID(id)) {
                log.warn("current id is virtual id, {}, will return {}", brfsPath.getFileName(),
                        Arrays.asList(analysisSecondIds));
                return;
            }
            validSecondIds.add(id);
        }

        List<String> aliveMultiIds = getAliveMultiIds();
        log.debug("analysis second ids from route:{}, valid second ids:{}, aliveMultiIds:{}", analysisSecondIds,
                validSecondIds,
                aliveMultiIds);

        // 3.收集已经不可用的服务集合，若集合为空，则文件不需要恢复
        List<String> deadSecondIds =
                validSecondIds.stream().filter(x -> !aliveMultiIds.contains(x)).collect(Collectors.toList());
        if (deadSecondIds.isEmpty()) {
            log.warn("dead second ids is empty!");
            return;
        }
        // 4.解析文件名 用于当前需要发布的路由规则
        Pair<String, List<String>> fileInfoPair = BlockAnalyzer.analyzingFileName(brfsPath.getFileName());
        int fileCode = BlockAnalyzer.sumName(fileInfoPair.getFirst());
        List<String> excludes = fileInfoPair.getSecond();
        excludes.addAll(idManager.getSecondIds(idManager.getFirstSever(), balanceSummary.getStorageIndex()));
        // 排除本机二级serverId

        log.debug("dead second ids:{}", deadSecondIds);
        // 5.遍历不可用的服务
        for (String deadServer : deadSecondIds) {
            log.debug("deadServer: {}", deadServer);
            int pot = validSecondIds.indexOf(deadServer) + 1;
            // 5-1.若发现不可用的secondid，则可能发生新的变更，将由下次任务执行，本次不进行操作
            if (!deadServer.equals(normal.getBaseSecondId())) {
                log.warn("recovery find unable file:[{}],analysis:{} second {} route:[{}]", brfsPath.getFileName(),
                        validSecondIds, deadServer, normal);
                continue;
            }
            // 5-2.根据预发布的路由规则进行解析，
            String selectMultiId = normal.locateNormalServer(fileCode, excludes);

            log.debug("alive multiIds:{}, normal route:{}", aliveMultiIds, normal);

            // 5-3.判断选取的新节点是否存活
            if (isAlive(aliveMultiIds, selectMultiId)) {
                String secondServerIDSelected =
                        idManager.getSecondId(balanceSummary.getPartitionId(), balanceSummary.getStorageIndex());
                // 5-4.判断要恢复的secondId 与选中的secondid是否一致，一致，则表明该节点已经启动，则不做处理，不一致则需要作恢复
                log.debug("select second server id:{}, select multi id:{}", secondServerIDSelected, selectMultiId);

                if (!secondServerIDSelected.equals(selectMultiId)) {
                    String firstID = idManager.getFirstId(selectMultiId, balanceSummary.getStorageIndex());
                    FileRecoverMeta fileMeta =
                            new FileRecoverMeta(partitionPath + File.separator + brfsPath.toString(), brfsPath.getFileName(),
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
     * @param brfsPath      本机文件块信息
     * @param partitionPath 本机磁盘id根目录
     * @param parser        路由规则解析
     */
    private void dealFileV3(BRFSPath brfsPath,
                            String partitionPath,
                            BlockAnalyzer oldParser,
                            BlockAnalyzer parser,
                            String localSecond) {
        // 1.由路由规则解析出来的服务
        String fileName = brfsPath.getFileName();
        // 2. 通过未添加任务路由规则协议解析文件块
        String[] oldServices = oldParser.searchVaildIds(fileName);
        // 2-1 根据之前的路由判断文件块是不是脏数据，若是脏数据则不进行恢复工作
        List<String> oldRouteService = (List<String>) Arrays.asList(oldServices);
        if (!oldRouteService.contains(localSecond)) {
            log.info("发现脏数据 {}，文件应该存在的节点[{}]，当前节点{} 跳过恢复", brfsPath.toString(), oldRouteService, localSecond);
            return;
        }
        // 3. 通过任务路由规则协议解析文件块
        String[] newServices = parser.searchVaildIds(fileName);
        List<String> newServicesList = Arrays.asList(newServices);
        boolean allMatch = true;
        for (int i = 0; i < newServices.length; i++) {
            allMatch = allMatch && oldServices[i].equals(newServices[i]);
        }
        // 4.若新的解析与旧解析的文件块顺序及内容完全一样则说明副本数是完整的不需要迁移
        if (allMatch) {
            log.info("文件 {} 不需要恢复", brfsPath.toString());
            return;
        }
        // 5. 本文件块在新旧协议中的位置，若存在不一致，则copy一份到对应的目标目录下，---再确认
        int index = newServicesList.indexOf(localSecond);
        if (index != Integer.parseInt(brfsPath.getIndex())) {
            copy(partitionPath, brfsPath, index + "");
        }

        // 6. 判定要迁移到的服务及远程路径
        // 6-1 获取存活的secondId
        List<String> aliveMultiIds = getAliveMultiIds();
        int storageIndex = balanceSummary.getStorageIndex();

        for (int i = 0; i < newServices.length; i++) {
            String newSecondId = newServices[i];
            // 6-2 若二级serverid与本节点二级serverid一致跳过
            if (localSecond.equals(newSecondId)) {
                continue;
            }
            // 6-3 若二级serverid 存活
            if (isAlive(aliveMultiIds, newSecondId)) {
                // 6-3-1 获取选中的二级serverid的磁盘id
                String partitionId = idManager.getPartitionId(newSecondId, storageIndex);
                // 6-3-2 获取选中的二级serverid的一级serverid
                String firstId = idManager.getFirstId(newSecondId, storageIndex);
                // 6-3-3 生成remote host的文件相对路径。
                FileRecoverMeta fileMeta =
                        new FileRecoverMeta(partitionPath + File.separator + brfsPath.toString(), fileName,
                                newSecondId, getTimeDir(brfsPath), Integer.parseInt(brfsPath.getIndex()), i + 1,
                                firstId, partitionPath);
                try {
                    fileRecoverQueue.put(fileMeta);
                } catch (InterruptedException e) {
                    log.error("put file [{}] err", fileMeta, e);
                }
            }
        }


    }

    /**
     * 本文件块在新旧协议中的位置，若存在不一致，则copy一份到对应的目标目录下
     */
    private void copy(String partitionPath, BRFSPath brfsPath, String index) {
        String oldPath = partitionPath + File.separator + brfsPath.toString();
        brfsPath.setIndex(index);
        String newPath = partitionPath + File.separator + brfsPath.toString();
        log.info("file placement is different between old route protocol and new route protocol, "
                        + "new path is [{}], old path is [{}]",
                oldPath, newPath);
        try {
            org.apache.commons.io.FileUtils.copyFile(new File(oldPath), new File(newPath));
        } catch (IOException e) {
            log.error("copy file failed because of [{}].", e);
        }
    }

    /**
     * 获取BRFS文件块的时间目录
     *
     * @param brfsPath
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
                    FileRecoverMeta fileRecover = null;
                    while (fileRecover != null || !overFlag) {
                        log.info("current task status:{}", cache.getStatus());
                        if (cache.getStatus().get().equals(TaskStatus.CANCEL)) {
                            break;
                        } else if (cache.getStatus().get().equals(TaskStatus.PAUSE)) {
                            log.info("task pause!!!");
                            Thread.sleep(1000);
                        } else if (cache.getStatus().get().equals(TaskStatus.RUNNING)) {
                            fileRecover = fileRecoverQueue.poll(1, TimeUnit.SECONDS);
                            log.info("fileRecover:{}", fileRecover);
                            if (fileRecover != null) {
                                String localDir = storageRegion.getName() + FileUtils.FILE_SEPARATOR + fileRecover
                                        .getReplica() + FileUtils.FILE_SEPARATOR + fileRecover.getTime();
                                String remoteDir = storageRegion.getName() + FileUtils.FILE_SEPARATOR + fileRecover
                                        .getPot() + FileUtils.FILE_SEPARATOR + fileRecover.getTime();
                                String localFilePath = fileRecover.getPartitionPath() + FileUtils.FILE_SEPARATOR + localDir
                                        + FileUtils.FILE_SEPARATOR + fileRecover
                                        .getFileName();
                                boolean success;
                                int retryTimes = 0;
                                while (true) {
                                    if (cache.getStatus().get().equals(TaskStatus.PAUSE)) {
                                        log.info("task pause!!!");
                                        Thread.sleep(1000);
                                        continue;
                                    }
                                    if (cache.getStatus().get().equals(TaskStatus.CANCEL)) {
                                        break;
                                    }
                                    Service service = serviceManager.getServiceById(
                                            config.getDataNodeGroup(), fileRecover.getFirstServerID());
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
                                    } else {
                                        if (retryTimes < 3) {
                                            retryTimes++;
                                        } else {
                                            // 当文件超过重试次数传输失败时，取消任务
                                            balanceSummary.setTaskStatus(TaskStatus.CANCEL);
                                            String taskPath = ZKPaths
                                                    .makePath(taskNode, String.valueOf(balanceSummary.getStorageIndex()),
                                                            Constants.TASK_NODE);
                                            log.info("replica file transfer failed, will cancel the normal task [{}]", taskPath);

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
                                                log.error("clear normal task [{}] failed", taskPath, e);
                                            }
                                            break;
                                        }
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
                    log.error("consumer queue thread interrupted ", e);
                }
            }
        };
    }

    private List<String> getAliveMultiIds() {
        return balanceSummary.getAliveServer();
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
        try {
            if (curatorFramework.checkExists().forPath(node) != null) {
                curatorFramework.setData().forPath(node, JsonUtils.toJsonBytes(detail));
            }
        } catch (Exception e) {
            log.error("update task detail error!", e);
        }

    }

    /**
     * 概述：注册节点
     *
     * @param node
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public TaskDetail registerNodeDetail(String node) throws Exception {
        TaskDetail detail;
        if (curatorFramework.checkExists().forPath(node) == null) {
            detail = new TaskDetail(idManager.getFirstSever(), ExecutionStatus.INIT, 0, 0, 0);
            curatorFramework.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(node, JsonUtils.toJsonBytesQuietly(detail));
        } else {
            byte[] data = curatorFramework.getData().forPath(node);
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
            log.error("copy file {}/{} to {}:{} happen error", localPath, fileName, service.getHost(), remoteDir, e);
        }
        return success;
    }

}
