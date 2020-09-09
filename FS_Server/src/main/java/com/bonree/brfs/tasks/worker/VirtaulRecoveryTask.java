package com.bonree.brfs.tasks.worker;

import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.rebalance.Constants;
import com.bonree.brfs.common.rebalance.TaskVersion;
import com.bonree.brfs.common.rebalance.route.VirtualRoute;
import com.bonree.brfs.common.resource.vo.LocalPartitionInfo;
import com.bonree.brfs.common.resource.vo.PartitionInfo;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.task.TaskState;
import com.bonree.brfs.common.task.TaskType;
import com.bonree.brfs.common.utils.BRFSPath;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.common.utils.Pair;
import com.bonree.brfs.common.utils.TimeUtils;
import com.bonree.brfs.duplication.storageregion.StorageRegion;
import com.bonree.brfs.duplication.storageregion.StorageRegionManager;
import com.bonree.brfs.identification.IDSManager;
import com.bonree.brfs.identification.impl.DiskDaemon;
import com.bonree.brfs.partition.DiskPartitionInfoManager;
import com.bonree.brfs.partition.LocalPartitionCache;
import com.bonree.brfs.rebalance.route.BlockAnalyzer;
import com.bonree.brfs.rebalance.route.RouteCache;
import com.bonree.brfs.schedulers.ManagerContralFactory;
import com.bonree.brfs.schedulers.task.manager.MetaTaskManagerInterface;
import com.bonree.brfs.schedulers.task.model.AtomTaskModel;
import com.bonree.brfs.schedulers.task.model.AtomTaskResultModel;
import com.bonree.brfs.schedulers.task.model.TaskModel;
import com.bonree.brfs.schedulers.task.model.TaskResultModel;
import com.bonree.brfs.schedulers.task.model.TaskServerNodeModel;
import com.bonree.brfs.tasks.monitor.RebalanceTaskMonitor;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VirtaulRecoveryTask implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(VirtaulRecoveryTask.class);
    private CuratorFramework client;
    private ServiceManager serviceManager;
    private StorageRegionManager regionManager;
    private IDSManager idsManager;
    private RouteCache routeCache;
    private RebalanceTaskMonitor taskMonitor;
    private MetaTaskManagerInterface taskMetaWork;
    private ZookeeperPaths zkPaths;
    private String currentTask;
    private String group;
    private DiskPartitionInfoManager localPartitionCache;
    private DiskDaemon partitionCache;
    private TaskType taskType = TaskType.VIRTUAL_ID_RECOVERY;

    public VirtaulRecoveryTask(String currentTask) {
        this.currentTask = currentTask;
        ManagerContralFactory mcf = ManagerContralFactory.getInstance();
        this.taskMetaWork = mcf.getTm();
        this.zkPaths = mcf.getZkPath();
        this.serviceManager = mcf.getSm();
        this.regionManager = mcf.getSnm();
        this.idsManager = mcf.getSim();
        this.routeCache = mcf.getRouteCache();
        this.taskMonitor = mcf.getTaskMonitor();
        this.group = mcf.getGroupName();
        this.partitionCache = mcf.getDaemon();
        this.localPartitionCache = mcf.getPartitionInfoManager();
        this.client = mcf.getClient();
    }

    @Override
    public void run() {
        // 1.副本迁移任务执行则任务不执行
        if (taskMonitor.isExecute()) {
            LOG.warn("rebalance task is running skip run {} {}", taskType, currentTask);
            return;
        }

        TaskServerNodeModel model =
            taskMetaWork.getTaskServerContentNodeInfo(taskType.name(), currentTask, idsManager.getFirstSever());
        if (model == null) {
            LOG.warn("rebalance task server node is null {} {} {}", taskType, currentTask, idsManager.getFirstSever());
            model = new TaskServerNodeModel();
        }
        try {
            String time = TimeUtils.formatTimeStamp(System.currentTimeMillis(), TimeUtils.TIME_MILES_FORMATE);
            model.setTaskStartTime(time);
            // 2.获取要执行的内容
            TaskModel task = taskMetaWork.getTaskContentNodeInfo(taskType.name(), currentTask);
            List<AtomTaskModel> atoms = task.getAtomList();
            // 2.获取执行的内容
            if (atoms == null || atoms.isEmpty()) {
                model.setTaskState(TaskState.FINISH.code());
                model.setTaskStopTime(time);
                taskMetaWork.updateServerTaskContentNode(idsManager.getFirstSever(), currentTask, taskType.name(), model);
                return;
            } else {
                model.setTaskState(TaskState.RUN.code());
                taskMetaWork.updateServerTaskContentNode(idsManager.getFirstSever(), currentTask, taskType.name(), model);
            }
            TaskResultModel resultModel = new TaskResultModel();
            for (AtomTaskModel atom : atoms) {
                if (taskMonitor.isExecute()) {
                    LOG.warn("cycle atom rebalance task is running skip run {} {}", taskType, currentTask);
                    return;
                }
                AtomTaskResultModel result = dealAtom(atom, taskMonitor);
                if (result == null) {
                    continue;
                }
                resultModel.setSuccess(resultModel.isSuccess() && result.isSuccess());
                resultModel.add(result);
            }
            model.setTaskState(resultModel.isSuccess() ? TaskState.FINISH.code() : TaskState.EXCEPTION.code());
            String stopTime = TimeUtils.formatTimeStamp(System.currentTimeMillis(), TimeUtils.TIME_MILES_FORMATE);
            model.setTaskStopTime(stopTime);
            model.setResult(resultModel);
        } catch (Exception e) {
            LOG.error("run task happen error", e);
            model.setTaskState(TaskState.EXCEPTION.code());
            model.setRetryCount(model.getRetryCount() + 1);
        }
        taskMetaWork.updateServerTaskContentNode(idsManager.getFirstSever(), currentTask, taskType.name(), model);
        String lockPath = ZKPaths.makePath(zkPaths.getBaseLocksPath(), currentTask);
        InterProcessMutex lock = new InterProcessMutex(client, lockPath);
        LOG.info("select lock {}", lockPath);
        try {
            lock.acquire();
            LOG.info("get lock {}", lockPath);
            // 更新TaskContent
            List<Pair<String, Integer>> serverStatus = taskMetaWork.getServerStatus(taskType.name(), currentTask);
            if (serverStatus == null || serverStatus.isEmpty()) {
                LOG.warn("status is null !!!");
                return;
            }
            int cstat;
            boolean isException = false;
            int finishCount = 0;
            int size = serverStatus.size();
            for (Pair<String, Integer> pair : serverStatus) {
                cstat = pair.getSecond();
                if (TaskState.EXCEPTION.code() == cstat) {
                    isException = true;
                    finishCount += 1;
                } else if (TaskState.FINISH.code() == cstat) {
                    finishCount += 1;
                }
            }
            if (finishCount != size) {
                return;
            }
            TaskModel task = taskMetaWork.getTaskContentNodeInfo(taskType.name(), currentTask);
            if (task == null) {
                LOG.warn("task is null !!! {} {} {}", taskType, currentTask);
                task = new TaskModel();
                task.setCreateTime(TimeUtils.formatTimeStamp(System.currentTimeMillis(), TimeUtils.TIME_MILES_FORMATE));
            }
            if (isException) {
                task.setTaskState(TaskState.EXCEPTION.code());
            } else {
                task.setTaskState(TaskState.FINISH.code());
            }
            taskMetaWork.updateTaskContentNode(task, taskType.name(), currentTask);
            LOG.info("complete task :{} - {} - {}", taskType, currentTask, TaskState.valueOf(task.getTaskState()).name());
        } catch (Exception e) {
            LOG.error("run {} happen error", currentTask, e);
        } finally {
            try {
                if (lock.isAcquiredInThisProcess()) {
                    lock.release();
                }
                if (client.checkExists().forPath(lockPath) != null) {
                    client.delete().deletingChildrenIfNeeded().forPath(lockPath);
                }
            } catch (Exception e) {
                LOG.error("release lock happen error ", e);
            }
            LOG.info("release lockpath {}", lockPath);
        }
    }

    public AtomTaskResultModel dealAtom(AtomTaskModel model, RebalanceTaskMonitor taskMonitor) throws Exception {
        AtomTaskResultModel result = new AtomTaskResultModel();
        result.setSn(model.getStorageName());
        StorageRegion region = regionManager.findStorageRegionByName(model.getStorageName());
        String virtual = model.getTaskOperation();
        // 0. 判断自己是否可以执行虚拟serverid恢复任务，
        if (idsManager.hasVirtual(idsManager.getFirstSever(), virtual, region.getId())) {
            result.setMessage("NO");
            result.setOperationFileCount(0);
            result.setSuccess(true);
            LOG.info("storageregion : {} virtual {} firstServer {} not execute", region.getName(), virtual,
                     idsManager.getFirstSever());
            return result;
        }
        String lockPath = ZKPaths.makePath(zkPaths.getBaseLocksPath(), virtual);
        InterProcessMutex lock = new InterProcessMutex(client, lockPath);
        try {
            lock.acquire();
            // 1 虚拟server是否已经发布，发布则不进行任务操作
            BlockAnalyzer blockAnalyzer = routeCache.getBlockAnalyzer(region.getId());
            if (blockAnalyzer.isRoute(virtual)) {
                result.setMessage("NO");
                result.setOperationFileCount(0);
                result.setSuccess(true);
                LOG.info("storageregion : {} virtual {} is deal by rebalance skip", region.getName(), virtual);
                return result;
            }
            List<Service> services = serviceManager.getServiceListByGroup(group);
            String partitionId = getMaxFree();
            LocalPartitionInfo local = null;
            if (partitionId == null) {
                local = partitionCache.getPartitions().stream().findFirst().get();
            } else {
                local = partitionCache.getPartition(partitionId);
            }
            String uuid = UUID.randomUUID().toString();
            String changeID = System.currentTimeMillis() / 1000 + uuid;
            String seconid = idsManager.getSecondId(local.getPartitionId(), region.getId());
            VirtualRoute route = new VirtualRoute(changeID, region.getId(), virtual, seconid, TaskVersion.V2);
            int count = 0;
            for (Service server : services) {
                RemoteFileWorker worker = new RemoteFileWorker(server, idsManager.getSecondMaintainer(), routeCache);
                Collection<BRFSPath> files = worker.listFiles(region);
                if (files.isEmpty()) {
                    continue;
                }
                Map<BRFSPath, BRFSPath> virtualMap = new HashMap<>();
                for (BRFSPath file : files) {
                    List<String> field = Arrays.asList(StringUtils.split(file.getFileName(), "_"));
                    int index = field.indexOf(virtual);
                    if (index < 0) {
                        continue;
                    }
                    BRFSPath copy = file.copy();
                    copy.setIndex(String.valueOf(index));
                    virtualMap.put(file, copy);
                    count++;
                }
                if (virtualMap.isEmpty()) {
                    continue;
                }
                if (taskMonitor.isExecute()) {
                    return null;
                }
                worker.downloadFiles(virtualMap, local.getDataDir(), 100);
                // 当任务执行完后，对虚拟serverid进行 二次检查 若已经发布则取消现有路由规则的发布
                blockAnalyzer = routeCache.getBlockAnalyzer(region.getId());
                if (blockAnalyzer.isRoute(virtual)) {
                    result.setMessage("NO");
                    result.setOperationFileCount(0);
                    result.setSuccess(true);
                    LOG.info("storageregion : {} virtual {} is deal by rebalance skip", region.getName(), virtual);
                    return result;
                }
            }
            if (taskMonitor.isExecute()) {
                return null;
            }
            String path = ZKPaths.makePath(zkPaths.getBaseV2RoutePath(), Constants.VIRTUAL_ROUTE, region.getId() + "", uuid);
            client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(path, JsonUtils.toJsonBytes(route));
            idsManager.invalidVirtualId(region.getId(), virtual);
            idsManager.deleteVirtualId(region.getId(), virtual);
            result.setMessage("YES");
            result.setOperationFileCount(count);
            result.setSuccess(true);
            return result;
        } catch (Exception e) {
            throw e;
        } finally {
            try {
                if (lock.isAcquiredInThisProcess()) {
                    lock.release();
                }
                if (client.checkExists().forPath(lockPath) != null) {
                    client.delete().deletingChildrenIfNeeded().forPath(lockPath);
                }
            } catch (Exception e) {
                LOG.error("release lock happen error ", e);
            }
        }

    }

    private String getMaxFree() {
        Map<String, PartitionInfo> partitionInfos = localPartitionCache.getPartitionInfosByServiceId(idsManager.getFirstSever());
        PartitionInfo max = null;
        for (PartitionInfo partition : partitionInfos.values()) {
            if (max == null) {
                max = partition;
                continue;
            }
            if (max.getFreeSize() < partition.getFreeSize()) {
                max = partition;
            }
        }
        return max == null ? null : max.getPartitionId();
    }
}
