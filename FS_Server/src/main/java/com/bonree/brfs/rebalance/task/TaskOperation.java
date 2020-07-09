package com.bonree.brfs.rebalance.task;

import com.bonree.brfs.common.rebalance.Constants;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.common.utils.PooledThreadFactory;
import com.bonree.brfs.common.zookeeper.curator.cache.CuratorCacheFactory;
import com.bonree.brfs.common.zookeeper.curator.cache.CuratorTreeCache;
import com.bonree.brfs.duplication.storageregion.StorageRegion;
import com.bonree.brfs.duplication.storageregion.StorageRegionManager;
import com.bonree.brfs.guice.ClusterConfig;
import com.bonree.brfs.identification.IDSManager;
import com.bonree.brfs.identification.LocalPartitionInterface;
import com.bonree.brfs.rebalance.DataRecover;
import com.bonree.brfs.rebalance.DataRecover.RecoverType;
import com.bonree.brfs.rebalance.recover.MultiRecover;
import com.bonree.brfs.rebalance.recover.VirtualRecover;
import com.bonree.brfs.rebalance.route.RouteCache;
import com.bonree.brfs.rebalance.task.listener.TaskExecutorListener;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @date 2018年3月30日 下午3:11:15
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description: 任务执行节点
 ******************************************************************************/
public class TaskOperation implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(TaskOperation.class);

    private CuratorFramework curatorFramework;
    private IDSManager idManager;
    private CuratorTreeCache treeCache;
    private String tasksPath;
    private StorageRegionManager snManager;
    private ServiceManager serviceManager;
    private LocalPartitionInterface partitionInterface;
    private RouteCache routeCache;
    private String baseBalancePath;
    private ClusterConfig config;
    private ExecutorService es = Executors.newFixedThreadPool(10, new PooledThreadFactory("task_executor"));

    public TaskOperation(CuratorFramework curatorFramework, final String baseBalancePath,
                         ClusterConfig config, IDSManager idManager,
                         StorageRegionManager snManager, ServiceManager serviceManager,
                         LocalPartitionInterface partitionInterface, RouteCache routeCache) {
        this.config = config;
        this.curatorFramework = curatorFramework;
        this.idManager = idManager;
        this.baseBalancePath = baseBalancePath;
        this.tasksPath = ZKPaths.makePath(baseBalancePath, Constants.TASKS_NODE);
        treeCache = CuratorCacheFactory.getTreeCache();
        this.snManager = snManager;
        this.serviceManager = serviceManager;
        this.partitionInterface = partitionInterface;
        this.routeCache = routeCache;
    }

    public void start() {
        LOG.info("add tree cache for path: {}", tasksPath);
        treeCache.addListener(tasksPath, new TaskExecutorListener(this));
    }

    public void launchDelayTaskExecutor(BalanceTaskSummary taskSummary, String taskPath) throws Exception {
        DataRecover recover = null;
        List<String> multiIds = taskSummary.getOutputServers();  // 二级serverId集合
        Collection<String> currentSecondIds =
            idManager.getSecondIds(idManager.getFirstSever(), taskSummary.getStorageIndex());  // 获取本机二级serverId集合
        LOG.info("multiIds :{}, currentSecondIds :{}", multiIds, currentSecondIds);
        boolean contain = multiIds.stream().anyMatch(currentSecondIds::contains);

        if (contain) {
            // 注册自身的selfMultiId,并设置为created阶段
            if (taskSummary.getTaskType() == RecoverType.NORMAL) { // 正常迁移任务
                LOG.info("current storage region list: {}", snManager.getStorageRegionList());
                StorageRegion node = getStorageRegion(snManager, taskSummary.getStorageIndex(), 5, 1000);
                if (node == null) {
                    LOG.error("无法开启对" + taskSummary.getStorageIndex() + "的任务");
                    return;
                }
                recover = new MultiRecover(config, partitionInterface, routeCache, taskSummary,
                                           idManager, serviceManager, taskPath,
                                           curatorFramework, node, baseBalancePath);

            } else if (taskSummary.getTaskType() == RecoverType.VIRTUAL) { // 虚拟迁移任务
                StorageRegion node = getStorageRegion(snManager, taskSummary.getStorageIndex(), 5, 1000);
                if (node == null) {
                    LOG.error("run storage {}  virtual task happen error ", taskSummary.getStorageIndex());
                    return;
                }
                String storageName = node.getName();
                recover =
                    new VirtualRecover(config, curatorFramework, taskSummary, taskPath, storageName, idManager, serviceManager,
                                       partitionInterface, baseBalancePath);
            }

            updateTaskStatus(taskSummary, TaskStatus.RUNNING);
            launchTask(recover);
        }
    }

    private StorageRegion getStorageRegion(StorageRegionManager storageRegionManager, int storageIndex, int count, long sleep) {
        int times = count;
        while (times > 0) {
            StorageRegion sr = storageRegionManager.findStorageRegionById(storageIndex);
            if (sr != null) {
                return sr;
            }
            try {
                Thread.sleep(sleep);
            } catch (InterruptedException ignore) {
                // ignore Exception
            }
        }
        return null;
    }

    public void updateTaskStatus(BalanceTaskSummary task, TaskStatus status) throws Exception {
        task.setTaskStatus(status);
        String taskNode = ZKPaths.makePath(tasksPath, String.valueOf(task.getStorageIndex()), Constants.TASK_NODE);
        curatorFramework.setData().forPath(taskNode, JsonUtils.toJsonBytesQuietly(task));

    }

    /**
     * 概述：生成一个具有延时的任务
     *
     * @param recover
     *
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    private void launchTask(final DataRecover recover) {
        es.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    recover.recover();
                } catch (Exception e) {
                    LOG.error("recover happen error", e);
                }
            }
        });
    }

    @Override
    public void close() throws IOException {
        es.shutdown();
        treeCache.cancelListener(tasksPath);
    }
}
