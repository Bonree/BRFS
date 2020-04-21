package com.bonree.brfs.rebalanceV2.task;

import com.bonree.brfs.common.rebalance.Constants;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.common.utils.PooledThreadFactory;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.common.zookeeper.curator.cache.CuratorCacheFactory;
import com.bonree.brfs.common.zookeeper.curator.cache.CuratorTreeCache;
import com.bonree.brfs.duplication.storageregion.StorageRegion;
import com.bonree.brfs.duplication.storageregion.StorageRegionManager;
import com.bonree.brfs.identification.IDSManager;
import com.bonree.brfs.identification.LocalPartitionInterface;
import com.bonree.brfs.rebalance.DataRecover;
import com.bonree.brfs.rebalance.DataRecover.RecoverType;
import com.bonree.brfs.rebalance.task.TaskStatus;
import com.bonree.brfs.rebalanceV2.recover.MultiRecoverV2;
import com.bonree.brfs.rebalanceV2.recover.VirtualRecoverV2;
import com.bonree.brfs.rebalanceV2.task.listener.TaskExecutorListenerV2;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @date 2018年3月30日 下午3:11:15
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description: 任务执行节点
 ******************************************************************************/
public class TaskOperationV2 implements Closeable {

    private final static Logger LOG = LoggerFactory.getLogger(TaskOperationV2.class);

    private CuratorClient client;
    private IDSManager idManager;
    private CuratorTreeCache treeCache;
    private String tasksPath;
    private StorageRegionManager snManager;
    private ServiceManager serviceManager;
    private String baseRoutesPath;
    private LocalPartitionInterface partitionInterface;
    private ExecutorService es = Executors.newFixedThreadPool(10, new PooledThreadFactory("task_executor"));

    public TaskOperationV2(final CuratorClient client, final String baseBalancePath, String baseRoutesPath, IDSManager idManager, StorageRegionManager snManager, ServiceManager serviceManager, LocalPartitionInterface partitionInterface) {
        this.client = client;
        this.idManager = idManager;
        this.tasksPath = ZKPaths.makePath(baseBalancePath, Constants.TASKS_NODE);
        this.baseRoutesPath = baseRoutesPath;
        treeCache = CuratorCacheFactory.getTreeCache();
        this.snManager = snManager;
        this.serviceManager = serviceManager;
        this.partitionInterface = partitionInterface;
    }

    public void start() {
        LOG.info("add tree cache for path: {}", tasksPath);
        treeCache.addListener(tasksPath, new TaskExecutorListenerV2(this));
    }

    public void launchDelayTaskExecutor(BalanceTaskSummaryV2 taskSummary, String taskPath) {
        DataRecover recover = null;
        List<String> multiIds = taskSummary.getOutputServers();  // 二级serverId集合
        Collection<String> currentSecondIds = idManager.getSecondIds(idManager.getFirstSever(), taskSummary.getStorageIndex());  // 获取本机二级serverId集合
        LOG.info("multiIds :{}, currentSecondIds :{}", multiIds, currentSecondIds);
        boolean contain = multiIds.stream().anyMatch(currentSecondIds::contains);

        if (contain) {
            // 注册自身的selfMultiId,并设置为created阶段
            if (taskSummary.getTaskType() == RecoverType.NORMAL) { // 正常迁移任务
                LOG.info("current storage region list: {}", snManager.getStorageRegionList());
                StorageRegion node = snManager.findStorageRegionById(taskSummary.getStorageIndex());
                if (node == null) {
                    LOG.error("无法开启对" + taskSummary.getStorageIndex() + "的任务");
                    return;
                }
                String storageName = snManager.findStorageRegionById(taskSummary.getStorageIndex()).getName();
                recover = new MultiRecoverV2(partitionInterface, taskSummary, idManager, serviceManager, taskPath, client, storageName, baseRoutesPath);
            } else if (taskSummary.getTaskType() == RecoverType.VIRTUAL) { // 虚拟迁移任务
                StorageRegion node = snManager.findStorageRegionById(taskSummary.getStorageIndex());
                if (node == null) {
                    LOG.error("无法开启对" + taskSummary.getStorageIndex() + "的任务");
                    return;
                }
                String storageName = snManager.findStorageRegionById(taskSummary.getStorageIndex()).getName();
                recover = new VirtualRecoverV2(client, taskSummary, taskPath, storageName, idManager, serviceManager, partitionInterface);
            }

            updateTaskStatus(taskSummary, TaskStatus.RUNNING);
            launchTask(recover);
        }
    }

    public void updateTaskStatus(BalanceTaskSummaryV2 task, TaskStatus status) {
        task.setTaskStatus(status);
        String taskNode = ZKPaths.makePath(tasksPath, String.valueOf(task.getStorageIndex()), Constants.TASK_NODE);
        client.setData(taskNode, JsonUtils.toJsonBytesQuietly(task));
    }

    /**
     * 概述：生成一个具有延时的任务
     *
     * @param recover
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    private void launchTask(final DataRecover recover) {
        es.execute(new Runnable() {
            @Override
            public void run() {
                recover.recover();
            }
        });
    }

    @Override
    public void close() throws IOException {
        es.shutdown();
        treeCache.cancelListener(tasksPath);
    }
}
