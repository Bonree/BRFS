package com.bonree.brfs.rebalance.task.listener;

import com.bonree.brfs.common.rebalance.Constants;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.rebalance.task.BalanceTaskSummary;
import com.bonree.brfs.rebalance.task.DiskPartitionChangeSummary;
import com.bonree.brfs.rebalance.task.TaskOperation;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent.Type;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaskExecutorListener implements TreeCacheListener {

    private static final Logger LOG = LoggerFactory.getLogger(TaskExecutorListener.class);

    private TaskOperation opt;

    public TaskExecutorListener(TaskOperation opt) {
        this.opt = opt;
    }

    @Override
    public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
        LOG.info("get task from {}", event.getType());
        // 此处只捕捉NODE_ADDED事件
        if (event.getType() == Type.NODE_ADDED) {
            // 是否为任务类型节点
            String path = event.getData().getPath();
            String taskStr = StringUtils.substring(path, path.lastIndexOf('/') + 1, path.length());
            if (StringUtils.equals(taskStr, Constants.TASK_NODE)) {
                // 标识为一个任务节点
                if (event.getData() != null && event.getData().getData() != null && event.getData().getData().length > 0) {
                    byte[] data = event.getData().getData();
                    BalanceTaskSummary taskSummary = JsonUtils.toObjectQuietly(data, BalanceTaskSummary.class);
                    LOG.info("deal task:" + taskSummary);
                    String taskPath = event.getData().getPath();
                    if (taskSummary.getVersion() == null) {
                        client.delete().deletingChildrenIfNeeded().forPath(taskPath);
                        LOG.warn("find v1 invalid task remove it {}", taskPath);
                        return;
                    }
                    if (taskSummary != null) {
                        opt.launchDelayTaskExecutor(taskSummary, taskPath);
                    }
                }
            }
        }

    }

}
