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
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaskExecutorListener implements TreeCacheListener {

    private static final Logger LOG = LoggerFactory.getLogger(TaskExecutorListener.class);

    private TaskOperation opt;
    private String taskQueuePath;
    private String taskHistoryQueuePath;

    public TaskExecutorListener(TaskOperation opt, String banalcePath) {
        this.opt = opt;
        this.taskHistoryQueuePath = ZKPaths.makePath(banalcePath, Constants.TASKS_HISTORY_NODE);
        this.taskQueuePath = ZKPaths.makePath(banalcePath, Constants.TASKS_NODE);
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

                    if (taskSummary != null) {
                        if (taskSummary.getVersion() == null) {
                            try {
                                client.delete().deletingChildrenIfNeeded().forPath(taskPath);
                                String parent = null;
                                if (taskPath.contains(Constants.TASK_NODE)) {
                                    parent = taskPath.substring(0, taskPath.indexOf(Constants.TASK_NODE));
                                } else {
                                    parent = taskPath;
                                }
                                String historyPath =
                                    StringUtils.replace(parent, taskQueuePath, taskHistoryQueuePath) + taskSummary.getId();

                                if (client.checkExists().forPath(historyPath) == null) {
                                    client.create()
                                          .creatingParentsIfNeeded()
                                          .withMode(CreateMode.PERSISTENT)
                                          .forPath(historyPath, data);
                                } else {
                                    client.setData().forPath(historyPath, data);
                                }
                                LOG.warn("find v1 invalid task remove it {}", taskPath);
                            } catch (Exception e) {
                                LOG.warn("remove invald v1 task happen error", e);
                            }
                            return;
                        }
                        opt.launchDelayTaskExecutor(taskSummary, taskPath);
                    } else {
                        client.delete().deletingChildrenIfNeeded().forPath(taskPath);
                        LOG.info("delete a empty task! task path is [{}]!", taskPath);
                    }
                }
            }
        }

    }

}
