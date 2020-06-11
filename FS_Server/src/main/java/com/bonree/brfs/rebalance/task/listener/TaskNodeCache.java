package com.bonree.brfs.rebalance.task.listener;

import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.rebalance.task.BalanceTaskSummary;
import com.bonree.brfs.rebalance.task.TaskStatus;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaskNodeCache implements NodeCacheListener {
    private Logger log;
    private CuratorFramework client;
    private String taskNode;
    private BalanceTaskSummary currentTask;
    private AtomicReference<TaskStatus> status;

    public TaskNodeCache(BalanceTaskSummary currentTask, CuratorFramework client, String taskNode) {
        this.log = LoggerFactory.getLogger(currentTask.getTaskType().name());
        this.client = client;
        this.taskNode = taskNode;
        this.currentTask = currentTask;
        this.status = new AtomicReference<>(currentTask.getTaskStatus());
    }

    @Override
    public void nodeChanged() throws Exception {
        if (client.checkExists().forPath(taskNode) != null) {
            try {
                byte[] data = client.getData().forPath(taskNode);
                BalanceTaskSummary bts = JsonUtils.toObject(data, BalanceTaskSummary.class);
                String newID = bts.getId();
                String currentID = currentTask.getId();
                if (newID.equals(currentID)) { // 是同一个任务
                    TaskStatus stats = bts.getTaskStatus();
                    // 更新缓存
                    status.set(stats);
                    log.info("stats:" + stats);
                } else { // 不是同一个任务
                    log.info("newID:{} not match oldID:{}", newID, currentID);
                    log.info("cancel multi recover:{}", currentID);
                    status.set(TaskStatus.CANCEL);
                }
            } catch (Exception e) {
                log.error("get {} data happen error !! will cancle task", e);
                status.set(TaskStatus.CANCEL);
            }
        } else {
            log.info("task is deleted!!,this task will cancel!");
            status.set(TaskStatus.CANCEL);
        }
    }

    public AtomicReference<TaskStatus> getStatus() {
        return status;
    }
}
