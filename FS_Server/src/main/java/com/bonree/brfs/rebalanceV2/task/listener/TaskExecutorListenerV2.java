package com.bonree.brfs.rebalanceV2.task.listener;

import com.bonree.brfs.common.rebalance.Constants;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.rebalanceV2.task.BalanceTaskSummaryV2;
import com.bonree.brfs.rebalanceV2.task.TaskOperationV2;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent.Type;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaskExecutorListenerV2 implements TreeCacheListener {

    private final static Logger LOG = LoggerFactory.getLogger(TaskExecutorListenerV2.class);

    private TaskOperationV2 opt;

    public TaskExecutorListenerV2(TaskOperationV2 opt) {
        this.opt = opt;
    }

    @Override
    public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
        LOG.info("event info:" + event);
        // 此处只捕捉NODE_ADDED时间
        if (event.getType() == Type.NODE_ADDED) {
            // 是否为任务类型节点
            String path = event.getData().getPath();
            String taskStr = StringUtils.substring(path, path.lastIndexOf('/') + 1, path.length());
            if (StringUtils.equals(taskStr, Constants.TASK_NODE)) {
                // 标识为一个任务节点
                if (event.getData() != null && event.getData().getData() != null && event.getData().getData().length > 0) {
                    byte[] data = event.getData().getData();
                    BalanceTaskSummaryV2 taskSummary = JsonUtils.toObjectQuietly(data, BalanceTaskSummaryV2.class);
                    LOG.info("deal task:" + taskSummary);
                    String taskPath = event.getData().getPath();
                    if (taskSummary != null) {
                        opt.launchDelayTaskExecutor(taskSummary, taskPath);
                    }
                }
            }
        }

    }

}
