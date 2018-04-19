package com.bonree.brfs.rebalance.task.listener;

import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.bonree.brfs.common.zookeeper.curator.cache.AbstractTreeCacheListener;
import com.bonree.brfs.rebalance.Constants;
import com.bonree.brfs.rebalance.task.BalanceTaskSummary;
import com.bonree.brfs.rebalance.task.TaskOperation;

public class TaskExecutorListener extends AbstractTreeCacheListener {

    private final static Logger LOG = LoggerFactory.getLogger(TaskExecutorListener.class);

    private TaskOperation opt;

    public TaskExecutorListener(String listenName, TaskOperation opt) {
        super(listenName);
        this.opt = opt;
    }

    @Override
    public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
        System.out.println("触发TaskExecutorListener:"+event);
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
                    BalanceTaskSummary taskSummary = JSON.parseObject(data, BalanceTaskSummary.class);
                    System.out.println("deal task:" + taskSummary);
                    String taskPath = event.getData().getPath();
                    opt.launchDelayTaskExecutor(taskSummary, taskPath);
                }
            }
        }

    }

}
