package com.bonree.brfs.rebalance.task;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;

import com.alibaba.fastjson.JSON;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.common.zookeeper.curator.cache.AbstractTreeCacheListener;
import com.bonree.brfs.common.zookeeper.curator.cache.CuratorTreeCache;
import com.bonree.brfs.rebalance.Constants;
import com.bonree.brfs.rebalance.DataRecover;
import com.bonree.brfs.rebalance.recover.MultiRecover;
import com.bonree.brfs.rebalance.recover.VirtualRecover;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年3月30日 下午3:11:15
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description: 任务执行节点
 ******************************************************************************/
public class TaskExecutor {

    private CuratorClient client;
    private CuratorTreeCache treeCache;

    public void start() {
        treeCache = CuratorTreeCache.getTreeCacheInstance(Constants.zkUrl);
        treeCache.addListener(Constants.PATH_TASKS, new TaskExecutorListener("task_executor"));
        treeCache.startPathCache(Constants.PATH_TASKS);
    }

    /*
     * 监听器实现
     */
    class TaskExecutorListener extends AbstractTreeCacheListener {

        public TaskExecutorListener(String listenName) {
            super(listenName);
        }

        @Override
        public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
            if (event.getData() != null && event.getData().getData() != null) {
                byte[] data = event.getData().getData();
                BalanceTaskSummary taskSummary = JSON.parseObject(data, BalanceTaskSummary.class);
                launchDelayExecutorTask(taskSummary);
            }
        }

        public void launchDelayExecutorTask(BalanceTaskSummary taskSummary) {
            DataRecover recover = null;
            long delayTime = 60l;

            if (taskSummary.getTaskType() == 1) { // 正常迁移任务
                recover = new MultiRecover(taskSummary);
                delayTime = taskSummary.getRuntime();
            } else if (taskSummary.getTaskType() == 2) { // 虚拟迁移任务
                recover = new VirtualRecover(taskSummary);
                delayTime = taskSummary.getRuntime();
            }

            // 调用成岗的任务执行模块
        }

    }

}
