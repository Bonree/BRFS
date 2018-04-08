package com.bonree.brfs.rebalance.task;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.common.zookeeper.curator.cache.AbstractTreeCacheListener;
import com.bonree.brfs.common.zookeeper.curator.cache.CuratorTreeCache;
import com.bonree.brfs.rebalance.Constants;
import com.bonree.brfs.rebalance.DataRecover;
import com.bonree.brfs.rebalance.recover.MultiRecover;
import com.bonree.brfs.rebalance.recover.VirtualRecover;
import com.bonree.brfs.server.ServerInfo;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年3月30日 下午3:11:15
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description: 任务执行节点
 ******************************************************************************/
public class TaskOperation implements Closeable {

    private final static Logger LOG = LoggerFactory.getLogger(TaskOperation.class);

    private ServerInfo selfServer;
    private CuratorClient client;
    private CuratorTreeCache treeCache;
    private String tasksPath;

    public TaskOperation() {
        client = CuratorClient.getClientInstance(Constants.zkUrl);
        tasksPath = Constants.PATH_TASKS;
    }

    public void start() throws InterruptedException {
        client.blockUntilConnected();
        treeCache = CuratorTreeCache.getTreeCacheInstance(Constants.zkUrl);
        treeCache.addListener(tasksPath, new TaskExecutorListener("task_executor"));
        treeCache.startPathCache(tasksPath);
    }

    class TaskExecutorListener extends AbstractTreeCacheListener {

        public TaskExecutorListener(String listenName) {
            super(listenName);
        }

        @Override
        public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
            System.out.println("event info:" + event);
            LOG.info("event info:" + event);
            // 此处只捕捉NODE_ADDED时间
            if (event.getType() == Type.NODE_ADDED) {
                // 是否为任务类型节点
                String path = event.getData().getPath();
                System.out.println(path);
                String taskStr = StringUtils.substring(path, path.lastIndexOf('/') + 1, path.length());
                if (StringUtils.equals(taskStr, Constants.TASK_NODE)) {
                    // 标识为一个任务节点
                    if (event.getData() != null && event.getData().getData() != null) {
                        byte[] data = event.getData().getData();
                        BalanceTaskSummary taskSummary = JSON.parseObject(data, BalanceTaskSummary.class);
                        String basePath = event.getData().getPath();
                        launchDelayTaskExecutor(taskSummary, basePath);
                    }
                }
            }

        }
    }

    public void launchDelayTaskExecutor(BalanceTaskSummary taskSummary, String path) {
        DataRecover recover = null;
        long delayTime = 60l;
        List<String> multiIds = taskSummary.getOutputServers();
        if (multiIds.contains(selfServer.getMultiIdentification())) {
            // 注册自身的selfMultiId,并设置为created阶段
            String node = path + Constants.SEPARATOR + selfServer.getMultiIdentification();

            if (taskSummary.getTaskType() == 1) { // 正常迁移任务
                recover = new MultiRecover(taskSummary, selfServer, this);
                delayTime = taskSummary.getRuntime();
            } else if (taskSummary.getTaskType() == 2) { // 虚拟迁移任务
                recover = new VirtualRecover(taskSummary);
                delayTime = taskSummary.getRuntime();
            }

            if (!client.checkExists(node)) {
                client.createPersistent(path, false, DataRecover.CREATE_STAGE.getBytes());
            }
            // 调用成岗的任务创建模块
            launchTask(delayTime, recover);
        }
    }

    /** 概述：生成一个具有延时的任务
     * @param delay
     * @param recover
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    private void launchTask(long delay, DataRecover recover) {
        // TODO 这边需要和成岗进行沟通
    }

    /** 概述：设置任务状态为运行中
     * @param node
     * @param status
     * @user <a href=mailto:weizheng@bonree.com>魏征</a>
     */
    public void setTaskStatus(String node, String status) {
        if (client.checkExists(node)) {
            try {
                client.setData(node, status.getBytes());
            } catch (Exception e) {
                LOG.error("change Task status error!", e);
            }
        }
    }

    @Override
    public void close() throws IOException {

    }

    public static void main(String[] args) throws InterruptedException, IOException {
        TaskOperation opt = new TaskOperation();
        opt.start();
        Thread.sleep(Long.MAX_VALUE);
        opt.close();
    }

}
