package com.bonree.brfs.rebalance.task;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.common.zookeeper.curator.cache.CuratorCacheFactory;
import com.bonree.brfs.common.zookeeper.curator.cache.CuratorTreeCache;
import com.bonree.brfs.configuration.Configuration;
import com.bonree.brfs.configuration.Configuration.ConfigException;
import com.bonree.brfs.configuration.ServerConfig;
import com.bonree.brfs.rebalance.Constants;
import com.bonree.brfs.rebalance.DataRecover;
import com.bonree.brfs.rebalance.DataRecover.RecoverType;
import com.bonree.brfs.rebalance.recover.VirtualRecover;
import com.bonree.brfs.rebalance.task.listener.TaskExecutorListener;
import com.bonree.brfs.server.identification.ServerIDManager;

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

    private CuratorClient client;
    private ServerIDManager idManager;
    private CuratorTreeCache treeCache;
    private String tasksPath;

    public TaskOperation(final CuratorClient client, final String baseBalancePath, ServerIDManager idManager) {
        this.client = client;
        this.idManager = idManager;
        this.tasksPath = baseBalancePath + Constants.SEPARATOR + Constants.TASKS_NODE;
        treeCache = CuratorCacheFactory.getTreeCache();
    }

    public void start() {
        LOG.info("add tree cache:" + tasksPath);
        treeCache.addListener(tasksPath, new TaskExecutorListener("task_executor", this));
    }

    public void launchDelayTaskExecutor(BalanceTaskSummary taskSummary, String taskPath) {
        DataRecover recover = null;
        long delayTime = 30l;
        List<String> multiIds = taskSummary.getOutputServers();
        System.out.println("output :" + multiIds);
        System.out.println(idManager.getSecondServerID(taskSummary.getStorageIndex()));
        System.out.println("task type:" + taskSummary.getTaskType());
        if (multiIds.contains(idManager.getSecondServerID(taskSummary.getStorageIndex()))) {
            // 注册自身的selfMultiId,并设置为created阶段
            if (taskSummary.getTaskType() == RecoverType.NORMAL) { // 正常迁移任务
                // recover = new MultiRecover(taskSummary, idManager, node, client);
                // delayTime = taskSummary.getRuntime();
            } else if (taskSummary.getTaskType() == RecoverType.VIRTUAL) { // 虚拟迁移任务
                recover = new VirtualRecover(taskSummary, taskPath, client, idManager);
                delayTime = taskSummary.getRuntime();
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
    private void launchTask(long delay, final DataRecover recover) {
        // TODO
        new Thread() {
            @Override
            public void run() {
                // TODO 这边需要和成岗进行沟通
                System.out.println("10s后启动！！！");
                try {
                    Thread.sleep(delay * 1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                recover.recover();
            }
        }.start();
    }

    @Override
    public void close() throws IOException {

    }

    public static final String CONFIG_NAME1 = "E:/BRFS1/config/server.properties";
    public static final String HOME1 = "E:/BRFS1";

    public static void main(String[] args) throws InterruptedException, IOException, ConfigException {
        Configuration conf = Configuration.getInstance();
        conf.parse(CONFIG_NAME1);
        conf.printConfigDetail();
        ServerConfig serverConfig = ServerConfig.parse(conf, HOME1);
        CuratorCacheFactory.init(serverConfig.getZkHosts());
        ZookeeperPaths zookeeperPaths = ZookeeperPaths.create(serverConfig.getClusterName(), serverConfig.getZkHosts());
        ServerIDManager idManager = new ServerIDManager(serverConfig, zookeeperPaths);
        CuratorClient client = CuratorClient.getClientInstance(serverConfig.getZkHosts(), 500, 500);
        TaskOperation opt = new TaskOperation(client, zookeeperPaths.getBaseRebalancePath(), idManager);
        CuratorTreeCache cache = CuratorCacheFactory.getTreeCache();
        cache.addListener(zookeeperPaths.getBaseRebalancePath() + Constants.SEPARATOR + Constants.TASKS_NODE, new TaskExecutorListener("aaa", opt));
        Thread.sleep(Long.MAX_VALUE);
    }

}
