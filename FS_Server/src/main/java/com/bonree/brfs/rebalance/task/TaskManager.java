package com.bonree.brfs.rebalance.task;

import java.util.Date;
import java.util.List;

import com.alibaba.fastjson.JSON;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.google.common.collect.Lists;

public class TaskManager {

    private static String zkUrl = "192.168.101.86:2181";

    private static String basePath = "/brfs/wz/rebalance";

    private static final String SEPARATOR = "/";

    private static final String taskQueue = "tasks";

    public static void main(String[] args) {
        publishTask();
    }

    public static void publishTask() {
        BalanceTaskSummary task = new BalanceTaskSummary();
        task.setServerId("server1");
        task.setStorageIndex(1);
        task.setOutputServers(Lists.asList("server2", new String[] { "server3", "server4" }));
        task.setInputServers(Lists.asList("server2", new String[] { "server3", "server4" }));
        task.setTaskType(1);
        task.setRuntime(new Date().getTime() / 1000);
        String jsonStr = JSON.toJSONString(task);
        CuratorClient client = CuratorClient.getClientInstance(zkUrl);
        if (!client.checkExists(basePath)) {
            client.createPersistent(basePath, true);
        }
        // client.createPersistentSequential(basePath+SEPARATOR+taskQueue, false,jsonStr.getBytes());
        // client.delete(basePath, true);
        // 创建该SN下面的子任务
        if (!client.checkExists(basePath + SEPARATOR + taskQueue + SEPARATOR + task.getStorageIndex()))
            client.createPersistent(basePath + SEPARATOR + taskQueue + SEPARATOR + task.getStorageIndex(), true);
        client.createPersistentSequential(basePath + SEPARATOR + taskQueue + SEPARATOR + task.getStorageIndex() + SEPARATOR + task.getStorageIndex() + "_", false, jsonStr.getBytes());

        client.close();
    }

    public static void consumeTask() {
           
    }

}
