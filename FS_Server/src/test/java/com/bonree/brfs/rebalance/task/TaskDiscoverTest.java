package com.bonree.brfs.rebalance.task;

import com.bonree.brfs.common.zookeeper.curator.CuratorClient;

public class TaskDiscoverTest {

    public static void main(String[] args) throws InterruptedException {
        String zkUrl = "192.168.101.86";
        String serversPath = "/brfs/wz/servers";
        CuratorClient client =CuratorClient.getClientInstance(zkUrl);
        client.createEphemeral(serversPath+"/server1", true);
        Thread.sleep(1000);
        client.close();
    }

}
