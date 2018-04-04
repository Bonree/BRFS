package com.bonree.brfs.rebalance.task;

import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.rebalance.Constants;

public class TaskOperationTest {

    public static void main(String[] args) throws InterruptedException {
        CuratorClient client = CuratorClient.getClientInstance(Constants.zkUrl);
        client.blockUntilConnected();
        client.delete(Constants.PATH_TASKS, true);
        if(!client.checkExists(Constants.PATH_TASKS)) {
            client.createPersistent(Constants.PATH_TASKS, false);
        }
//        String node = Constants.PATH_TASKS+Constants.SEPARATOR+1+Constants.SEPARATOR+"server2";
//        client.createPersistent(node, true,"test".getBytes());
        client.close();
    }

}
