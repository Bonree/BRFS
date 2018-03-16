package com.bonree.brfs.server.identification.impl;

import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.common.zookeeper.curator.locking.Executor;

public class ZookeeperIdentificationGen implements Executor {

    private final String dataNode;

    private String ServerId;

    public ZookeeperIdentificationGen(String dataNode) {
        this.dataNode = dataNode;
    }

    @Override
    public void execute(CuratorClient client) {

        if (!client.checkExists(dataNode)) {
            client.createPersistent(dataNode, true, "0".getBytes());
        }
        byte[] bytes = client.getData(dataNode);
        int tmp = Integer.parseInt(new String(bytes)) + 1;
        ServerId = String.valueOf(tmp);
        client.setData(dataNode, ServerId.getBytes());

    }
    
    public String getServerId() {
        return ServerId;
    }

}
