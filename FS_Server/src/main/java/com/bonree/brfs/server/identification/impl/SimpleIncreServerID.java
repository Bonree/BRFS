package com.bonree.brfs.server.identification.impl;

import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.server.identification.IncreServerID;

public class SimpleIncreServerID implements IncreServerID<String> {

    @Override
    public String increServerID(CuratorClient client, String dataNode) {
        if (!client.checkExists(dataNode)) {
            client.createPersistent(dataNode, true, "0".getBytes());
        }
        byte[] bytes = client.getData(dataNode);
        String serverId = new String(bytes);
        int tmp = Integer.parseInt(new String(bytes)) + 1;
        client.setData(dataNode, String.valueOf(tmp).getBytes());
        return serverId;
    }

}
