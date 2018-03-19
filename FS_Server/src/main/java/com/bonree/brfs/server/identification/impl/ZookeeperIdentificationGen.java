package com.bonree.brfs.server.identification.impl;

import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.common.zookeeper.curator.locking.Executor;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年3月19日 上午11:26:33
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description: 服务标识生成策略
 ******************************************************************************/
public class ZookeeperIdentificationGen implements Executor<String> {

    private final String dataNode;

    public ZookeeperIdentificationGen(String dataNode) {
        this.dataNode = dataNode;
    }

    @Override
    public String execute(CuratorClient client) {

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
