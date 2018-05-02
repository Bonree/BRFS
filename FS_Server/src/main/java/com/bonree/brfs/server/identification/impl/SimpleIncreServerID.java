package com.bonree.brfs.server.identification.impl;

import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.server.identification.IncreServerID;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年4月28日 下午3:20:10
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description: serverID递增获取
 ******************************************************************************/
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
