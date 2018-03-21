package com.bonree.brfs.rebalance.task;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;

import com.alibaba.fastjson.JSON;
import com.bonree.brfs.common.zookeeper.curator.cache.AbstractPathChildrenCacheListener;
import com.bonree.brfs.server.model.ServerInfoModel;

public class ServersChangeListener extends AbstractPathChildrenCacheListener {

    public ServersChangeListener(String listenName) {
        super(listenName);
    }

    @Override
    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
        switch (event.getType()) {
            case CHILD_ADDED: {
                System.out.println("one server join in");
                //新来一个节点，会进行新来的节点判断，是否进行虚拟ID恢复
                byte[] bytes = event.getData().getData();
                String jsonStr = new String(bytes);
                ServerInfoModel serverModel = JSON.parseObject(jsonStr, ServerInfoModel.class);
                //无论是否为新的节点，则都需要是否virtualID迁移检查
                if (!serverModel.isInit()) {

                }
                break;
            }
            case CHILD_REMOVED: {
                System.out.println("one server remove out");
                // 如果有个服务器挂掉，那么触发此处,需要准备一定的时间后，进行服务平衡
                break;
            }
            default:
                break;
        }
    }

}
