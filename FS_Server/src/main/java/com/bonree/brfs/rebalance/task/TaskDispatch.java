package com.bonree.brfs.rebalance.task;

import java.io.Closeable;
import java.io.IOException;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;

import com.bonree.brfs.common.utils.StringUtils;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.common.zookeeper.curator.cache.AbstractTreeCacheListener;
import com.bonree.brfs.common.zookeeper.curator.cache.CuratorTreeCache;
import com.google.common.base.Preconditions;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年3月23日 下午4:25:05
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description: 此处来进行任务分配，任务核心控制
 ******************************************************************************/
public class TaskDispatch implements Closeable {

    public final CuratorClient client;
    
    public final LeaderLatch leaderLath;
    
    public final CuratorTreeCache treeCache;

    public final static String SEPARATOR = "/";

    public final static String LEADER_NODE = "leadermonitor";

    public final static String TASK_NODE = "taskmonitor";

    //用于处理变更任务
    class TaskDispatchListener extends AbstractTreeCacheListener {

        public TaskDispatchListener(String listenName) {
            super(listenName);
        }

        @Override
        public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
            if(leaderLath.hasLeadership()) {
                System.out.println(event.getData().getPath());
            }
        }
        
    }

    public TaskDispatch(String zkUrl, String basePath) {
        client = CuratorClient.getClientInstance(Preconditions.checkNotNull(zkUrl, "zkUrk is not null!"));
        String str = StringUtils.trimBasePath(Preconditions.checkNotNull(basePath, "basePath is not null!"));
        treeCache = CuratorTreeCache.getTreeCacheInstance(zkUrl);
        leaderLath = new LeaderLatch(client.getInnerClient(), str + SEPARATOR + LEADER_NODE);
        treeCache.addListener(str + SEPARATOR, new TaskDispatchListener("task_dispatch"));
    }

    public void start() {

    }

    public void dispatch() {

    }

    // public static void main(String[] args) throws Exception {
    // dispatch();
    // }

    @Override
    public void close() throws IOException {
        if (leaderLath != null) {
            leaderLath.close();
        }

        if (client != null) {
            client.close();
        }
    }

}
