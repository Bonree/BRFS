package com.bonree.brfs.rebalance.task;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.jboss.netty.util.internal.ConcurrentHashMap;

import com.alibaba.fastjson.JSON;
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

    private Map<Integer, List<ChangeSummary>> cacheSummaryCache = new ConcurrentHashMap<Integer, List<ChangeSummary>>();

    // 用于处理变更任务
    class TaskDispatchListener extends AbstractTreeCacheListener {

        public TaskDispatchListener(String listenName) {
            super(listenName);
        }

        @Override
        public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
            if (leaderLath.hasLeadership()) {
                String jsonStr = new String(event.getData().getData());
                ChangeSummary changeSummary = JSON.parseObject(jsonStr, ChangeSummary.class);
                int storageIndex = changeSummary.getStorageIndex();
                List<ChangeSummary> changeSummarys = cacheSummaryCache.get(storageIndex);
                if (changeSummarys == null) {
                    changeSummarys = new ArrayList<ChangeSummary>();
                    cacheSummaryCache.put(storageIndex, changeSummarys);
                }
                changeSummarys.add(changeSummary);
                auditTask(changeSummarys);
            }
        }

    }

    public void auditTask(List<ChangeSummary> changeSummarys) {
        // for (int i=0;i<changeSummarys.size();i++) {
        ChangeSummary changeSummary = changeSummarys.get(0);
        if (changeSummary.getChangeType() == ChangeType.ADD) {
            /*
             * 增加机器分为两种情况：
             * 1.新机器或服务添加，需要判断是否进行虚拟ID迁移
             * 2.旧机器回归，若已经触发副本恢复平衡机制，则可根据进度来选择是否中断，依旧需要判断是否进行虚拟ID迁移
             */
        } else if (changeSummary.getChangeType() == ChangeType.REMOVE) {
            /*
             * 移除机器也分为两种情况：
             * 1.正在发生虚拟ID迁移时，移除机器。则迁移作废。继而触发副本迁移任务
             * 2.
             */
        }
        // }
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
    
    public static void main(String[] args) {
        List<Integer> a = new ArrayList<>();
        a.add(1);
        a.add(2);
        a.add(3);
        a.add(4);
        System.out.println(a);
        Iterator<Integer> it =a.iterator();
        while(it.hasNext()) {
            int b=it.next();
            if(b==2) {
                it.remove();
            }
        }
        System.out.println(a.get(1));
    }

}
