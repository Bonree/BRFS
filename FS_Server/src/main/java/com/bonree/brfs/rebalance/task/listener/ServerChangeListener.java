package com.bonree.brfs.rebalance.task.listener;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.zookeeper.curator.cache.AbstractTreeCacheListener;
import com.bonree.brfs.rebalance.task.TaskDispatcher;
import com.bonree.brfs.rebalance.task.TaskDispatcher.ChangeDetail;


/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年4月19日 下午3:31:55
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description: 监听Server变更，以便生成任务
 ******************************************************************************/
public class ServerChangeListener extends AbstractTreeCacheListener {

    private final static Logger LOG = LoggerFactory.getLogger(ServerChangeListener.class);

    private TaskDispatcher dispatcher;

    public ServerChangeListener(String listenName, TaskDispatcher dispatcher) {
        super(listenName);
        this.dispatcher = dispatcher;
    }

    @Override
    public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
        LOG.info("leaderLath:" + dispatcher.getLeaderLatch().hasLeadership());
        LOG.info("server change event detail:" + event.getType());
        if (dispatcher.getLeaderLatch().hasLeadership()) {
            // 检查event是否有数据
            if (!dispatcher.isRemovedNode(event)) { // 不是remove的事件，则需要处理
                if (event.getData() != null && !dispatcher.isEmptyByte(event.getData().getData())) {

                    // 需要进行检查，在切换leader的时候，变更记录需要加载进来。
                    if (!dispatcher.isLoad().get()) {
                        // 此处加载缓存
                        LOG.info("load all");
                        dispatcher.loadCache(client, event);
                        dispatcher.isLoad().set(true);
                    }
                    ChangeDetail detail = new ChangeDetail(client, event);
                    // 将变更细节添加到队列即可
                    dispatcher.getDetailQueue().put(detail);
                } else {
                    LOG.info("ignore the change:" + event);
                }
            }
        }
    }

}
