package com.bonree.brfs.rebalance.task.listener;

import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.common.utils.RebalanceUtils;
import com.bonree.brfs.rebalance.task.DiskPartitionChangeSummary;
import com.bonree.brfs.rebalance.task.TaskDispatcher;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent.Type;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @date 2018年4月19日 下午3:31:55
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description: 监听changes目录，根据变更情况生成磁盘摘要信息并放入队列
 ******************************************************************************/
public class ServerChangeListener implements TreeCacheListener {

    private static final Logger LOG = LoggerFactory.getLogger(ServerChangeListener.class);

    private TaskDispatcher dispatcher;

    public ServerChangeListener(TaskDispatcher dispatcher) {
        this.dispatcher = dispatcher;
    }

    @Override
    public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
        LOG.info("is leaderLath: {}", dispatcher.getLeaderLatch().hasLeadership());
        if (dispatcher.getLeaderLatch().hasLeadership()) {

            // 发现changes目录有新的任务产生，之所以不监听REMOVE事件是因为REMOVE事件是任务监听器逻辑控制的
            if (event.getType() != Type.NODE_REMOVED) {
                if (event.getData() != null && event.getData().getData() != null) {
                    // 需要进行检查，在切换leader的时候，变更记录需要加载进来。
                    if (!dispatcher.isLoad().get()) {
                        // 此处加载缓存,所有的changes
                        LOG.info("load all changes to summary cache.");
                        dispatcher.loadCache();
                        dispatcher.isLoad().set(true);
                    }

                    if (event.getData().getData() != null) {
                        LOG.info("parse and add change: {}", RebalanceUtils.convertEvent(event));
                        String absolutePath = event.getData().getPath();
                        String changeId = StringUtils
                            .substring(absolutePath, absolutePath.lastIndexOf('/') + 1, absolutePath.length());   // changeId
                        if (changeId.length() > 16) {
                            DiskPartitionChangeSummary summary =
                                JsonUtils.toObjectQuietly(event.getData().getData(), DiskPartitionChangeSummary.class);
                            if (summary != null) {
                                // 将变更细节添加到队列即可
                                dispatcher.getDetailQueue().put(summary);
                            }
                        } else {
                            LOG.info("ignore the change: {}", RebalanceUtils.convertEvent(event));
                        }
                    }

                } else {
                    LOG.info("ignore the invalid change: {}", event);
                }
            }
        }
    }

}
