package com.bonree.brfs.rebalancev2.task.listener;

import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.common.utils.RebalanceUtils;
import com.bonree.brfs.rebalancev2.task.DiskPartitionChangeSummary;
import com.bonree.brfs.rebalancev2.task.TaskDispatcherV2;
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
 * @Description: 监听Server变更，以便生成任务
 ******************************************************************************/
public class ServerChangeListenerV2 implements TreeCacheListener {

    private static final Logger LOG = LoggerFactory.getLogger(ServerChangeListenerV2.class);

    private TaskDispatcherV2 dispatcher;

    public ServerChangeListenerV2(TaskDispatcherV2 dispatcher) {
        this.dispatcher = dispatcher;
    }

    @Override
    public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
        LOG.info("leaderLath:" + dispatcher.getLeaderLatch().hasLeadership());
        if (dispatcher.getLeaderLatch().hasLeadership()) {

            // 发现changes目录有新的任务产生，之所以不监听REMOVE事件是因为REMOVE事件是任务监听器逻辑控制的
            if (event.getType() != Type.NODE_REMOVED) {
                if (event.getData() != null && event.getData().getData() != null) {
                    // 需要进行检查，在切换leader的时候，变更记录需要加载进来。
                    if (!dispatcher.isLoad().get()) {
                        // 此处加载缓存
                        LOG.info("load all changes to summary cache.");
                        dispatcher.loadCache();
                        dispatcher.isLoad().set(true);
                    }

                    if (event.getData().getData() != null) {
                        LOG.info("parse and add change: {}", RebalanceUtils.convertEvent(event));
                        String absolutePath = event.getData().getPath();
                        String chanName = StringUtils
                            .substring(absolutePath, absolutePath.lastIndexOf('/') + 1, absolutePath.length());   // changeId
                        if (chanName.length() > 16) {
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
