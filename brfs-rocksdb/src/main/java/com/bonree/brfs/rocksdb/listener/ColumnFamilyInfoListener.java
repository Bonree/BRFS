package com.bonree.brfs.rocksdb.listener;

import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.lifecycle.LifecycleStart;
import com.bonree.brfs.common.lifecycle.LifecycleStop;
import com.bonree.brfs.common.lifecycle.ManageLifecycle;
import com.bonree.brfs.common.process.LifeCycle;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.common.utils.PooledThreadFactory;
import com.bonree.brfs.common.rocksdb.RocksDBManager;
import com.bonree.brfs.rocksdb.impl.RocksDBZkPaths;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.Map;
import java.util.concurrent.Executors;

/*******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date 2020/3/19 15:59
 * @Author: <a href=mailto:zhangqi@bonree.com>张奇</a>
 * @Description:
 ******************************************************************************/
@ManageLifecycle
public class ColumnFamilyInfoListener implements LifeCycle {
    private static final Logger LOG = LoggerFactory.getLogger(ColumnFamilyInfoListener.class);

    private CuratorFramework client;
    private NodeCache nodeCache;
    private NodeCacheListener listener;
    private RocksDBManager rocksDBManager;

    @Inject
    public ColumnFamilyInfoListener(CuratorFramework client, ZookeeperPaths zhPaths, RocksDBManager rocksDBManager) {
        this.client = client.usingNamespace(zhPaths.getBaseRocksDBPath().substring(1));
        this.rocksDBManager = rocksDBManager;
    }

    @LifecycleStart
    @Override
    public void start() throws Exception {
        this.nodeCache = new NodeCache(client, RocksDBZkPaths.DEFAULT_PATH_ROCKSDB_COLUMN_FAMILY_INFO);
        this.nodeCache.start();
        this.listener = new ColumnFamilyNodeCacheListener();
        this.nodeCache.getListenable().addListener(listener, Executors.newSingleThreadExecutor(new PooledThreadFactory("column_family_listener")));
        LOG.info("add column family node cache listener");
    }

    @LifecycleStop
    @Override
    public void stop() throws Exception {
        this.nodeCache.getListenable().removeListener(this.listener);
        this.nodeCache.close();
        LOG.info("column family listener stop");
    }

    private class ColumnFamilyNodeCacheListener implements NodeCacheListener {

        @Override
        public void nodeChanged() throws Exception {
            byte[] data = ColumnFamilyInfoListener.this.nodeCache.getCurrentData().getData();
            Map<String, Integer> cfMap = JsonUtils.toObject(data, new TypeReference<Map<String, Integer>>() {
            });
            ColumnFamilyInfoListener.this.rocksDBManager.updateColumnFamilyHandles(cfMap);
        }
    }
}
