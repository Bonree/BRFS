package com.bonree.brfs.duplication.rocksdb.listener;

import com.bonree.brfs.common.process.LifeCycle;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.common.utils.PooledThreadFactory;
import com.bonree.brfs.duplication.rocksdb.RocksDBManager;
import com.bonree.brfs.duplication.rocksdb.impl.RocksDBZkPaths;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class ColumnFamilyInfoListener implements LifeCycle {
    private static final Logger LOG = LoggerFactory.getLogger(ColumnFamilyInfoListener.class);

    private CuratorFramework client;
    private NodeCache nodeCache;
    private NodeCacheListener listener;
    private RocksDBManager rocksDBManager;

    public ColumnFamilyInfoListener(CuratorFramework client, RocksDBManager rocksDBManager) {
        this.client = client;
        this.rocksDBManager = rocksDBManager;
    }


    @Override
    public void start() throws Exception {
        this.nodeCache = new NodeCache(client, RocksDBZkPaths.DEFAULT_PATH_ROCKSDB_COLUMN_FAMILY_INFO);
        this.nodeCache.start();
        this.listener = new ColumnFamilyNodeCacheListener();
        this.nodeCache.getListenable().addListener(listener, Executors.newSingleThreadExecutor(new PooledThreadFactory("column_family_listener")));
        LOG.info("column family listener running...");
    }

    @Override
    public void stop() throws Exception {
        this.nodeCache.getListenable().removeListener(this.listener);
        this.nodeCache.close();
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
