package com.bonree.brfs.rocksdb.zk;

import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.rocksdb.impl.RocksDBZkPaths;
import com.fasterxml.jackson.core.type.TypeReference;
import java.util.HashMap;
import java.util.Map;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date 2020/3/19 20:54
 * @Author: <a href=mailto:zhangqi@bonree.com>张奇</a>
 * @Description: ZK上存储的RocksDB列族信息管理器
 ******************************************************************************/
public class ColumnFamilyInfoManager {

    private static final Logger LOG = LoggerFactory.getLogger(ColumnFamilyInfoManager.class);

    private CuratorFramework client;

    public ColumnFamilyInfoManager(CuratorFramework client) {
        this.client = client;
    }

    public void initOrAddColumnFamilyInfo(String columnFamily, int ttl) {
        try {

            if (this.client.checkExists().forPath(RocksDBZkPaths.DEFAULT_PATH_ROCKSDB_COLUMN_FAMILY_INFO) == null) {
                Map<String, Integer> cfMap = new HashMap<>();
                cfMap.put(columnFamily, ttl);
                byte[] bytes = JsonUtils.toJsonBytes(cfMap);
                this.client.create()
                           .creatingParentContainersIfNeeded()
                           .withMode(CreateMode.PERSISTENT)
                           .forPath(RocksDBZkPaths.DEFAULT_PATH_ROCKSDB_COLUMN_FAMILY_INFO, bytes);
                LOG.info("init column family info complete:{}", cfMap);
            } else {

                byte[] bytes = this.client.getData().forPath(RocksDBZkPaths.DEFAULT_PATH_ROCKSDB_COLUMN_FAMILY_INFO);
                Map<String, Integer> cfMap = JsonUtils.toObject(bytes, new TypeReference<Map<String, Integer>>() {
                });
                cfMap.put(columnFamily, ttl);
                this.client.setData()
                           .forPath(RocksDBZkPaths.DEFAULT_PATH_ROCKSDB_COLUMN_FAMILY_INFO, JsonUtils.toJsonBytes(cfMap));
                LOG.info("update zk column family info complete:{}", cfMap);
            }

        } catch (Exception e) {
            LOG.error("init or add zk column family info err, cf:{}, ttl:{}", columnFamily, ttl, e);
        }
    }

    public void deleteColumnFamilyInfo(String columnFamily) {

        try {
            if (this.client.checkExists().forPath(RocksDBZkPaths.DEFAULT_PATH_ROCKSDB_COLUMN_FAMILY_INFO) == null) {
                return;
            }
            byte[] bytes = this.client.getData().forPath(RocksDBZkPaths.DEFAULT_PATH_ROCKSDB_COLUMN_FAMILY_INFO);
            Map<String, Integer> cfMap = JsonUtils.toObject(bytes, new TypeReference<Map<String, Integer>>() {
            });
            cfMap.remove(columnFamily);
            this.client.setData().forPath(RocksDBZkPaths.DEFAULT_PATH_ROCKSDB_COLUMN_FAMILY_INFO, JsonUtils.toJsonBytes(cfMap));
            LOG.info("delete zk column family info complete: {}", cfMap);
        } catch (Exception e) {
            LOG.error("delete zk column family info err", e);
        }

    }

    public void resetColumnFamilyInfo(Map<String, Integer> columnFamilyMap) {
        try {

            if (this.client.checkExists().forPath(RocksDBZkPaths.DEFAULT_PATH_ROCKSDB_COLUMN_FAMILY_INFO) == null) {
                byte[] bytes = JsonUtils.toJsonBytes(columnFamilyMap);
                this.client.create()
                           .creatingParentContainersIfNeeded()
                           .withMode(CreateMode.PERSISTENT)
                           .forPath(RocksDBZkPaths.DEFAULT_PATH_ROCKSDB_COLUMN_FAMILY_INFO, bytes);
                LOG.info("first reset column family info complete:{}", columnFamilyMap);
            } else {
                this.client.setData().forPath(RocksDBZkPaths.DEFAULT_PATH_ROCKSDB_COLUMN_FAMILY_INFO,
                                              JsonUtils.toJsonBytes(columnFamilyMap));
                LOG.info("reset column family info complete:{}", columnFamilyMap);
            }

        } catch (Exception e) {
            LOG.error("reset column family info err, map:{}", columnFamilyMap, e);
        }
    }

    public Map<String, Integer> getColumnFamilyInfo() {
        Map<String, Integer> map = null;
        try {
            if (this.client.checkExists().forPath(RocksDBZkPaths.DEFAULT_PATH_ROCKSDB_COLUMN_FAMILY_INFO) == null) {
                return null;
            }
            byte[] bytes = this.client.getData().forPath(RocksDBZkPaths.DEFAULT_PATH_ROCKSDB_COLUMN_FAMILY_INFO);
            map = JsonUtils.toObject(bytes, new TypeReference<Map<String, Integer>>() {
            });
        } catch (Exception e) {
            LOG.error("get column family info err", e);
        }
        return map;
    }

}
