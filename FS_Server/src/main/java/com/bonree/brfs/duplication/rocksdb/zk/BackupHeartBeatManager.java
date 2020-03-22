package com.bonree.brfs.duplication.rocksdb.zk;

import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.duplication.rocksdb.impl.RocksDBZkPaths;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date 2020/3/19 18:51
 * @Author: <a href=mailto:zhangqi@bonree.com>张奇</a>
 * @Description: RocksDB数据备份心跳管信息管理
 ******************************************************************************/
public class BackupHeartBeatManager {
    private static final Logger LOG = LoggerFactory.getLogger(BackupHeartBeatManager.class);

    private CuratorFramework client;
    private Service service;

    public BackupHeartBeatManager(CuratorFramework client, Service service) {
        this.client = client;
        this.service = service;
    }

    /**
     * @param time 当前备份时间点
     * @description: 更新备份心跳信息到ZK， 信息为service.getHost time:本次备份时间
     */
    public void updateBackupHeartBeat(long time) {
        String makePath = ZKPaths.makePath(RocksDBZkPaths.DEFAULT_PATH_ROCKSDB_BACKUP_HEARTBEAT, this.service.getHost());

        try {
            if (this.client.checkExists().forPath(makePath) == null) {
                this.client.create()
                        .creatingParentsIfNeeded()
                        .withMode(CreateMode.PERSISTENT)
                        .forPath(makePath, String.valueOf(time).getBytes());
                LOG.info("create backup heartbeat success, path:{}, time:{}", makePath, time);
            } else {
                this.client.setData().forPath(makePath, String.valueOf(time).getBytes());
                LOG.info("update backup heartbeat success, path:{}, time:{}", makePath, time);
            }
        } catch (Exception e) {
            LOG.error("send backup heartbeat err, path:{}", makePath, e);
        }

    }

    /**
     * @return 最近一次备份时间
     * @description: 获取最近一次备份时间点
     */
    public long lastBackupHeartBeat() {
        String makePath = ZKPaths.makePath(RocksDBZkPaths.DEFAULT_PATH_ROCKSDB_BACKUP_HEARTBEAT, this.service.getHost());
        long lastBackupHeartBeat;
        try {
            if (this.client.checkExists().forPath(makePath) == null) {
                LOG.warn("service not exists, path:{}", makePath);
                return 0L;
            }

            byte[] lastBackupTimeBytes = this.client.getData().forPath(makePath);
            lastBackupHeartBeat = Long.parseLong(BrStringUtils.fromUtf8Bytes(lastBackupTimeBytes));

        } catch (Exception e) {
            LOG.info("check backup heartbeat err, path:{}", makePath, e);
            return 0L;
        }
        return lastBackupHeartBeat;
    }

}
