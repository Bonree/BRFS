package com.bonree.brfs.duplication.rocksdb.zk;

import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.CommonConfigs;
import com.bonree.brfs.duplication.rocksdb.impl.RocksDBZkPaths;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    private static final String COLON = ":";

    private CuratorFramework client;
    private Service service;
    private ServiceManager serviceManager;

    public BackupHeartBeatManager(CuratorFramework client, Service service, ServiceManager serviceManager) {
        this.client = client;
        this.service = service;
        this.serviceManager = serviceManager;
    }

    /**
     * @param backupId 当前备份ID
     * @description: 更新备份心跳信息到ZK，节点名称为service.getHost 数据内容为 time:backupId
     */
    public void sendBackupHeartBeat(long backupId) {
        String makePath = ZKPaths.makePath(RocksDBZkPaths.DEFAULT_PATH_ROCKSDB_BACKUP_HEARTBEAT, this.service.getHost());
        String data = System.currentTimeMillis() + COLON + backupId;

        try {
            if (this.client.checkExists().forPath(makePath) == null) {
                this.client.create()
                        .creatingParentsIfNeeded()
                        .withMode(CreateMode.PERSISTENT)
                        .forPath(makePath, data.getBytes());
                LOG.info("create backup heartbeat node success, path:{}, data:{}", makePath, data);
            } else {
                this.client.setData().forPath(makePath, data.getBytes());
                LOG.info("update backup heartbeat info success, path:{}, data:{}", makePath, data);
            }
        } catch (Exception e) {
            LOG.error("send backup heartbeat err, path:{}", makePath, e);
        }

    }

    /**
     * @param
     * @return
     * @description: 遍历ZK上存储的备份心跳信息，取最小的backupId
     */
    public int getBackupIdByLatestBackupTiming() {
        List<String> hosts = new ArrayList<>();

        List<Service> services = this.serviceManager.getServiceListByGroup(Configs.getConfiguration().GetConfig(CommonConfigs.CONFIG_REGION_SERVICE_GROUP_NAME));
        for (Service service : services) {
            hosts.add(service.getHost());
        }

        try {
            if (this.client.checkExists().forPath(RocksDBZkPaths.DEFAULT_PATH_ROCKSDB_BACKUP_HEARTBEAT) != null) {
                List<String> backupHosts = this.client.getChildren().forPath(RocksDBZkPaths.DEFAULT_PATH_ROCKSDB_BACKUP_HEARTBEAT);

                if (backupHosts != null && !backupHosts.isEmpty()) {

                    int targetBackupId = 0;

                    for (String backupHost : backupHosts) {
                        String makePath = ZKPaths.makePath(RocksDBZkPaths.DEFAULT_PATH_ROCKSDB_BACKUP_HEARTBEAT, backupHost);

                        byte[] data = this.client.getData().forPath(makePath);
                        String hostAndBackupId = BrStringUtils.fromUtf8Bytes(data);

                        String[] split = hostAndBackupId.split(COLON);
                        long time = Long.parseLong(split[0]);
                        int backupId = Integer.parseInt(split[1]);

                        if (!hosts.contains(backupHost)) {
                            // 若ZK上保存的备份心跳信息未在存活列表之内且最后一次备份时间距离当前时间已超过一小时，则删除
                            if (System.currentTimeMillis() - time > 3600000) {
                                this.client.delete().forPath(makePath);
                                LOG.warn("delete useless zk backup path:{}", makePath);
                                continue;
                            }
                        }

                        if (targetBackupId == 0) {
                            targetBackupId = backupId;
                        }
                        targetBackupId = Math.min(targetBackupId, backupId);

                    }
                    return targetBackupId;
                }

            }
        } catch (Exception e) {
            LOG.error("get backupId by latest backup timing err", e);
            return 0;
        }

        return 0;
    }

}

