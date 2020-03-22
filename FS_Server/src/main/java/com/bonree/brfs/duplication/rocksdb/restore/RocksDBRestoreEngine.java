package com.bonree.brfs.duplication.rocksdb.restore;

import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.utils.TimeUtils;
import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.RocksDBConfigs;
import com.bonree.brfs.duplication.rocksdb.zk.BackupHeartBeatManager;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/*******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date 2020/3/19 20:53
 * @Author: <a href=mailto:zhangqi@bonree.com>张奇</a>
 * @Description: RocksDB数据恢复引擎
 ******************************************************************************/
public class RocksDBRestoreEngine {

    private static final Logger LOG = LoggerFactory.getLogger(RocksDBRestoreEngine.class);

    // 1.检查缺失数据块确定是否要进行恢复
    // 如果需要，发送signal到对应Service请求建立临时Socket连接，传输备份文件

    // 2.进行恢复

    private BackupHeartBeatManager heartBeatManager;
    private long backupCycle;

    public RocksDBRestoreEngine(CuratorFramework client, Service service) {
        this.heartBeatManager = new BackupHeartBeatManager(client, service);
        this.backupCycle = Configs.getConfiguration().GetConfig(RocksDBConfigs.ROCKSDB_BACKUP_CYCLE);
    }

    public void restore() {
        long prevTimeStamp = TimeUtils.prevTimeStamp(System.currentTimeMillis(), backupCycle);
        long lastBackupHeartBeat = heartBeatManager.lastBackupHeartBeat();
        if (lastBackupHeartBeat == 0) {
            return;
        }

        // 需要进行恢复
        if (prevTimeStamp == lastBackupHeartBeat) {
            LOG.info("Don't need rocksdb restore, prevTimeStamp equals lastBackupHeartBeat:{}", lastBackupHeartBeat);
            return;
        }


    }

    /**
     * @param prevTimeStamp       当前备份时间
     * @param lastBackupHeartBeat 最后一次备份时间
     * @return 缺失备份文件名
     * @description: 根据当前备份时间点和最后一次备份时间点计算缺失备份文件列表
     */
    public List<String> calMissedBackupFiles(long prevTimeStamp, long lastBackupHeartBeat) {
        List<String> missedFiles = new ArrayList<>();

        return missedFiles;
    }

}
