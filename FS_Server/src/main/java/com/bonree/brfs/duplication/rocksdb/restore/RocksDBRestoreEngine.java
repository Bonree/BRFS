package com.bonree.brfs.duplication.rocksdb.restore;

import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.supervisor.TimeWatcher;
import com.bonree.brfs.common.utils.TimeUtils;
import com.bonree.brfs.common.utils.ZipUtils;
import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.CommonConfigs;
import com.bonree.brfs.configuration.units.RocksDBConfigs;
import com.bonree.brfs.duplication.rocksdb.connection.RegionNodeConnection;
import com.bonree.brfs.duplication.rocksdb.connection.RegionNodeConnectionPool;
import com.bonree.brfs.duplication.rocksdb.zk.BackupHeartBeatManager;
import com.bonree.brfs.duplication.rocksdb.zk.ServiceRegisterTimeManager;
import com.bonree.brfs.rebalance.transfer.SimpleFileServer;
import org.apache.curator.framework.CuratorFramework;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

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

    private Service service;
    private ServiceRegisterTimeManager registerTimeManager;
    private BackupHeartBeatManager heartBeatManager;
    private RegionNodeConnectionPool regionNodeConnectionPool;

    public RocksDBRestoreEngine(CuratorFramework client, ServiceManager serviceManager, Service service, RegionNodeConnectionPool regionNodeConnectionPool) {
        this.service = service;
        this.regionNodeConnectionPool = regionNodeConnectionPool;
        this.heartBeatManager = new BackupHeartBeatManager(client, service);
        this.registerTimeManager = new ServiceRegisterTimeManager(serviceManager);
    }

    /**
     * @description: 进行数据检查，根据情况决定是否执行数据恢复过程
     */
    public void restore() {

        String backupPath = Configs.getConfiguration().GetConfig(RocksDBConfigs.ROCKSDB_BACKUP_PATH);
        String rocksDBPath = Configs.getConfiguration().GetConfig(RocksDBConfigs.ROCKSDB_STORAGE_PATH);
        long backupCycle = Configs.getConfiguration().GetConfig(RocksDBConfigs.ROCKSDB_BACKUP_CYCLE);

        long prevTimeStamp = TimeUtils.prevTimeStamp(System.currentTimeMillis(), backupCycle);
        long lastBackupHeartBeat = heartBeatManager.lastBackupHeartBeat();

        if (lastBackupHeartBeat == 0) {
            LOG.info("lastBackupHeartBeat is zero");
            return;
        }

        if (prevTimeStamp == lastBackupHeartBeat) {
            LOG.info("Don't need rocksdb restore, prevTimeStamp equals lastBackupHeartBeat:{}", lastBackupHeartBeat);
            return;
        }

        List<String> missedBackupFiles = calMissedBackupFiles(prevTimeStamp, lastBackupHeartBeat, backupCycle);
        SimpleFileServer fileServer = null;

        Service earliestRegisterService = this.registerTimeManager.getEarliestRegisterService();
        if (!earliestRegisterService.getHost().equals(this.service.getHost())) {
            RegionNodeConnection connection = this.regionNodeConnectionPool.getConnection(Configs.getConfiguration().GetConfig(CommonConfigs.CONFIG_REGION_SERVICE_GROUP_NAME), earliestRegisterService.getServiceId());
            if (connection == null || connection.getClient() == null) {
                LOG.warn("region node connection/client is null! serviceId:{}", this.service.getServiceId());
                return;
            }

            String tmpFileName = UUID.randomUUID().toString() + ".zip";
            boolean isOk = connection.getClient().establishSocket(tmpFileName, backupPath, this.service.getHost(), this.service.getPort() + 30, missedBackupFiles);
            if (isOk) {
                try {
                    fileServer = new SimpleFileServer(this.service.getPort() + 30, rocksDBPath, 1);
                    fileServer.start();
                } catch (IOException e) {
                    LOG.error("simple file server err", e);
                }
            }

            String tmpFilePath = backupPath + "/" + tmpFileName;
            ZipUtils.unZip(tmpFilePath, backupPath);
            if (new File(tmpFilePath).delete()) {
                LOG.info("server delete tmp transfer file :{}", tmpFilePath);
            }
        }

        RestoreOptions restoreOptions = null;
        BackupableDBOptions backupableDBOptions = null;
        BackupEngine backupEngine = null;

        try {
            TimeWatcher watcher = new TimeWatcher();
            restoreOptions = new RestoreOptions(false);

            for (String file : missedBackupFiles) {
                try {
                    backupableDBOptions = new BackupableDBOptions(backupPath + "/" + file);
                    backupEngine = BackupEngine.open(Env.getDefault(), backupableDBOptions);
                    backupEngine.restoreDbFromLatestBackup(rocksDBPath, rocksDBPath, restoreOptions);
                    LOG.info("file {} restore complete, cost time:{}", file, watcher.getElapsedTimeAndRefresh());
                } finally {
                    if (backupEngine != null) {
                        backupEngine.close();
                    }
                    if (backupableDBOptions != null) {
                        backupableDBOptions.close();
                    }
                }
            }

            LOG.info("restore engine execute complete, restore file list:{}, prevTimeStamp:{}, lastBackupHeartBeat:{}", missedBackupFiles, prevTimeStamp, lastBackupHeartBeat);

        } catch (RocksDBException e) {
            LOG.error("restore engine err", e);
        } finally {
            if (fileServer != null) {
                try {
                    fileServer.close();
                } catch (IOException ioe) {
                    LOG.error("file server close err", ioe);
                }
            }
            if (backupEngine != null) {
                backupEngine.close();
            }
            if (backupableDBOptions != null) {
                backupableDBOptions.close();
            }
            if (restoreOptions != null) {
                restoreOptions.close();
            }

        }

    }

    /**
     * @param prevTimeStamp       当前备份时间
     * @param lastBackupHeartBeat 最后一次备份时间
     * @return 缺失备份文件名
     * @description: 根据当前备份时间点和最后一次备份时间点计算缺失备份文件列表
     */
    public List<String> calMissedBackupFiles(long prevTimeStamp, long lastBackupHeartBeat, long backupCycle) {
        long tmpPrevTimeStamp = prevTimeStamp;
        List<String> missedFiles = new ArrayList<>();

        while (tmpPrevTimeStamp > lastBackupHeartBeat) {
            missedFiles.add(String.valueOf(tmpPrevTimeStamp));
            tmpPrevTimeStamp -= backupCycle;
        }
        return missedFiles;
    }

}
