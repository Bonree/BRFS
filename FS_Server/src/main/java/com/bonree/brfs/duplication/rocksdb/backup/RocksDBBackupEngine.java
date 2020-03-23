package com.bonree.brfs.duplication.rocksdb.backup;

import com.bonree.brfs.common.process.LifeCycle;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.supervisor.TimeWatcher;
import com.bonree.brfs.common.utils.PooledThreadFactory;
import com.bonree.brfs.common.utils.TimeUtils;
import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.RocksDBConfigs;
import com.bonree.brfs.duplication.rocksdb.RocksDBManager;
import com.bonree.brfs.duplication.rocksdb.zk.BackupHeartBeatManager;
import org.apache.curator.framework.CuratorFramework;
import org.rocksdb.BackupEngine;
import org.rocksdb.BackupableDBOptions;
import org.rocksdb.Env;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/*******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date 2020/3/19 20:52
 * @Author: <a href=mailto:zhangqi@bonree.com>张奇</a>
 * @Description: RocksDB数据备份引擎
 ******************************************************************************/
public class RocksDBBackupEngine implements LifeCycle {

    private static final Logger LOG = LoggerFactory.getLogger(RocksDBBackupEngine.class);

    private ScheduledExecutorService executor;
    private RocksDBManager rocksDBManager;
    private BackupHeartBeatManager heartBeatManager;

    private BackupableDBOptions backupableDBOptions;
    private BackupEngine backupEngine;

    public RocksDBBackupEngine(CuratorFramework client, Service service, RocksDBManager rocksDBManager) {
        this.rocksDBManager = rocksDBManager;
        this.heartBeatManager = new BackupHeartBeatManager(client, service);
        this.executor = Executors.newScheduledThreadPool(1, new PooledThreadFactory("rocksdb_backup_executor"));
    }

    @Override
    public void start() throws Exception {

        TimeWatcher watcher = new TimeWatcher();
        String backupPath = Configs.getConfiguration().GetConfig(RocksDBConfigs.ROCKSDB_BACKUP_PATH);
        long backupCycle = Configs.getConfiguration().GetConfig(RocksDBConfigs.ROCKSDB_BACKUP_CYCLE);

        this.executor.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                long prevTimeStamp = TimeUtils.prevTimeStamp(System.currentTimeMillis(), backupCycle);

                String currBackupPath = backupPath + "/" + prevTimeStamp;
                backupableDBOptions = new BackupableDBOptions(currBackupPath);

                try {
                    watcher.getElapsedTimeAndRefresh();
                    backupEngine = BackupEngine.open(Env.getDefault(), backupableDBOptions);
                    backupEngine.createNewBackup(rocksDBManager.getRocksDB());
                    LOG.info("current backup cost time:{}, backup path:{}", watcher.getElapsedTimeAndRefresh(), currBackupPath);

                    heartBeatManager.updateBackupHeartBeat(prevTimeStamp);
                } catch (RocksDBException e) {
                    LOG.error("generate backup engine err, path:{}", currBackupPath, e);
                } finally {
                    if (backupEngine != null) {
                        backupEngine.close();
                    }
                    if (backupableDBOptions != null) {
                        backupableDBOptions.close();
                    }
                }

                cleanExpiredBackupFile(backupPath, prevTimeStamp);


            }
        }, 3000, backupCycle, TimeUnit.MILLISECONDS);
    }

    public void cleanExpiredBackupFile(String backupPath, long prevTimeStamp) {
        File file = new File(backupPath);
        for (File f : Objects.requireNonNull(file.listFiles())) {
            long tmpTime = Long.parseLong(f.getName());
            if (tmpTime < prevTimeStamp) {
                if (f.delete()) {
                    LOG.info("clear expired backup file:{}", f.getAbsolutePath());
                } else {
                    LOG.warn("delete expired backup file failed:{}", f.getAbsolutePath());
                }
            }
        }
    }

    @Override
    public void stop() throws Exception {
        if (backupEngine != null) {
            backupEngine.close();
        }
        if (backupableDBOptions != null) {
            backupableDBOptions.close();
        }
        this.executor.shutdown();
    }

}
