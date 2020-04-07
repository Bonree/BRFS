package com.bonree.brfs.rocksdb.backup;

import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.lifecycle.LifecycleStart;
import com.bonree.brfs.common.lifecycle.LifecycleStop;
import com.bonree.brfs.common.process.LifeCycle;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.supervisor.TimeWatcher;
import com.bonree.brfs.common.utils.PooledThreadFactory;
import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.rocksdb.RocksDBManager;
import com.bonree.brfs.configuration.units.RocksDBConfigs;
import com.bonree.brfs.rocksdb.zk.BackupHeartBeatManager;
import org.apache.curator.framework.CuratorFramework;
import org.rocksdb.BackupEngine;
import org.rocksdb.BackupInfo;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
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
    private BackupEngine backupEngine;

    @Inject
    public RocksDBBackupEngine(CuratorFramework client, ZookeeperPaths zkPaths, Service service, ServiceManager serviceManager, RocksDBManager rocksDBManager) {
        this.rocksDBManager = rocksDBManager;
        this.heartBeatManager = new BackupHeartBeatManager(client.usingNamespace(zkPaths.getBaseRocksDBPath().substring(1)), service, serviceManager);
        this.executor = Executors.newScheduledThreadPool(1, new PooledThreadFactory("rocksdb_backup"));
    }

    @LifecycleStart
    @Override
    public void start() throws Exception {
        String backupPath = Configs.getConfiguration().GetConfig(RocksDBConfigs.ROCKSDB_BACKUP_PATH);
        long backupCycle = Configs.getConfiguration().GetConfig(RocksDBConfigs.ROCKSDB_BACKUP_CYCLE);
        LOG.info("start backup engine, backup path:{}, backup cycle:{}", backupPath, backupCycle);

        File file = new File(backupPath);
        if (!file.exists()) {
            file.mkdirs();
        }

        backupEngine = BackupEngineFactory.getInstance().getBackupEngineByPath(backupPath);

        TimeWatcher watcher = new TimeWatcher();
        this.executor.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {

                try {
                    watcher.getElapsedTimeAndRefresh();
                    int backupId = createNewBackup();
                    LOG.info("current backup cost time:{}, backupId :{}", watcher.getElapsedTime(), backupId);
                    heartBeatManager.sendBackupHeartBeat(backupId);
                } catch (RocksDBException e) {
                    LOG.error("create new backup err", e);
                }

                int backupId = heartBeatManager.getBackupIdByLatestBackupTiming();
                LOG.info("current latest backup timing backupId:{}", backupId);
                cleanExpiredBackupFile(backupId);

            }
        }, calculateInitialDelay(backupCycle), backupCycle, TimeUnit.MILLISECONDS);
    }

    /**
     * @param backupId 备份时间点最晚的节点的当前backupId
     * @description: 根据backupId清理过期备份文件
     */
    public void cleanExpiredBackupFile(int backupId) {
        List<BackupInfo> backupInfos = this.backupEngine.getBackupInfo();

        try {
            for (BackupInfo info : backupInfos) {
                if (info.backupId() < backupId) {
                    this.backupEngine.deleteBackup(info.backupId());
                    LOG.info("clear expired backup, backupId: {}", info.backupId());
                }
            }
        } catch (RocksDBException e) {
            LOG.error("clear expired backup err", e);
        }
    }

    /**
     * @param backupCycle 备份周期
     * @return 距离第一次备份任务执行的时间长度
     * @description: 根据备份周期计算距离第一次备份任务执行的时间长度，也就是线程池的initialDelay参数值
     */
    private long calculateInitialDelay(long backupCycle) {
        return backupCycle - System.currentTimeMillis() % backupCycle;
    }

    public int createNewBackup() throws RocksDBException {
        backupEngine.createNewBackup(rocksDBManager.getRocksDB());
        List<BackupInfo> backupInfos = backupEngine.getBackupInfo();
        return backupInfos.get(backupInfos.size() - 1).backupId();
    }

    public List<Integer> getBackupIds() {
        List<Integer> backupIds = new ArrayList<>();
        List<BackupInfo> backupInfos = this.backupEngine.getBackupInfo();

        for (BackupInfo info : backupInfos) {
            backupIds.add(info.backupId());
        }
        return backupIds;
    }

    @LifecycleStop
    @Override
    public void stop() throws Exception {
        this.executor.shutdown();
        LOG.info("rocksdb backup engine close");
    }

}
