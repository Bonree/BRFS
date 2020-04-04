package com.bonree.brfs.rocksdb.restore;

import com.bonree.brfs.common.lifecycle.LifecycleStart;
import com.bonree.brfs.common.lifecycle.LifecycleStop;
import com.bonree.brfs.common.process.LifeCycle;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.supervisor.TimeWatcher;
import com.bonree.brfs.common.utils.FileUtils;
import com.bonree.brfs.common.utils.PooledThreadFactory;
import com.bonree.brfs.common.utils.ZipUtils;
import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.CommonConfigs;
import com.bonree.brfs.rocksdb.RocksDBManager;
import com.bonree.brfs.rocksdb.backup.BackupEngineFactory;
import com.bonree.brfs.rocksdb.configuration.RocksDBConfigs;
import com.bonree.brfs.rocksdb.connection.RegionNodeConnection;
import com.bonree.brfs.rocksdb.connection.RegionNodeConnectionPool;
import com.bonree.brfs.rocksdb.file.SimpleFileReceiver;
import com.bonree.brfs.rocksdb.zk.ServiceRegisterTimeManager;
import org.rocksdb.BackupEngine;
import org.rocksdb.RestoreOptions;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.File;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/*******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date 2020/3/19 20:53
 * @Author: <a href=mailto:zhangqi@bonree.com>张奇</a>
 * @Description: RocksDB数据恢复引擎
 ******************************************************************************/
public class RocksDBRestoreEngine implements LifeCycle {

    private static final Logger LOG = LoggerFactory.getLogger(RocksDBRestoreEngine.class);

    private static final String TRANSFER_FILE_NAME = UUID.randomUUID().toString() + ZipUtils.SUFFIX;

    private Service service;
    private RocksDBManager rocksDBManager;
    private ServiceRegisterTimeManager registerTimeManager;
    private RegionNodeConnectionPool regionNodeConnectionPool;
    private ExecutorService executor;
    private ExecutorService simpleFileServer;
    private SimpleFileReceiver fileServer;

    private String restorePath;
    private String tmpRestorePath;

    private int transferPort;

    @Inject
    public RocksDBRestoreEngine(Service service, RocksDBManager rocksDBManager, ServiceManager serviceManager, RegionNodeConnectionPool regionNodeConnectionPool) {
        this.service = service;
        this.rocksDBManager = rocksDBManager;
        this.regionNodeConnectionPool = regionNodeConnectionPool;
        this.registerTimeManager = new ServiceRegisterTimeManager(serviceManager);
        restorePath = Configs.getConfiguration().GetConfig(RocksDBConfigs.ROCKSDB_RESTORE_PATH);
        tmpRestorePath = Configs.getConfiguration().GetConfig(RocksDBConfigs.ROCKSDB_RESTORE_TEMPORARY_PATH);
        transferPort = Configs.getConfiguration().GetConfig(RocksDBConfigs.ROCKSDB_BACKUP_FILE_TRANSFER_PORT);
        executor = Executors.newSingleThreadExecutor(new PooledThreadFactory("rocksdb_restore"));
        simpleFileServer = Executors.newSingleThreadExecutor(new PooledThreadFactory("restore_file_server"));
    }

    @LifecycleStart
    @Override
    public void start() {
        LOG.info("start restore engine...");
        Service earliestService = this.registerTimeManager.getEarliestRegisterService();

        // 只有一个RegionNode时没有必要做恢复操作
        if (earliestService.getHost().equals(this.service.getHost())) {
            LOG.info("earliest register service is local, restore exit.");
            return;
        }

        simpleFileServer.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    FileUtils.createDir(restorePath, false);
                    FileUtils.createDir(tmpRestorePath, false);

                    fileServer = new SimpleFileReceiver(service.getHost(), transferPort, 4, restorePath + File.separator + TRANSFER_FILE_NAME);
                    fileServer.start();
                } catch (Exception e) {
                    LOG.error("simple file server err", e);
                }
            }
        });
        executor.execute(new RocksDBRestoreTask(service, regionNodeConnectionPool, earliestService.getServiceId()));
    }

    private class RocksDBRestoreTask implements Runnable {
        private Service service;
        private RegionNodeConnectionPool regionNodeConnectionPool;
        private String serviceId;

        private RocksDBRestoreTask(Service service, RegionNodeConnectionPool regionNodeConnectionPool, String serviceId) {
            this.service = service;
            this.regionNodeConnectionPool = regionNodeConnectionPool;
            this.serviceId = serviceId;
        }

        @Override
        public void run() {

            RegionNodeConnection connection = this.regionNodeConnectionPool.getConnection(Configs.getConfiguration().GetConfig(CommonConfigs.CONFIG_REGION_SERVICE_GROUP_NAME), serviceId);
            if (connection == null || connection.getClient() == null) {
                LOG.warn("region node connection/client is null! serviceId:{}", this.service.getServiceId());
                return;
            }

            List<Integer> backupIds = connection.getClient().restoreData(TRANSFER_FILE_NAME, restorePath, this.service.getHost(), transferPort);
            if (backupIds == null || backupIds.isEmpty()) {
                LOG.info("backupIds is null or empty, restore engine exit.");
                return;
            }

            String transferFilePath = restorePath + File.separator + TRANSFER_FILE_NAME;
            ZipUtils.unZip(transferFilePath, restorePath);

            if (FileUtils.deleteFile(transferFilePath)) {
                LOG.info("delete tmp transfer file :{}", transferFilePath);
            }

            try {
                RestoreOptions restoreOptions = new RestoreOptions(false);
                BackupEngine backupEngine = BackupEngineFactory.getInstance().getBackupEngineByPath(restorePath);

                TimeWatcher watcher = new TimeWatcher();
                for (Integer backupId : backupIds) {
                    backupEngine.restoreDbFromBackup(backupId, tmpRestorePath, tmpRestorePath, restoreOptions);
                }
                LOG.info("restore complete, backupIds:{}, cost time: {}, from [{}] to [{}]", backupIds, watcher.getElapsedTimeAndRefresh(), restorePath, tmpRestorePath);

                rocksDBManager.dataTransfer(tmpRestorePath);
                LOG.info("data transfer complete, cost time:{}", watcher.getElapsedTime());

                FileUtils.deleteDir(restorePath, true);
                FileUtils.deleteDir(tmpRestorePath, true);
            } catch (RocksDBException e) {
                LOG.error("restore engine err", e);
            } finally {
                close();
            }
        }
    }


    @LifecycleStop
    @Override
    public void stop() throws Exception {

    }

    public void close() {
        if (fileServer != null) {
            try {
                fileServer.stop();
            } catch (Exception e) {
                LOG.error("file server close err", e);
            }
        }
        if (executor != null) {
            executor.shutdown();
        }
        if (simpleFileServer != null) {
            simpleFileServer.shutdown();
        }
        LOG.info("rocksdb restore engine stop");
    }
}
