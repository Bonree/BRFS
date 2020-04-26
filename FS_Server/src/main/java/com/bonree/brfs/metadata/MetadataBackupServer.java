package com.bonree.brfs.metadata;

import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.lifecycle.LifecycleStart;
import com.bonree.brfs.common.lifecycle.LifecycleStop;
import com.bonree.brfs.common.lifecycle.ManageLifecycle;
import com.bonree.brfs.common.process.LifeCycle;
import com.bonree.brfs.common.supervisor.TimeWatcher;
import com.bonree.brfs.common.utils.FileUtils;
import com.bonree.brfs.common.utils.PooledThreadFactory;
import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.CommonConfigs;
import com.bonree.brfs.metadata.backup.DefaultMetadataBackupEngine;
import com.bonree.brfs.metadata.backup.MetadataBackupEngine;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date 2020/4/16 15:41
 * @Author: <a href=mailto:zhangqi@bonree.com>张奇</a>
 * @Description: 负责周期备份元数据
 ******************************************************************************/
@ManageLifecycle
public class MetadataBackupServer implements LifeCycle {
    private static final Logger LOG = LoggerFactory.getLogger(MetadataBackupServer.class);

    private ZookeeperPaths zkPaths;
    private ScheduledExecutorService metadataExecutor;

    @Inject
    public MetadataBackupServer(ZookeeperPaths zkPaths) {
        this.zkPaths = zkPaths;
        metadataExecutor = Executors.newSingleThreadScheduledExecutor(new PooledThreadFactory("metadata_backup"));
    }

    @LifecycleStart
    @Override
    public void start() throws IOException {
        String zkHost = Configs.getConfiguration().getConfig(CommonConfigs.CONFIG_ZOOKEEPER_ADDRESSES);
        String zkPath = zkPaths.getBaseClusterName();
        String metadataPath = Configs.getConfiguration().getConfig(CommonConfigs.CONFIG_METADATA_BACKUP_PATH) + "/brfs.metadata";
        FileUtils.createFile(metadataPath, true);
        long backupCycle = Configs.getConfiguration().getConfig(CommonConfigs.CONFIG_METADATA_BACKUP_CYCLE) * 60 * 1000;

        metadataExecutor.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                TimeWatcher watcher = new TimeWatcher();

                MetadataBackupEngine backupEngine =
                    new DefaultMetadataBackupEngine(zkHost, zkPath, Math.max(2, Runtime.getRuntime().availableProcessors() / 4));
                ZNode root = backupEngine.backup();

                try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(metadataPath))) {
                    if (root != null) {
                        oos.writeObject(root);
                        LOG.info("metadata backup success, backup file: {}, cost time: {} ms", metadataPath,
                                 watcher.getElapsedTime());
                    }
                } catch (IOException e) {
                    LOG.error("metadata executor error", e);
                }
            }
        }, 3000, backupCycle, TimeUnit.MILLISECONDS);
    }

    @LifecycleStop
    @Override
    public void stop() throws Exception {
        if (metadataExecutor != null) {
            metadataExecutor.shutdown();
        }
    }
}
