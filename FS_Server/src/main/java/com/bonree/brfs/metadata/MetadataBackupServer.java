package com.bonree.brfs.metadata;

import com.bonree.brfs.common.process.LifeCycle;
import com.bonree.brfs.common.supervisor.TimeWatcher;
import com.bonree.brfs.common.utils.PooledThreadFactory;
import com.bonree.brfs.metadata.backup.DefaultMetadataBackupEngine;
import com.bonree.brfs.metadata.backup.MetadataBackupEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/*******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date 2020/4/16 15:41
 * @Author: <a href=mailto:zhangqi@bonree.com>张奇</a>
 * @Description: 负责周期备份元数据
 ******************************************************************************/
public class MetadataBackupServer implements LifeCycle {
    private static final Logger LOG = LoggerFactory.getLogger(MetadataBackupServer.class);

    private ScheduledExecutorService metadataExecutor;

    public MetadataBackupServer() {
        metadataExecutor = Executors.newSingleThreadScheduledExecutor(new PooledThreadFactory("metadata_backup"));
    }

    @Override
    public void start() {
        // read from config
        String zkHost = "";
        String zkPath = "";
        String metadataPath = "";
        long backupCycle = 3000L;

        metadataExecutor.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                TimeWatcher watcher = new TimeWatcher();

                MetadataBackupEngine backupEngine = new DefaultMetadataBackupEngine(zkHost, zkPath, Math.max(2, Runtime.getRuntime().availableProcessors() / 4));
                ZNode root = backupEngine.backup();

                try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(metadataPath))) {
                    if (root != null) {
                        oos.writeObject(root);
                        LOG.info("metadata backup success, path: {}, cost time: {} ms", metadataPath, watcher.getElapsedTime());
                    }
                } catch (IOException e) {
                    LOG.error("metadata executor error", e);
                }
            }
        }, 3000, backupCycle, TimeUnit.MILLISECONDS);
    }

    @Override
    public void stop() throws Exception {
        if (metadataExecutor != null) {
            metadataExecutor.shutdown();
        }
    }
}
