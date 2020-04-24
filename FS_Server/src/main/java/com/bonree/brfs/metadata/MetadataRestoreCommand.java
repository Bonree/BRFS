package com.bonree.brfs.metadata;

import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.utils.FileUtils;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.common.zookeeper.curator.cache.CuratorCacheFactory;
import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.CommonConfigs;
import com.bonree.brfs.metadata.restore.DefaultMetadataRestoreEngine;
import com.bonree.brfs.metadata.restore.MetadataRestoreEngine;
import io.airlift.airline.Command;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.ObjectInputStream;

/*******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date 2020/4/17 15:23
 * @Author: <a href=mailto:zhangqi@bonree.com>张奇</a>
 * @Description:
 ******************************************************************************/

@Command(
        name = "restore",
        description = "restore brfs zk metadata"
)
public class MetadataRestoreCommand implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(MetadataRestoreCommand.class);

    @Override
    public void run() {
        String zkHost = Configs.getConfiguration().getConfig(CommonConfigs.CONFIG_ZOOKEEPER_ADDRESSES);
        CuratorClient client = CuratorClient.getClientInstance(zkHost);
        ZookeeperPaths zkPaths = ZookeeperPaths.create(Configs.getConfiguration().getConfig(CommonConfigs.CONFIG_CLUSTER_NAME), client.getInnerClient());
        CuratorCacheFactory.init(client.getInnerClient());

        String zkPath = zkPaths.getBaseClusterName();
        String metadataPath = Configs.getConfiguration().getConfig(CommonConfigs.CONFIG_METADATA_BACKUP_PATH) + "/brfs.metadata";

        ZooKeeper zookeeper = null;

        if (!FileUtils.isExist(metadataPath)) {
            LOG.info("metadata backup file not exists!");
            System.exit(1);
        }
        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(metadataPath))) {
            ZNode root = (ZNode) ois.readObject();
            if (root != null) {
                zookeeper = new ZooKeeper(zkHost, 40000, new LoggingWatcher());
                MetadataRestoreEngine restoreEngine = new DefaultMetadataRestoreEngine(zookeeper, zkPath, root, true, true, -1, 1000);
                restoreEngine.restore();
                LOG.info("restore brfs zk metadata complete.");
            }
        } catch (Exception e) {
            LOG.error("restore brfs zk metadata failed", e);
            System.exit(1);
        } finally {
            if (zookeeper != null) {
                try {
                    zookeeper.close();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
