package com.bonree.brfs.disknode.check;

import static com.bonree.brfs.common.ZookeeperPaths.ROOT;
import static com.bonree.brfs.common.ZookeeperPaths.SEPARATOR;

import com.bonree.brfs.common.lifecycle.LifecycleStart;
import com.bonree.brfs.common.lifecycle.ManageLifecycle;
import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.DataNodeConfigs;
import com.bonree.brfs.disknode.StorageConfig;
import com.bonree.brfs.guice.ClusterConfig;
import java.io.File;
import java.util.List;
import javax.inject.Inject;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @ClassName DefaultCheckerWhenInitDataNode
 * @Description
 * @Author Tang Daqian
 * @Date 2020/12/23 18:27
 **/
@ManageLifecycle
public class DefaultCheckerWhenInitDataNode {
    private static final Logger log = LoggerFactory.getLogger(DefaultCheckerWhenInitDataNode.class);
    private StorageConfig storageConfig;
    private ClusterConfig clusterConfig;
    private CuratorFramework client;

    @Inject
    public DefaultCheckerWhenInitDataNode(CuratorFramework client,
                                          StorageConfig storageConfig,
                                          ClusterConfig clusterConfig) {
        this.client = client;
        this.storageConfig = storageConfig;
        this.clusterConfig = clusterConfig;
    }

    /**
     * 检查新建集群的文件路径是否为空，若不为空，则抛出异常并停止启动
     * 判断是否是新集群的依据： 通过判断集群的znode创建时间与当前启动时间比较，如果比这个配置的时间小，则认为是新的集群
     *
     */
    @LifecycleStart
    public void start() {
        List<String> storageDirs = storageConfig.getStorageDirs();
        String clusterPath = SEPARATOR + ROOT + SEPARATOR + clusterConfig.getName();
        Stat stat = null;
        try {
            stat = client.checkExists().forPath(clusterPath);
        } catch (Exception e) {
            log.error("get znode info for zkPath [{}] failed, giving up check file directory", clusterPath);
            return;
        }
        if (stat == null) {
            checkFileExists(storageDirs);
        }

        long creatime = stat.getCtime();
        if (System.currentTimeMillis() - creatime < Configs.getConfiguration().getConfig(
            DataNodeConfigs.CHECK_FILE_ISEMPTY_TIMEDELAY) * 1000) {
            checkFileExists(storageDirs);
        }
    }

    private void checkFileExists(List<String> storageDirs) {
        storageDirs.forEach(dirs -> {
            File file = new File(dirs);
            if (file.isDirectory()) {
                if (file.listFiles().length != 0) {
                    throw new RuntimeException("new cluster ["
                                                   + clusterConfig.getName()
                                                   + "] file dir ["
                                                   + dirs
                                                   + "] is not empty! "
                                                   + "Please choose empty dirs for brfs's file content");
                }
            }
        });
    }
}
