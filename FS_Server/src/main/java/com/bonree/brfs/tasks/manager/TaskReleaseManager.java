package com.bonree.brfs.tasks.manager;

import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.lifecycle.LifecycleStart;
import com.bonree.brfs.common.lifecycle.LifecycleStop;
import com.bonree.brfs.common.lifecycle.ManageLifecycle;
import com.bonree.brfs.common.process.LifeCycle;
import com.bonree.brfs.common.utils.StringUtils;
import com.bonree.brfs.common.zookeeper.curator.CuratorConfig;
import com.bonree.brfs.common.zookeeper.curator.CuratorModule;
import com.bonree.brfs.configuration.ResourceTaskConfig;
import com.bonree.brfs.schedulers.MetaTaskLeaderManager;
import com.google.inject.Inject;
import java.nio.charset.StandardCharsets;
import org.apache.curator.ensemble.fixed.FixedEnsembleProvider;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.GzipCompressionProvider;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;
import org.apache.curator.shaded.com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 任务发布管理器，负责各个系统任务的创建
 */
@ManageLifecycle
public class TaskReleaseManager implements LifeCycle {
    private static final Logger LOG = LoggerFactory.getLogger(TaskReleaseManager.class);
    private MetaTaskLeaderManager manager;
    private LeaderLatch leaderLatch = null;
    private ResourceTaskConfig config;
    private CuratorFramework client;
    private ZookeeperPaths zkPaths;

    @Inject
    public TaskReleaseManager(
        MetaTaskLeaderManager manager,
        CuratorFramework client, ZookeeperPaths zkPaths, ResourceTaskConfig config) {
        this.manager = manager;
        this.client = client;
        this.zkPaths = zkPaths;
        this.config = config;
    }

    @LifecycleStart
    @Override
    public void start() throws Exception {
        if (config.isTaskFrameWorkSwitch()) {
            this.leaderLatch =
                new LeaderLatch(
                    client, zkPaths.getBaseLocksPath() + "/TaskManager/MetaTaskLeaderLock");
            leaderLatch.addListener(manager);
            leaderLatch.start();
            LOG.info("task release manager start .");
        } else {
            LOG.info("task framework switch is {}", config.isTaskFrameWorkSwitch());
        }

    }

    @LifecycleStop
    @Override
    public void stop() throws Exception {
        if (leaderLatch != null) {
            leaderLatch.removeListener(manager);
            leaderLatch.close();
            this.client.close();
            LOG.info("task release manager stop .");
        } else {
            LOG.info("task framework switch is {}", config.isTaskFrameWorkSwitch());
        }
    }
}
