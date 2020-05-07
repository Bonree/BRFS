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
    private static final int BASE_SLEEP_TIME_MS = 1000;
    private static final int MAX_SLEEP_TIME_MS = 45000;
    private static final int MAX_RETRIES = 30;
    private static final Logger LOG = LoggerFactory.getLogger(TaskReleaseManager.class);
    private MetaTaskLeaderManager manager;
    private LeaderLatch leaderLatch = null;
    private ResourceTaskConfig config;
    private CuratorFramework client;
    private ZookeeperPaths zkPaths;

    @Inject
    public TaskReleaseManager(
        MetaTaskLeaderManager manager,
        CuratorConfig curatorConfig, ZookeeperPaths zkPaths, ResourceTaskConfig config) {
        this.manager = manager;
        this.client = create(curatorConfig);
        this.zkPaths = zkPaths;
        this.config = config;
    }

    @LifecycleStart
    @Override
    public void start() throws Exception {
        if (config.isTaskFrameWorkSwitch()) {
            this.client.start();
            this.client.blockUntilConnected();
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

    private CuratorFramework create(CuratorConfig config) {
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();
        if (!Strings.isNullOrEmpty(config.getZkUser()) && !Strings.isNullOrEmpty(config.getZkPasswd())) {
            builder.authorization(
                config.getAuthScheme(),
                StringUtils.format("%s:%s", config.getZkUser(), config.getZkPasswd()).getBytes(StandardCharsets.UTF_8)
            );
        }

        if (config.isEnableCompression()) {
            builder.compressionProvider(new GzipCompressionProvider());
        }

        CuratorFramework framework = builder
            .ensembleProvider(new FixedEnsembleProvider(config.getAddresses()))
            .sessionTimeoutMs(config.getZkSessionTimeoutMs())
            .retryPolicy(new BoundedExponentialBackoffRetry(BASE_SLEEP_TIME_MS, MAX_SLEEP_TIME_MS, MAX_RETRIES))
            .build();
        return framework;
    }

    @LifecycleStop
    @Override
    public void stop() throws Exception {
        if (config.isTaskFrameWorkSwitch()) {
            leaderLatch.removeListener(manager);
            leaderLatch.close();
            this.client.close();
            LOG.info("task release manager stop .");
        } else {
            LOG.info("task framework switch is {}", config.isTaskFrameWorkSwitch());
        }
    }
}
