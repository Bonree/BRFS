package com.bonree.brfs.tasks.manager;

import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.lifecycle.LifecycleStart;
import com.bonree.brfs.common.lifecycle.LifecycleStop;
import com.bonree.brfs.common.lifecycle.ManageLifecycle;
import com.bonree.brfs.common.process.LifeCycle;
import com.bonree.brfs.disknode.TaskConfig;
import com.bonree.brfs.schedulers.MetaTaskLeaderManager;
import com.google.inject.Inject;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
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
    private TaskConfig taskConfig;
    private CuratorFramework client;
    private ZookeeperPaths zkPaths;

    @Inject
    public TaskReleaseManager(
        MetaTaskLeaderManager manager,
        CuratorFramework client,
        ZookeeperPaths zkPaths,
        TaskConfig taskConfig) {
        this.manager = manager;
        this.client = client;
        this.zkPaths = zkPaths;
        this.taskConfig = taskConfig;
    }

    @LifecycleStart
    @Override
    public void start() throws Exception {
        if (!taskConfig.getTaskTypeSwitch().isEmpty()) {
            this.leaderLatch =
                new LeaderLatch(
                    client, zkPaths.getBaseLocksPath() + "/TaskManager/MetaTaskLeaderLock", "TaskReleaseManager");
            leaderLatch.addListener(manager);
            leaderLatch.start();
            LOG.info("task release manager start .");
        } else {
            LOG.info("no task need to release ");
        }

    }

    @LifecycleStop
    @Override
    public void stop() throws Exception {
        if (leaderLatch != null) {
            leaderLatch.removeListener(manager);
            leaderLatch.close();
            LOG.info("task release manager stop .");
        } else {
            LOG.info("no task need to stop");
        }
    }
}
