package com.bonree.brfs.tasks.maintain;

import com.bonree.brfs.common.lifecycle.LifecycleStart;
import com.bonree.brfs.common.lifecycle.LifecycleStop;
import com.bonree.brfs.common.lifecycle.ManageLifecycle;
import com.bonree.brfs.common.process.LifeCycle;
import com.bonree.brfs.common.task.TaskState;
import com.bonree.brfs.tasks.resource.ResourceTask;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.Collection;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import javax.inject.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 资源采集类，
 */
@ManageLifecycle
public class ResourceMaintainer implements LifeCycle {
    private static final Logger LOG = LoggerFactory.getLogger(ResourceMaintainer.class);
    private ScheduledExecutorService pool = null;
    private Collection<ResourceTask> tasks = null;

    @Inject
    public ResourceMaintainer(@Named("resourceTasks") Collection<ResourceTask> tasks) {
        this.tasks = tasks;
    }

    @LifecycleStart
    @Override
    public void start() throws Exception {
        if (tasks == null || tasks.isEmpty()) {
            LOG.warn("no resource task !! no need work");
            return;
        }
        pool = Executors
            .newScheduledThreadPool(tasks.size(), new ThreadFactoryBuilder().setNameFormat("ResourceMaintainer").build());
        tasks.stream().forEach(x -> {
            x.setStatus(TaskState.RUN);
            long delay = 60 - System.currentTimeMillis() / 1000 % 60;
            pool.scheduleAtFixedRate(x, delay, x.getIntervalSecond(), TimeUnit.SECONDS);
        });
        LOG.info("resource pool start");

    }

    @LifecycleStop
    @Override
    public void stop() {
        if (tasks == null || tasks.isEmpty()) {
            LOG.warn("no resource task !! no need stop!!");
            return;
        }
        if (tasks != null && !tasks.isEmpty()) {
            tasks.stream().forEach(x -> {
                x.setStatus(TaskState.PAUSE);
            });
        }
        if (pool != null) {
            pool.shutdownNow();
        }
        LOG.info("resource pool stop");
    }
}