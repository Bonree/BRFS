package com.bonree.brfs.disknode;

import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.guice.JsonConfigProvider;
import com.bonree.brfs.common.lifecycle.LifecycleModule;
import com.bonree.brfs.common.lifecycle.ManageLifecycle;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.task.TaskType;
import com.bonree.brfs.configuration.ResourceTaskConfig;
import com.bonree.brfs.duplication.storageregion.StorageRegionManager;
import com.bonree.brfs.identification.IDSManager;
import com.bonree.brfs.identification.impl.DiskDaemon;
import com.bonree.brfs.rebalance.route.RouteCache;
import com.bonree.brfs.rebalance.route.RouteLoader;
import com.bonree.brfs.rebalance.route.impl.SimpleRouteZKLoader;
import com.bonree.brfs.resource.ResourceGatherInterface;
import com.bonree.brfs.resource.ResourceRegisterInterface;
import com.bonree.brfs.resource.impl.LocalResourceGather;
import com.bonree.brfs.resource.impl.ZKResourceRegister;
import com.bonree.brfs.resource.vo.LimitServerResource;
import com.bonree.brfs.schedulers.ManagerContralFactory;
import com.bonree.brfs.schedulers.MetaTaskLeaderManager;
import com.bonree.brfs.schedulers.task.manager.MetaTaskManagerInterface;
import com.bonree.brfs.schedulers.task.manager.RunnableTaskInterface;
import com.bonree.brfs.schedulers.task.manager.SchedulerManagerInterface;
import com.bonree.brfs.schedulers.task.manager.impl.DefaultReleaseTask;
import com.bonree.brfs.schedulers.task.manager.impl.DefaultRunnableTask;
import com.bonree.brfs.schedulers.task.manager.impl.DefaultSchedulersManager;
import com.bonree.brfs.tasks.maintain.FileBlockMaintainer;
import com.bonree.brfs.tasks.maintain.ResourceMaintainer;
import com.bonree.brfs.tasks.manager.TaskOpertionManager;
import com.bonree.brfs.tasks.manager.TaskReleaseManager;
import com.bonree.brfs.tasks.monitor.RebalanceTaskMonitor;
import com.bonree.brfs.tasks.monitor.impl.CycleRebalanceTaskMonitor;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import java.util.ArrayList;
import java.util.List;
import org.apache.curator.framework.CuratorFramework;

/**
 * 版权信息: 北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007-2020 北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 *
 * @date: 2020年04月19日 21:49
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description:
 **/
public class TaskModule implements Module {
    @Override
    public void configure(Binder binder) {
        JsonConfigProvider.bind(binder, "datanode.resource", ResourceConfig.class);
        binder.bind(ResourceGatherInterface.class).to(LocalResourceGather.class).in(Singleton.class);
        binder.bind(ResourceRegisterInterface.class).to(ZKResourceRegister.class);
        binder.bind(RebalanceTaskMonitor.class).to(CycleRebalanceTaskMonitor.class).in(Singleton.class);

        binder.bind(ResourceTaskConfig.class);
        binder.bind(RunnableTaskInterface.class).to(DefaultRunnableTask.class).in(Singleton.class);
        binder.bind(MetaTaskManagerInterface.class).to(DefaultReleaseTask.class).in(Singleton.class);
        binder.bind(MetaTaskLeaderManager.class).in(Singleton.class);
        binder.bind(RouteLoader.class).to(SimpleRouteZKLoader.class).in(Singleton.class);
        binder.bind(SchedulerManagerInterface.class).to(DefaultSchedulersManager.class).in(Singleton.class);

        binder.bind(CycleRebalanceTaskMonitor.class).in(ManageLifecycle.class);
        binder.bind(FileBlockMaintainer.class).in(ManageLifecycle.class);
        binder.bind(TaskReleaseManager.class).in(ManageLifecycle.class);
        binder.bind(TaskOpertionManager.class).in(ManageLifecycle.class);
        binder.bind(DefaultSchedulersManager.class).in(ManageLifecycle.class);
        binder.bind(ResourceMaintainer.class).in(ManageLifecycle.class);

        LifecycleModule.register(binder, CycleRebalanceTaskMonitor.class);
        LifecycleModule.register(binder, FileBlockMaintainer.class);
        LifecycleModule.register(binder, TaskReleaseManager.class);
        LifecycleModule.register(binder, TaskOpertionManager.class);
        LifecycleModule.register(binder, DefaultSchedulersManager.class);
        LifecycleModule.register(binder, ResourceMaintainer.class);
    }

    @Provides
    @Singleton
    public ManagerContralFactory getManagerContralFactory(
        ResourceTaskConfig managerConfig,
        ZookeeperPaths zkPath,
        ServiceManager sm,
        StorageRegionManager snm,
        IDSManager sim,
        Service localServer,
        DiskDaemon diskDaemon,
        RebalanceTaskMonitor taskMonitor,
        RouteCache routeCache,
        SchedulerManagerInterface manager,
        LimitServerResource lmit,
        MetaTaskManagerInterface release,
        RunnableTaskInterface run) throws Exception {
        managerConfig.printDetail();
        ManagerContralFactory mcf = ManagerContralFactory.getInstance();
        mcf.setTm(release);
        mcf.setRt(run);
        mcf.setZkPath(zkPath);
        mcf.setDaemon(diskDaemon);
        mcf.setServerId(localServer.getServiceId());
        mcf.setGroupName(localServer.getServiceGroup());
        mcf.setLimitServerResource(lmit);
        mcf.setRouteCache(routeCache);
        mcf.setRt(run);
        mcf.setSim(sim);
        mcf.setSm(sm);
        mcf.setSnm(snm);
        mcf.setStm(manager);
        mcf.setTaskMonitor(taskMonitor);
        mcf.setTm(release);
        mcf.setZkPath(zkPath);
        // 创建任务线程池
        if (managerConfig.isTaskFrameWorkSwitch()) {
            List<TaskType> tasks = managerConfig.getSwitchOnTaskType();
            if (tasks == null || tasks.isEmpty()) {
                mcf.setTaskOn(new ArrayList<>(0));
            } else {
                mcf.setTaskOn(tasks);
            }
        }
        return mcf;

    }

}
