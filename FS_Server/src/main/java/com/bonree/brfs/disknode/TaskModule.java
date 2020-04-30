package com.bonree.brfs.disknode;

import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.guice.JsonConfigProvider;
import com.bonree.brfs.common.lifecycle.Lifecycle;
import com.bonree.brfs.common.lifecycle.LifecycleModule;
import com.bonree.brfs.common.lifecycle.ManageLifecycle;
import com.bonree.brfs.rebalance.route.RouteLoader;
import com.bonree.brfs.rebalance.route.impl.SimpleRouteZKLoader;
import com.bonree.brfs.resource.GuiResourceMaintainer;
import com.bonree.brfs.resource.ResourceGatherInterface;
import com.bonree.brfs.resource.ResourceRegisterInterface;
import com.bonree.brfs.resource.impl.GuiFileMaintainer;
import com.bonree.brfs.resource.impl.LocalResourceGather;
import com.bonree.brfs.resource.impl.ZKResourceRegister;
import com.bonree.brfs.tasks.maintain.FileBlockMaintainer;
import com.bonree.brfs.tasks.maintain.ResourceMaintainer;
import com.bonree.brfs.tasks.monitor.RebalanceTaskMonitor;
import com.bonree.brfs.tasks.monitor.impl.CycleRebalanceTaskMonitor;
import com.bonree.brfs.tasks.resource.ResourceTask;
import com.bonree.brfs.tasks.resource.impl.GuiResourcTask;
import com.bonree.brfs.tasks.resource.impl.ResourceRegistTask;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import java.util.Collection;
import java.util.HashSet;
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
        JsonConfigProvider.bind(binder, "datanode.gui", GuiResourceConfig.class);
        JsonConfigProvider.bind(binder, "datanode.resource", ResourceConfig.class);
        binder.bind(ResourceGatherInterface.class).to(LocalResourceGather.class).in(Singleton.class);
        binder.bind(ResourceRegisterInterface.class).to(ZKResourceRegister.class);
        binder.bind(RebalanceTaskMonitor.class).to(CycleRebalanceTaskMonitor.class).in(ManageLifecycle.class);
        binder.bind(GuiResourceMaintainer.class).to(GuiFileMaintainer.class).in(ManageLifecycle.class);
        binder.bind(FileBlockMaintainer.class).in(ManageLifecycle.class);
        LifecycleModule.register(binder, RebalanceTaskMonitor.class);
        LifecycleModule.register(binder, FileBlockMaintainer.class);
        LifecycleModule.register(binder, ResourceMaintainer.class);
    }

    @Provides
    @Singleton
    public RouteLoader getRouteLoader(CuratorFramework client, ZookeeperPaths zookeeperPaths) {
        return new SimpleRouteZKLoader(client, zookeeperPaths.getBaseRoutePath());
    }

    @Provides
    public ResourceMaintainer createResourceMaintainer(
        ResourceGatherInterface resourceGather,
        ResourceRegisterInterface resourceRegister,
        GuiResourceMaintainer resourceMaintainer,
        GuiResourceConfig guiConf,
        ResourceConfig conf,
        Lifecycle lifecycle
    ) {
        Collection<ResourceTask> tasks = new HashSet<>();
        if (conf.isRunFlag()) {
            tasks.add(new ResourceRegistTask(resourceGather, resourceRegister, conf));
        }
        if (guiConf.isRunFlag()) {
            tasks.add(new GuiResourcTask(resourceGather, resourceMaintainer, guiConf));
        }
        ResourceMaintainer maintainer = new ResourceMaintainer(tasks);
        lifecycle.addLifeCycleObject(new Lifecycle.LifeCycleObject() {
            @Override
            public void start() throws Exception {
                maintainer.start();
            }

            @Override
            public void stop() {
                maintainer.stop();
            }
        });
        return maintainer;
    }

}
