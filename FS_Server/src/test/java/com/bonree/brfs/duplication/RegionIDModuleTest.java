package com.bonree.brfs.duplication;

import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.lifecycle.Lifecycle;
import com.bonree.brfs.common.net.tcp.client.AsyncTcpClientGroup;
import com.bonree.brfs.common.plugin.NodeType;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.service.impl.DefaultServiceManager;
import com.bonree.brfs.common.zookeeper.curator.cache.CuratorCacheFactory;
import com.bonree.brfs.configuration.SystemProperties;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnectionPool;
import com.bonree.brfs.duplication.datastream.connection.tcp.TcpDiskNodeConnectionPool;
import com.bonree.brfs.duplication.filenode.FileNodeStorer;
import com.bonree.brfs.duplication.filenode.duplicates.DuplicateNodeSelector;
import com.bonree.brfs.duplication.filenode.duplicates.impl.RandomSelector;
import com.bonree.brfs.duplication.filenode.duplicates.impl.ResourceWriteSelector;
import com.bonree.brfs.duplication.filenode.zk.ZkFileNodeStorer;
import com.bonree.brfs.guice.ClusterConfig;
import com.bonree.brfs.guice.Initialization;
import com.bonree.brfs.identification.SecondIdsInterface;
import com.bonree.brfs.identification.VirtualServerID;
import com.bonree.brfs.partition.DiskPartitionInfoManager;
import com.bonree.brfs.rebalance.route.RouteLoader;
import com.bonree.brfs.resourceschedule.utils.LibUtils;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.inject.Singleton;
import org.apache.commons.io.FileUtils;
import org.apache.curator.framework.CuratorFramework;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/*******************************************************************************
 * 版权信息： 北京博睿宏远数据科技股份有限公司
 * Copyright (c) 2007-2020 北京博睿宏远数据科技股份有限公司，Inc. All Rights Reserved.
 * @date 2020年04月08日 10:18:53
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description:
 ******************************************************************************/

public class RegionIDModuleTest {
    private static final String rootPath = RegionIDModuleTest.class.getResource("/").getPath();

    @Before
    public void initProperties() {
        String config = rootPath + "/server_zcg.properties";
        String idsDir = rootPath + "/ids";
        checkAndCreateDir(idsDir);
        String partitionIdsDir = rootPath + "/partitionIds";
        checkAndCreateDir(partitionIdsDir);
        String rootDir = rootPath + "/data";
        checkAndCreateDir(rootDir);
        String libPath = "D:\\work\\Business\\bonree\\BrfsSecond\\BRFS\\lib";
        try {
            LibUtils.loadLibraryPath(libPath);
        } catch (Exception e) {
            Assert.fail("load lib happen error");
            e.printStackTrace();
        }
        System.setProperty(SystemProperties.PROP_CONFIGURATION_FILE, config);
        System.setProperty(SystemProperties.PROP_SERVER_ID_DIR, idsDir);
        System.setProperty(SystemProperties.PROP_RESOURCE_LIB_PATH, libPath);
        System.setProperty(SystemProperties.PROP_PARTITION_ID_IDR, partitionIdsDir);
        System.out.println(rootPath);

    }

    private void checkAndCreateDir(String path) {
        File dir = new File(path);
        if (!dir.exists()) {
            try {
                FileUtils.forceMkdir(dir);
            } catch (IOException e) {
                Assert.fail("create [ " + path + " ] fail !!!");
            }
        }
    }

    @Test
    public void init() {
        List<Module> modules = new ArrayList<>();
        modules.add(new ZKPathModel());
        modules.add(new RegionIDModule());
        Injector injector =
            Initialization.makeInjectorWithModules(NodeType.REGION_NODE, Initialization.makeSetupInjector(), modules);
        CuratorFramework client = injector.getInstance(CuratorFramework.class);
        CuratorCacheFactory.init(client);
        VirtualServerID virtualServerID = injector.getInstance(VirtualServerID.class);
        System.out.println(virtualServerID.getVirtualIdContainerPath());
        SecondIdsInterface secondIdsInterface = injector.getInstance(SecondIdsInterface.class);
        DiskPartitionInfoManager manager = injector.getInstance(DiskPartitionInfoManager.class);
        try {
            manager.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.printf("aa", manager.freeSizeSelector());
        RouteLoader loader = injector.getInstance(RouteLoader.class);
        try {
            System.out.println(loader.loadVirtualRoutes(0));
        } catch (Exception e) {
            e.printStackTrace();
        }
        DuplicateNodeSelector selector = injector.getInstance(DuplicateNodeSelector.class);
        if (selector instanceof ResourceWriteSelector) {
            System.out.println("resource");
        } else if (selector instanceof RandomSelector) {
            System.out.println("random");
        } else {
            System.out.println("what happen");
        }
        System.out.println(selector.getDuplicationNodes(0, 2).length);

    }

    private class ZKPathModel implements Module {

        @Override
        public void configure(Binder binder) {
            binder.bind(ServiceManager.class).to(DefaultServiceManager.class).in(Scopes.SINGLETON);
            binder.bind(DiskNodeConnectionPool.class).to(TcpDiskNodeConnectionPool.class).in(Scopes.SINGLETON);
            binder.bind(FileNodeStorer.class).to(ZkFileNodeStorer.class).in(Scopes.SINGLETON);
        }

        @Provides
        @Singleton
        public ZookeeperPaths getPaths(ClusterConfig clusterConfig, CuratorFramework zkClient, Lifecycle lifecycle) {
            ZookeeperPaths paths = ZookeeperPaths.create(clusterConfig.getName(), zkClient);
            lifecycle.addAnnotatedInstance(paths);
            return paths;
        }

        @Provides
        @Singleton
        public TcpDiskNodeConnectionPool getTcpConnectionPool(
            ServiceManager serviceManager,
            ConnectionPoolConfig config,
            Lifecycle lifecycle) {
            AsyncTcpClientGroup tcpClientGroup = new AsyncTcpClientGroup(config.getWriteWorkerThreads());
            TcpDiskNodeConnectionPool connectionPool = new TcpDiskNodeConnectionPool(serviceManager, tcpClientGroup);

            lifecycle.addCloseable(tcpClientGroup);

            return connectionPool;
        }
    }
}
