package com.bonree.brfs.disknode;

import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.lifecycle.Lifecycle;
import com.bonree.brfs.common.zookeeper.curator.CuratorModule;
import com.bonree.brfs.common.zookeeper.curator.cache.CuratorCacheFactory;
import com.bonree.brfs.configuration.SystemProperties;
import com.bonree.brfs.guice.ClusterConfig;
import com.bonree.brfs.guice.Initialization;
import com.bonree.brfs.identification.IDSManager;
import com.bonree.brfs.identification.impl.DiskDaemon;
import com.bonree.brfs.resourceschedule.utils.LibUtils;
import com.google.inject.*;
import org.apache.commons.io.FileUtils;
import org.apache.curator.framework.CuratorFramework;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.inject.Singleton;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/*******************************************************************************
 * 版权信息： 北京博睿宏远数据科技股份有限公司
 * Copyright (c) 2007-2020 北京博睿宏远数据科技股份有限公司，Inc. All Rights Reserved.
 * @date 2020年04月07日 14:29:13
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description: 测试id利用guice加载实例
 ******************************************************************************/

public class IDLoadTest {
    private static final String rootPath = IDLoadTest.class.getResource("/").getPath();
    @Before
    public void initProperties(){
        String config = rootPath+"/server_zcg.properties";
        String idsDir = rootPath+"/ids";
        checkAndCreateDir(idsDir);
        String partitionIdsDir = rootPath+"/partitionIds";
        checkAndCreateDir(partitionIdsDir);
        String rootDir = rootPath+"/data";
        checkAndCreateDir(rootDir);
        String libPath = "D:\\work\\Business\\bonree\\BrfsSecond\\BRFS\\lib";
        try {
            LibUtils.loadLibraryPath(libPath);
        } catch (Exception e) {
            Assert.fail("load lib happen error");
            e.printStackTrace();
        }
        System.setProperty(SystemProperties.PROP_CONFIGURATION_FILE,config);
        System.setProperty(SystemProperties.PROP_SERVER_ID_DIR,idsDir);
        System.setProperty(SystemProperties.PROP_RESOURCE_LIB_PATH,libPath);
        System.setProperty(SystemProperties.PROP_PARTITION_ID_IDR,partitionIdsDir);
        System.out.println(rootPath);

    }
    private void checkAndCreateDir(String path){
        File dir = new File(path);
        if(!dir.exists()){
            try {
                FileUtils.forceMkdir(dir);
            } catch (IOException e) {
                Assert.fail("create [ "+path+" ] fail !!!");
            }
        }
    }
    @Test
    public void init(){
        List<Module> modules = new ArrayList<>();
        modules.add(new ZKPathModel());
        modules.add(new IDModule());
        Injector injector = Initialization.makeInjectorWithModules(Initialization.makeSetupInjector(), modules);
        CuratorFramework client = injector.getInstance(CuratorFramework.class);
        CuratorCacheFactory.init(client);
        DiskDaemon diskDaemon = injector.getInstance(DiskDaemon.class);
        String partitionId = diskDaemon.getPartitionId("D:/tmp/data");
        System.out.println(partitionId);
        IDSManager idsManager = injector.getInstance(IDSManager.class);
        String firstServer = idsManager.getFirstSever();
        System.out.println(firstServer);
    }
    class ZKPathModel implements Module{

        @Override
        public void configure(Binder binder) {

        }
        @Provides
        @Singleton
        public ZookeeperPaths getPaths(ClusterConfig clusterConfig, CuratorFramework zkClient, Lifecycle lifecycle) {
            ZookeeperPaths paths = ZookeeperPaths.create(clusterConfig.getName(), zkClient);
            lifecycle.addAnnotatedInstance(paths);

            return paths;
        }
    }
}
