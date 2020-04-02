package com.bonree.brfs.disknode;

import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.configuration.units.DataNodeConfigs;
import com.bonree.brfs.identification.impl.DiskDaemon;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import org.apache.curator.framework.CuratorFramework;
/*******************************************************************************
 * 版权信息： 北京博睿宏远数据科技股份有限公司
 * Copyright (c) 2007-2020 北京博睿宏远数据科技股份有限公司，Inc. All Rights Reserved.
 * @date 2020年04月02日 10:01:14
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description: 磁盘功能连接映射
 ******************************************************************************/

public class PartitionModule implements Module {
    @Override
    public void configure(Binder binder) {

    }

    @Provides
    @Singleton
    public DiskDaemon getDiskDaemon(CuratorFramework client, Service localService, ZookeeperPaths zkPaths, DataNodeConfigs dataNodeConfigs){
        return null;
    }
}
