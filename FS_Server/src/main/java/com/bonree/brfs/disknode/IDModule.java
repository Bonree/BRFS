package com.bonree.brfs.disknode;

import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.identification.SecondMaintainerInterface;
import com.bonree.brfs.identification.VirtualServerID;
import com.bonree.brfs.identification.impl.FirstLevelServerIDImpl;
import com.bonree.brfs.identification.impl.SimpleSecondMaintainer;
import com.bonree.brfs.identification.impl.VirtualServerIDImpl;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import org.apache.curator.framework.CuratorFramework;

/**
 * 版权信息: 北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007-2020 北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 *
 * @date: 2020年04月06日 09:57
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description:
 **/
public class IDModule implements Module {
    @Override
    public void configure(Binder binder) {

    }
    @Provides
    @Singleton
    public FirstLevelServerIDImpl getFirstLevelServerIDImpl(CuratorFramework client,
                                                            ZookeeperPaths path,String idFiles){
        return new FirstLevelServerIDImpl(client,path.getBaseServerIdPath(),idFiles,path.getBaseSequencesPath());
    }
    @Provides
    @Singleton
    public VirtualServerIDImpl getVirtualServerId(CuratorFramework client,
                                              ZookeeperPaths path){
        return new VirtualServerIDImpl(client,path.getBaseServerIdSeqPath());
    }
    @Provides
    @Singleton
    public SecondMaintainerInterface getSecondMaintainer(CuratorFramework client,
                                                         ZookeeperPaths path,FirstLevelServerIDImpl firstLevelServerID){
        return new SimpleSecondMaintainer(client,path.getBaseV2SecondIDPath(),path.getBaseV2RoutePath(),path.getBaseServerIdSeqPath(),firstLevelServerID.initOrLoadServerID());
    }
}
