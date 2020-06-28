package com.bonree.brfs.rocksdb.guice;

import static com.bonree.brfs.common.http.rest.JaxrsBinder.jaxrs;

import com.bonree.brfs.common.guice.JsonConfigProvider;
import com.bonree.brfs.common.lifecycle.Lifecycle;
import com.bonree.brfs.common.lifecycle.LifecycleModule;
import com.bonree.brfs.common.lifecycle.ManageLifecycle;
import com.bonree.brfs.common.plugin.BrfsModule;
import com.bonree.brfs.common.plugin.NodeType;
import com.bonree.brfs.common.rocksdb.RocksDBManager;
import com.bonree.brfs.rocksdb.connection.RegionNodeConnectionPool;
import com.bonree.brfs.rocksdb.connection.http.HttpRegionNodeConnectionPool;
import com.bonree.brfs.rocksdb.impl.DefaultRocksDBManager;
import com.bonree.brfs.rocksdb.listener.ColumnFamilyInfoListener;
import com.google.inject.Binder;
import com.google.inject.Provides;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import javax.inject.Singleton;

/*******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date 2020/4/3 14:42
 * @Author: <a href=mailto:zhangqi@bonree.com>张奇</a>
 * @Description:
 ******************************************************************************/
public class RocksDBModule extends BrfsModule {

    @Override
    public void configure(NodeType nodeType, Binder binder) {
        JsonConfigProvider.bind(binder, "rocksdb", RocksDBConfig.class);

        binder.bind(RegionNodeConnectionPool.class).to(HttpRegionNodeConnectionPool.class).in(ManageLifecycle.class);
        binder.bind(RocksDBManager.class).to(DefaultRocksDBManager.class).in(ManageLifecycle.class);
        LifecycleModule.register(binder, ColumnFamilyInfoListener.class);

        jaxrs(binder).resource(RocksDBResource.class);

    }

    @Provides
    @Singleton
    @RocksDBRead
    public ExecutorService getDBreadExecs(Lifecycle lifecycle) {
        int threadNum = Math.max(1, Runtime.getRuntime().availableProcessors() / 2);
        ExecutorService execs = Executors.newFixedThreadPool(threadNum, new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "rocksdb_reader");
                t.setDaemon(true);

                return t;
            }
        });

        lifecycle.addCloseable(execs::shutdown);

        return execs;
    }
}
