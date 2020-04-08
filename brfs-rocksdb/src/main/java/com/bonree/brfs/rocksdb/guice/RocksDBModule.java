package com.bonree.brfs.rocksdb.guice;

import com.bonree.brfs.common.guice.JsonConfigProvider;
import com.bonree.brfs.common.lifecycle.Lifecycle;
import com.bonree.brfs.common.lifecycle.LifecycleModule;
import com.bonree.brfs.common.lifecycle.ManageLifecycle;
import com.bonree.brfs.common.plugin.BrfsModule;
import com.bonree.brfs.rocksdb.RocksDBManager;
import com.bonree.brfs.rocksdb.backup.BackupEngineFactory;
import com.bonree.brfs.rocksdb.backup.RocksDBBackupEngine;
import com.bonree.brfs.rocksdb.connection.RegionNodeConnectionPool;
import com.bonree.brfs.rocksdb.connection.http.HttpRegionNodeConnectionPool;
import com.bonree.brfs.rocksdb.impl.DefaultRocksDBManager;
import com.bonree.brfs.rocksdb.listener.ColumnFamilyInfoListener;
import com.bonree.brfs.rocksdb.restore.RocksDBRestoreEngine;
import com.google.inject.Binder;
import com.google.inject.Provides;

import static com.bonree.brfs.common.http.rest.JaxrsBinder.jaxrs;

/*******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date 2020/4/3 14:42
 * @Author: <a href=mailto:zhangqi@bonree.com>张奇</a>
 * @Description:
 ******************************************************************************/
public class RocksDBModule implements BrfsModule {

    @Override
    public void configure(Binder binder) {
        JsonConfigProvider.bind(binder, "rocksdb", RocksDBConfig.class);

        binder.bind(RegionNodeConnectionPool.class).to(HttpRegionNodeConnectionPool.class).in(ManageLifecycle.class);
        binder.bind(RocksDBManager.class).to(DefaultRocksDBManager.class).in(ManageLifecycle.class);
        binder.bind(RocksDBBackupEngine.class).in(ManageLifecycle.class);

        LifecycleModule.register(binder, ColumnFamilyInfoListener.class);
        LifecycleModule.register(binder, RocksDBRestoreEngine.class);

        LifecycleModule.register(binder, BackupEngineFactory.class);
        jaxrs(binder).resource(RocksDBResource.class);

    }

    @Provides
    public BackupEngineFactory getBackupEngineFactory(Lifecycle lifecycle) {
        lifecycle.addLifeCycleObject(new Lifecycle.LifeCycleObject() {
            @Override
            public void start() throws Exception {

            }

            @Override
            public void stop() {
                BackupEngineFactory.getInstance().stop();
            }
        });
        return BackupEngineFactory.getInstance();
    }
}
