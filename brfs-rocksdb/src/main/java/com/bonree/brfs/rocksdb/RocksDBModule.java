package com.bonree.brfs.rocksdb;

import com.bonree.brfs.common.plugin.BrfsModule;
import com.bonree.brfs.rocksdb.backup.RocksDBBackupEngine;
import com.bonree.brfs.rocksdb.connection.RegionNodeConnectionPool;
import com.bonree.brfs.rocksdb.connection.http.HttpRegionNodeConnectionPool;
import com.bonree.brfs.rocksdb.impl.DefaultRocksDBManager;
import com.bonree.brfs.rocksdb.listener.ColumnFamilyInfoListener;
import com.bonree.brfs.rocksdb.restore.RocksDBRestoreEngine;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date 2020/4/3 14:42
 * @Author: <a href=mailto:zhangqi@bonree.com>张奇</a>
 * @Description:
 ******************************************************************************/
public class RocksDBModule implements BrfsModule {

    private static final Logger LOG = LoggerFactory.getLogger(RocksDBModule.class);

    @Override
    public void configure(Binder binder) {
        binder.bind(RegionNodeConnectionPool.class).to(HttpRegionNodeConnectionPool.class).in(Scopes.SINGLETON);
        binder.bind(RocksDBManager.class).to(DefaultRocksDBManager.class).in(Scopes.SINGLETON);

        binder.bind(ColumnFamilyInfoListener.class).in(Scopes.SINGLETON);
        binder.bind(RocksDBBackupEngine.class).in(Scopes.SINGLETON);
        binder.bind(RocksDBRestoreEngine.class).in(Scopes.SINGLETON);

    }
}
