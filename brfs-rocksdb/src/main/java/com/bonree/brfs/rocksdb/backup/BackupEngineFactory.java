package com.bonree.brfs.rocksdb.backup;

import com.bonree.brfs.common.lifecycle.LifecycleStart;
import com.bonree.brfs.common.lifecycle.LifecycleStop;
import com.bonree.brfs.common.lifecycle.ManageLifecycle;
import com.bonree.brfs.common.process.LifeCycle;
import org.rocksdb.BackupEngine;
import org.rocksdb.BackupableDBOptions;
import org.rocksdb.Env;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/*******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date 2020/3/26 15:34
 * @Author: <a href=mailto:zhangqi@bonree.com>张奇</a>
 * @Description:
 ******************************************************************************/
@ManageLifecycle
public class BackupEngineFactory implements LifeCycle {
    private static final Logger LOG = LoggerFactory.getLogger(BackupEngineFactory.class);

    private Map<String, BackupEngine> backupEngineCache = new ConcurrentHashMap<>();

    private static class SingletonHolder {
        private static final BackupEngineFactory INSTANCE = new BackupEngineFactory();
    }

    private BackupEngineFactory() {
    }

    public static BackupEngineFactory getInstance() {
        return SingletonHolder.INSTANCE;
    }

    /**
     * @description: 根据路径获取BackupEngine实例
     */
    public BackupEngine getBackupEngineByPath(String path) throws RocksDBException {
        if (backupEngineCache.containsKey(path)) {
            return backupEngineCache.get(path);
        }

        BackupableDBOptions backupableDBOptions = new BackupableDBOptions(path);
        BackupEngine backupEngine = BackupEngine.open(Env.getDefault(), backupableDBOptions);
        backupEngineCache.put(path, backupEngine);
        return backupEngine;
    }

    @LifecycleStart
    @Override
    public void start() throws Exception {

    }

    @LifecycleStop
    @Override
    public void stop() {
        for (BackupEngine backupEngine : backupEngineCache.values()) {
            if (backupEngine != null) {
                backupEngine.close();
            }
        }
        LOG.info("backup engine factory close");
    }

}
