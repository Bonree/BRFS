package com.bonree.brfs.rocksdb.guice;

import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.RocksDBConfigs;
import com.fasterxml.jackson.annotation.JsonProperty;
import sun.security.krb5.Config;

/*******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date 2020/4/3 17:04
 * @Author: <a href=mailto:zhangqi@bonree.com>张奇</a>
 * @Description:
 ******************************************************************************/
public class RocksDBConfig {
    @JsonProperty("storage.path")
    public String rocksDBStoragePath = Configs.getConfiguration().getConfig(RocksDBConfigs.ROCKSDB_STORAGE_PATH);

    @JsonProperty("backup.path")
    public String rocksDBBackupPath = Configs.getConfiguration().getConfig(RocksDBConfigs.ROCKSDB_BACKUP_PATH);

    @JsonProperty("syncer.num")
    public int rocksDBSyncerNum = Configs.getConfiguration().getConfig(RocksDBConfigs.ROCKSDB_SYNCER_NUM);

    public String getRocksDBStoragePath() {
        return rocksDBStoragePath;
    }

    public String getRocksDBBackupPath() {
        return rocksDBBackupPath;
    }

    public int getRocksDBSyncerNum() {
        return rocksDBSyncerNum;
    }

    public static void main(String[] args) {
        System.out.println(Configs.getConfiguration().getConfig(RocksDBConfigs.ROCKSDB_SYNCER_NUM));
    }
}
