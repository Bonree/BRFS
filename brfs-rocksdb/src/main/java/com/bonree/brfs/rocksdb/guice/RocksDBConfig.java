package com.bonree.brfs.rocksdb.guice;

import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.rocksdb.configuration.RocksDBConfigs;
import com.fasterxml.jackson.annotation.JsonProperty;

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
    public String rocksDBStoragePath = Configs.getConfiguration().GetConfig(RocksDBConfigs.ROCKSDB_STORAGE_PATH);

    public String getRocksDBStoragePath(){
        return rocksDBStoragePath;
    }
}
