package com.bonree.brfs.rocksdb.impl;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/*******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date 2020/6/18 14:10
 * @Author: <a href=mailto:zhangqi@bonree.com>张奇</a>
 * @Description:
 ******************************************************************************/
public class RocksDBDataUnit {
    @JsonProperty("columnFamily")
    private String columnFamily;
    @JsonProperty("key")
    private byte[] key;
    @JsonProperty("value")
    private byte[] value;

    @JsonCreator
    public RocksDBDataUnit(@JsonProperty("columnFamily") String columnFamily, @JsonProperty("key") byte[] key,
                           @JsonProperty("value") byte[] value) {
        this.columnFamily = columnFamily;
        this.key = key;
        this.value = value;
    }

    public String getColumnFamily() {
        return columnFamily;
    }

    public byte[] getKey() {
        return key;
    }

    public byte[] getValue() {
        return value;
    }
}
