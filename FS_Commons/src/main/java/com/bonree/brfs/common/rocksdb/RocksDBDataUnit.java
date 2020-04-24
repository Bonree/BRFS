package com.bonree.brfs.common.rocksdb;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;

/*******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date 2020/3/19 18:11
 * @Author: <a href=mailto:zhangqi@bonree.com>张奇</a>
 * @Description:
 ******************************************************************************/
public class RocksDBDataUnit {
    String columnFamily;
    byte[] key;
    byte[] value;

    @JsonProperty("key")
    public byte[] getKey() {
        return key;
    }

    @JsonProperty("value")
    public byte[] getValue() {
        return value;
    }

    @JsonProperty("columnFamily")
    public String getColumnFamily() {
        return columnFamily;
    }

    @JsonCreator
    public RocksDBDataUnit(
        @JsonProperty("columnFamily") String columnFamily,
        @JsonProperty("key") byte[] key,
        @JsonProperty("value") byte[] value) {
        this.columnFamily = columnFamily;
        this.key = key;
        this.value = value;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this.getClass())
                          .add("columnFamily", columnFamily)
                          .add("key", new String(key))
                          .add("value", new String(value))
                          .toString();
    }
}
