package com.bonree.brfs.duplication.rocksdb;

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

    public byte[] getKey() {
        return key;
    }

    public byte[] getValue() {
        return value;
    }

    public String getColumnFamily() {
        return columnFamily;
    }

    public void setColumnFamily(String columnFamily) {
        this.columnFamily = columnFamily;
    }

    public void setKey(byte[] key) {
        this.key = key;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }

}
