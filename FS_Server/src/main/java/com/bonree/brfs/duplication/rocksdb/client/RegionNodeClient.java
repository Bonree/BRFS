package com.bonree.brfs.duplication.rocksdb.client;

import com.bonree.brfs.duplication.rocksdb.RocksDBDataUnit;

import java.io.Closeable;

/*******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date 2020/3/19 10:51
 * @Author: <a href=mailto:zhangqi@bonree.com>张奇</a>
 * @Description:
 ******************************************************************************/
public interface RegionNodeClient extends Closeable {
    boolean ping();

    byte[] readData(RocksDBDataUnit unit);

    void writeData(RocksDBDataUnit unit) throws Exception;

}
