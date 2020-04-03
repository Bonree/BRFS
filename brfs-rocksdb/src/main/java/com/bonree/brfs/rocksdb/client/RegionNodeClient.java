package com.bonree.brfs.rocksdb.client;

import com.bonree.brfs.rocksdb.RocksDBDataUnit;

import java.io.Closeable;
import java.util.List;

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

    byte[] readData(String columnFamily, String key);

    void writeData(RocksDBDataUnit unit) throws Exception;

    /**
     * @param fileName    一次传输使用的临时文件名称
     * @param restorePath 用于接收socket client端备份文件的本地数据恢复路径
     * @return 一次备份中的所有backupId
     * @description: 用于向合适的RegionNode请求建立临时Socket连接，进行RocksDB备份文件的传输，从而进行数据恢复
     */
    List<Integer> restoreData(String fileName, String restorePath, String host, int port);

}
