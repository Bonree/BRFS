package com.bonree.brfs.duplication.rocksdb.client;

import com.bonree.brfs.duplication.rocksdb.RocksDBDataUnit;

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

    byte[] readData(RocksDBDataUnit unit);

    void writeData(RocksDBDataUnit unit) throws Exception;

    /**
     * @param tmpFileName 一次传输使用的临时文件名称
     * @param backupPath  用于接收socket client端备份文件的本地备份路径
     * @param files       备份文件列表
     * @description: 用于向合适的RegionNode请求建立临时Socket连接，进行RocksDB备份文件的传输
     */
    boolean establishSocket(String tmpFileName, String backupPath, String socketHost, int socketPort, List<String> files);

}
