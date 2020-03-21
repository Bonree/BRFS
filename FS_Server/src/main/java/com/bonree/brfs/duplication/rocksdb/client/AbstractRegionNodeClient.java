package com.bonree.brfs.duplication.rocksdb.client;

/*******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date 2020/3/19 11:16
 * @Author: <a href=mailto:zhangqi@bonree.com>张奇</a>
 * @Description: 用于向合适的RegionNode发送建立临时Socket连接的信号，进行RocksDB备份文件的传输
 ******************************************************************************/
public abstract class AbstractRegionNodeClient implements RegionNodeClient {
    public static final String SIGNAL = "SIGNAL";

    public abstract void sendEstablishSocketSign(String signal);
}
