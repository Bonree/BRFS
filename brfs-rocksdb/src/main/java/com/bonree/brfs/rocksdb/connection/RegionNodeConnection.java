package com.bonree.brfs.rocksdb.connection;

import com.bonree.brfs.rocksdb.client.RegionNodeClient;

import java.io.Closeable;

/*******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date 2020/3/19 14:40
 * @Author: <a href=mailto:zhangqi@bonree.com>张奇</a>
 * @Description:
 ******************************************************************************/
public interface RegionNodeConnection extends Closeable {
    String getRemoteAddress();

    int getRemotePort();

    boolean isValid();

    RegionNodeClient getClient();
}
