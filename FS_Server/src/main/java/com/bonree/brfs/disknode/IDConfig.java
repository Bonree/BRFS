package com.bonree.brfs.disknode;

import com.bonree.brfs.configuration.SystemProperties;

import java.io.File;

/**
 * 版权信息: 北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007-2020 北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 *
 * @date: 2020年04月06日 12:03
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description:
 **/
public class IDConfig {
    /**
     * 一级serverid的id文件目录
     */
    private String serverIds = System.getProperty(SystemProperties.PROP_SERVER_ID_DIR);
    /**
     * 磁盘id的id文件目录
     */
    private String partitionIds = System.getProperty(SystemProperties.PROP_PARTITION_ID_IDR,new File(serverIds).getParent()+"/partitionIds");

    public String getServerIds() {
        return serverIds;
    }

    public void setServerIds(String serverIds) {
        this.serverIds = serverIds;
    }

    public String getPartitionIds() {
        return partitionIds;
    }

    public void setPartitionIds(String partitionIds) {
        this.partitionIds = partitionIds;
    }
}
