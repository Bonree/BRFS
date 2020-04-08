package com.bonree.brfs.disknode;

import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.DataNodeConfigs;
import com.fasterxml.jackson.annotation.JsonProperty;

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
    @JsonProperty("server.dir")
    private String serverIds = Configs.getConfiguration().GetConfig(DataNodeConfigs.CONFIG_SERVER_IDS_DIR);
    /**
     * 磁盘id的id文件目录
     */
    @JsonProperty("partition.dir")
    private String partitionIds = Configs.getConfiguration().GetConfig(DataNodeConfigs.CONFIG_PARTITION_IDS_DIR);


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
