package com.bonree.brfs.disknode;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * 版权信息: 北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007-2020 北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 *
 * @date: 2020年04月06日 11:55
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description: 磁盘信息配置
 **/

public class PartitionConfig {
    /**
     * 磁盘分区组名称
     */
    @JsonProperty("group")
    private String partitionGroupName = "partition_group";
    /**
     * 磁盘检查的周期 单位 s
     */
    @JsonProperty("check.interval.time")
    private int intervalTime = 5;

    public PartitionConfig() throws Exception {
    }

    public String getPartitionGroupName() {
        return partitionGroupName;
    }

    public void setPartitionGroupName(String partitionGroupName) {
        this.partitionGroupName = partitionGroupName;
    }

    public int getIntervalTime() {
        return intervalTime;
    }

    public void setIntervalTime(int intervalTime) {
        this.intervalTime = intervalTime;
    }
}
