package com.bonree.brfs.disknode;

import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.PartitionIdsConfigs;

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
    private String partitionGroupName = Configs.getConfiguration().GetConfig(PartitionIdsConfigs.CONFIG_PARTITION_GROUP_NAME);
    /**
     * 磁盘检查的周期 单位 s
     */
    private int intervalTime = Configs.getConfiguration().GetConfig(PartitionIdsConfigs.CONFIG_CHECK_INTERVAL_SECOND_TIME);

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
