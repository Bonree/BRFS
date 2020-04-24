package com.bonree.brfs.configuration.units;

import com.bonree.brfs.configuration.ConfigUnit;

/**
 * 版权信息: 北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007-2020 北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 *
 * @date: 2020年04月06日 11:50
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description: 磁盘分区配置
 **/
public class PartitionIdsConfigs {
    /**
     * 磁盘分区的组名称
     */
    public static final ConfigUnit<String> CONFIG_PARTITION_GROUP_NAME =
        ConfigUnit.ofString("partition.group", "partition_group");
    /**
     * 磁盘分区的检查周期
     */
    public static final ConfigUnit<Integer> CONFIG_CHECK_INTERVAL_SECOND_TIME =
        ConfigUnit.ofInt("partition.check.interval.time", 5);

}
