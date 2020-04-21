package com.bonree.brfs.configuration.units;

import com.bonree.brfs.configuration.ConfigUnit;

public final class CommonConfigs {
    /**
     * 服务所在的集群名
     */
    public static final ConfigUnit<String> CONFIG_CLUSTER_NAME =
            ConfigUnit.ofString("cluster.name", "brfs");
    /**
     * Zookeeper集群的地址信息
     */
    public static final ConfigUnit<String> CONFIG_ZOOKEEPER_ADDRESSES =
            ConfigUnit.ofString("zookeeper.addresses", "localhost:2181");

    public static final ConfigUnit<String> CONFIG_DATA_SERVICE_GROUP_NAME =
            ConfigUnit.ofString("datanode.service.group", "data_group");

    public static final ConfigUnit<String> CONFIG_REGION_SERVICE_GROUP_NAME =
            ConfigUnit.ofString("regionnode.service.group", "region_group");

    public static final ConfigUnit<String> CONFIG_PARTITION_GROUP_NAME =
            ConfigUnit.ofString("partition.group", "disk_group");

    /**
     * BRFS集群ZK元数据信息备份数据存储路径
     */
    public static final ConfigUnit<String> CONFIG_METADATA_BACKUP_PATH =
            ConfigUnit.ofString("metadata.backup.path", "/tmp/metadata");

    /**
     * BRFS集群ZK元数据信息备份周期，单位分钟
     */
    public static final ConfigUnit<Integer> CONFIG_METADATA_BACKUP_CYCLE =
            ConfigUnit.ofInt("metadata.backup.cycle", 10);

    private CommonConfigs() {
    }
}
