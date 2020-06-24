package com.bonree.brfs.configuration.units;

import com.bonree.brfs.configuration.ConfigUnit;

public final class ResourceConfigs {

    public static final ConfigUnit<Long> CONFIG_LIMIT_DISK_REMAIN_SIZE =
        ConfigUnit.ofLong("limit.resource.value.disk.remain.size", 20 * 1024 * 1024);

    public static final ConfigUnit<Long> CONFIG_LIMIT_FORCE_DISK_REMAIN_SIZE =
        ConfigUnit.ofLong("limit.resource.value.force.disk.remain.size", 10 * 1024 * 1024);

    public static final ConfigUnit<Integer> CONFIG_RESOURCE_CENT_SIZE =
        ConfigUnit.ofInt("resource.cent.size", 1000);

    private ResourceConfigs() {
    }
}
