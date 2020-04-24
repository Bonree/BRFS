package com.bonree.brfs.configuration.units;

import com.bonree.brfs.configuration.ConfigUnit;

public final class RebalanceConfigs {

    public static final ConfigUnit<Integer> CONFIG_VIRTUAL_DELAY =
        ConfigUnit.ofInt("rebalance.virtual.recover.time", 3600);

    public static final ConfigUnit<Integer> CONFIG_NORMAL_DELAY =
        ConfigUnit.ofInt("rebalance.serverdown.recover.time", 3600);

    private RebalanceConfigs() {
    }
}
