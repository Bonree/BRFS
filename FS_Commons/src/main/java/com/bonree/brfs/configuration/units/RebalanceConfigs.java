package com.bonree.brfs.configuration.units;

import com.bonree.brfs.configuration.ConfigUnit;

public final class RebalanceConfigs {

	public static final ConfigUnit<Integer> CONFIG_VIRTUAL_DELAY =
			ConfigUnit.ofInt("global.replication.virtual.recover.after.time", 3600);
	
	public static final ConfigUnit<Integer> CONFIG_NORMAL_DELAY =
			ConfigUnit.ofInt("global.replication.recover.after.time", 3600);
	
	private RebalanceConfigs() {}
}
