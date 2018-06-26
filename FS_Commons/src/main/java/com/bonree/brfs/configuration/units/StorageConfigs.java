package com.bonree.brfs.configuration.units;

import com.bonree.brfs.configuration.ConfigUnit;

public final class StorageConfigs {
	/**
	 * Storage空间中数据的过期时间
	 */
	public static final ConfigUnit<Integer> CONFIG_STORAGE_DATA_TTL =
			ConfigUnit.ofInt("storage.data.ttl", 30);
	/**
	 * Storage空间中数据的默认副本数
	 */
	public static final ConfigUnit<Integer> CONFIG_STORAGE_REPLICATE_COUNT =
			ConfigUnit.ofInt("storage.replicate.count", 2);

	private StorageConfigs() {}
}
