package com.bonree.brfs.configuration.units;

import com.bonree.brfs.configuration.ConfigUnit;

public final class StorageConfigs {
	/**
	 * Storage空间中数据的过期时间
	 */
	public static final ConfigUnit<String> CONFIG_STORAGE_REGION_DATA_TTL =
			ConfigUnit.ofString("storage.data.ttl", "P30D");
	/**
	 * Storage空间中数据的默认副本数
	 */
	public static final ConfigUnit<Integer> CONFIG_STORAGE_REGION_REPLICATE_COUNT =
			ConfigUnit.ofInt("storage.replicate.count", 2);
	
	public static final ConfigUnit<Long> CONFIG_STORAGE_REGION_FILE_CAPACITY =
			ConfigUnit.ofLong("storage.file.capacity", 64 * 1024 * 1024);
	
	public static final ConfigUnit<String> CONFIG_FILE_PATITION_DURATION =
			ConfigUnit.ofString("storage.file.patition.duration", "PT1H");

	private StorageConfigs() {}
}
