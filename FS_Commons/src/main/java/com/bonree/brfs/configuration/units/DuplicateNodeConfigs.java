package com.bonree.brfs.configuration.units;

import java.io.File;

import com.bonree.brfs.configuration.ConfigUnit;
import com.bonree.brfs.configuration.SystemProperties;

public final class DuplicateNodeConfigs {
	
	public static final ConfigUnit<String> CONFIG_HOST =
			ConfigUnit.ofString("duplicatenode.service.host", "127.0.0.1");
	
	public static final ConfigUnit<Integer> CONFIG_PORT =
			ConfigUnit.ofInt("duplicatenode.service.port", 8880);
	
	public static final ConfigUnit<String> CONFIG_LOG_DIR_PATH =
			ConfigUnit.ofString("duplicatenode.log.dir",
					new File(System.getProperty(SystemProperties.PROP_BRFS_HOME, "."), "logs").getAbsolutePath());
	
	public static final ConfigUnit<Integer> CONFIG_FILE_CLEAN_COUNT =
			ConfigUnit.ofInt("duplicatenode.file.clean.count", 10);
	
	public static final ConfigUnit<Integer> CONFIG_MAX_FILE_COUNT =
			ConfigUnit.ofInt("duplicatenode.file.max.count", 15);
	
	public static final ConfigUnit<Double> CONFIG_FILE_CLEAN_USAGE_RATE =
			ConfigUnit.ofDouble("duplicatenode.file.clean.usage.rate", 0.95);
	
	public static final ConfigUnit<Integer> CONFIG_WRITER_WORKER_NUM =
			ConfigUnit.ofInt("duplicatenode.writer.worker.num", Runtime.getRuntime().availableProcessors());
	
	public static final ConfigUnit<Integer> CONFIG_DATA_POOL_CAPACITY =
			ConfigUnit.ofInt("duplicatenode.data.pool.capacity", 512);
	
	public static final ConfigUnit<String> CONFIG_DATA_ENGINE_IDLE_TIME =
			ConfigUnit.ofString("duplicatenode.dataengine.idle.time", "PT1H");
	
	private DuplicateNodeConfigs() {}
}
