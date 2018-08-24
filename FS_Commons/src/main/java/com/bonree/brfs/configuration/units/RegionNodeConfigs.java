package com.bonree.brfs.configuration.units;

import java.io.File;

import com.bonree.brfs.configuration.ConfigUnit;
import com.bonree.brfs.configuration.SystemProperties;

public final class RegionNodeConfigs {
	
	public static final ConfigUnit<String> CONFIG_HOST =
			ConfigUnit.ofString("regionnode.service.host", "127.0.0.1");
	
	public static final ConfigUnit<Integer> CONFIG_PORT =
			ConfigUnit.ofInt("regionnode.service.port", 8880);
	
	public static final ConfigUnit<Integer> CONFIG_SERVER_IO_THREAD_NUM =
			ConfigUnit.ofInt("regionnode.server.io.num", Runtime.getRuntime().availableProcessors());
	
	public static final ConfigUnit<String> CONFIG_LOG_DIR_PATH =
			ConfigUnit.ofString("regionnode.log.dir",
					new File(System.getProperty(SystemProperties.PROP_BRFS_HOME, "."), "logs").getAbsolutePath());
	
	public static final ConfigUnit<Integer> CONFIG_FILE_CLEAN_COUNT =
			ConfigUnit.ofInt("regionnode.file.clean.count", 10);
	
	public static final ConfigUnit<Integer> CONFIG_MAX_FILE_COUNT =
			ConfigUnit.ofInt("regionnode.file.max.count", 15);
	
	public static final ConfigUnit<Double> CONFIG_FILE_CLEAN_USAGE_RATE =
			ConfigUnit.ofDouble("regionnode.file.clean.usage.rate", 0.95);
	
	public static final ConfigUnit<Integer> CONFIG_WRITER_WORKER_NUM =
			ConfigUnit.ofInt("regionnode.writer.worker.num", Runtime.getRuntime().availableProcessors());
	
	public static final ConfigUnit<Integer> CONFIG_DATA_POOL_CAPACITY =
			ConfigUnit.ofInt("regionnode.data.pool.capacity", 512);
	
	public static final ConfigUnit<String> CONFIG_DATA_ENGINE_IDLE_TIME =
			ConfigUnit.ofString("regionnode.dataengine.idle.time", "PT1H");
	
	private RegionNodeConfigs() {}
}
