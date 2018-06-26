package com.bonree.brfs.configuration.units;

import java.io.File;

import com.bonree.brfs.configuration.ConfigUnit;
import com.bonree.brfs.configuration.SystemProperties;

public final class DuplicateNodeConfigs {
	
	public static final ConfigUnit<String> CONFIG_HOST =
			ConfigUnit.ofString("duplicatenode.service.host", "127.0.0.1");
	
	public static final ConfigUnit<Integer> CONFIG_PORT =
			ConfigUnit.ofInt("duplicatenode.service.port", 8880);
	
	public static final ConfigUnit<String> CONFIG_SERVICE_GROUP_NAME =
			ConfigUnit.ofString("duplicatenode.service.group", "duplicate_group");
	
	public static final ConfigUnit<String> CONFIG_VIRTUAL_SERVICE_GROUP_NAME = 
			ConfigUnit.ofString("duplicatenode.virtual.service.group", "virtual_group");
	
	public static final ConfigUnit<String> CONFIG_LOG_DIR_PATH =
			ConfigUnit.ofString("duplicatenode.log.dir",
					new File(System.getProperty(SystemProperties.PROP_BRFS_HOME, "."), "logs").getAbsolutePath());
	
	public static final ConfigUnit<Integer> CONFIG_FILE_CAPACITY =
			ConfigUnit.ofInt("duplicatenode.file.capacity", 64 * 1024 * 1024);
	
	public static final ConfigUnit<Long> CONFIG_FILE_CLEAN_INTERVAL_SECONDS =
			ConfigUnit.ofLong("duplicatenode.file.clean.interval.seconds", 30);
	
	public static final ConfigUnit<Integer> CONFIG_FILE_CLEAN_COUNT =
			ConfigUnit.ofInt("duplicatenode.file.clean.count", 10);
	
	public static final ConfigUnit<Double> CONFIG_FILE_CLEAN_USAGE_RATE =
			ConfigUnit.ofDouble("duplicatenode.file.clean.usage.rate", 0.95);
	
	public static final ConfigUnit<Long> CONFIG_FILE_PATITION_INTERVAL_MINUTES =
			ConfigUnit.ofLong("duplicatenode.file.patition.interval.minutes", 60);
	
	public static final ConfigUnit<Integer> CONFIG_WRITER_CONCURRENT_FILE_NUM =
			ConfigUnit.ofInt("duplicatenode.writer.concurrent.file.num", Runtime.getRuntime().availableProcessors() / 2);
	
	public static final ConfigUnit<Integer> CONFIG_WRITER_WORKER_NUM =
			ConfigUnit.ofInt("duplicatenode.writer.worker.num", Runtime.getRuntime().availableProcessors());
	
	public static final ConfigUnit<Integer> CONFIG_WRITER_RESULT_HANDLER_NUM =
			ConfigUnit.ofInt("duplicatenode.writer.result.handler.num", Runtime.getRuntime().availableProcessors() / 2);
	
	private DuplicateNodeConfigs() {}
}
