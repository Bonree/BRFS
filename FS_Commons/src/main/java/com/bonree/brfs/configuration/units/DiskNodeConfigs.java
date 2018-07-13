package com.bonree.brfs.configuration.units;

import java.io.File;

import com.bonree.brfs.configuration.ConfigUnit;
import com.bonree.brfs.configuration.SystemProperties;

public final class DiskNodeConfigs {
	public static final ConfigUnit<String> CONFIG_HOST =
			ConfigUnit.ofString("disknode.service.host", "127.0.0.1");
	
	public static final ConfigUnit<Integer> CONFIG_PORT =
			ConfigUnit.ofInt("disknode.service.port", 8881);
	
	public static final ConfigUnit<String> CONFIG_DATA_ROOT =
			ConfigUnit.ofString("disknode.data.root",
					new File(System.getProperty(SystemProperties.PROP_BRFS_HOME, "."), "datas").getAbsolutePath());
	
	public static final ConfigUnit<Long> CONFIG_FILE_MAX_CAPACITY =
			ConfigUnit.ofLong("disknode.file.max.capacity", 64 * 1024 * 1024);
	
	public static final ConfigUnit<Integer> CONFIG_WRITER_WORKER_NUM =
			ConfigUnit.ofInt("disknode.writer.worker.num", Runtime.getRuntime().availableProcessors());
	
	public static final ConfigUnit<Integer> CONFIG_WRITER_DATA_CACHE_SIZE =
			ConfigUnit.ofInt("disknode.writer.data.cache", 512 * 1024);
	
	public static final ConfigUnit<Integer> CONFIG_WRITER_RECORD_CACHE_SIZE =
			ConfigUnit.ofInt("disknode.writer.record.cache", 64 * 1024);
	
	public static final ConfigUnit<String> CONFIG_FILE_IDLE_TIME =
			ConfigUnit.ofString("disknode.file.idle.time", "PT3S");
	
	public static final ConfigUnit<Integer> CONFIG_REQUEST_HANDLER_NUM =
			ConfigUnit.ofInt("disknode.request.handler.num", Runtime.getRuntime().availableProcessors());

	private DiskNodeConfigs() {}
}
