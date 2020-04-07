package com.bonree.brfs.configuration.units;

import java.io.File;

import com.bonree.brfs.configuration.ConfigUnit;
import com.bonree.brfs.configuration.SystemProperties;

public final class DataNodeConfigs {
	public static final ConfigUnit<String> CONFIG_HOST =
			ConfigUnit.ofString("datanode.service.host", "127.0.0.1");
	
	public static final ConfigUnit<Integer> CONFIG_PORT =
			ConfigUnit.ofInt("datanode.service.port", 8881);
	
	public static final ConfigUnit<Integer> CONFIG_FILE_PORT =
			ConfigUnit.ofInt("datanode.file.server.port", 8900);
	
	public static final ConfigUnit<Integer> CONFIG_SERVER_IO_NUM =
			ConfigUnit.ofInt("datanode.server.io.num", Runtime.getRuntime().availableProcessors());
	
	public static final ConfigUnit<String> CONFIG_DATA_ROOT =
			ConfigUnit.ofString("datanode.data.root",
					new File(System.getProperty(SystemProperties.PROP_BRFS_HOME, "."), "datas").getAbsolutePath());
	
	public static final ConfigUnit<Long> CONFIG_FILE_MAX_CAPACITY =
			ConfigUnit.ofLong("datanode.file.max.capacity", 64 * 1024 * 1024);
	
	public static final ConfigUnit<Integer> CONFIG_WRITER_WORKER_NUM =
			ConfigUnit.ofInt("datanode.writer.worker.num", Runtime.getRuntime().availableProcessors());
	
	public static final ConfigUnit<Integer> CONFIG_WRITER_DATA_CACHE_SIZE =
			ConfigUnit.ofInt("datanode.writer.data.cache", 512 * 1024);
	
	public static final ConfigUnit<Integer> CONFIG_WRITER_RECORD_CACHE_SIZE =
			ConfigUnit.ofInt("datanode.writer.record.cache", 64 * 1024);
	
	public static final ConfigUnit<String> CONFIG_FILE_IDLE_TIME =
			ConfigUnit.ofString("datanode.file.idle.time", "PT3S");
	
	public static final ConfigUnit<Integer> CONFIG_REQUEST_HANDLER_NUM =
			ConfigUnit.ofInt("datanode.request.handler.num", Runtime.getRuntime().availableProcessors());
	
	public static final ConfigUnit<Integer> CONFIG_FILE_READER_NUM =
			ConfigUnit.ofInt("datanode.file.reader.num", Runtime.getRuntime().availableProcessors());

	public static final ConfigUnit<Boolean> CONFIG_DATA_COMPRESS =
			ConfigUnit.ofBoolean("datanode.data.compress", true);

	public static final ConfigUnit<Boolean> CONFIG_READ_BY_ZEROCOPY =
			ConfigUnit.ofBoolean("datanode.read.by.zerocopy", true);
	private DataNodeConfigs() {}
}
