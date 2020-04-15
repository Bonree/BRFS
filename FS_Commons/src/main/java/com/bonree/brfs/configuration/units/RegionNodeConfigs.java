package com.bonree.brfs.configuration.units;

import com.bonree.brfs.configuration.ConfigUnit;

public final class RegionNodeConfigs {
	
	public static final ConfigUnit<String> CONFIG_HOST =
			ConfigUnit.ofString("regionnode.service.host", "127.0.0.1");
	
	public static final ConfigUnit<Integer> CONFIG_PORT =
			ConfigUnit.ofInt("regionnode.service.port", 8880);
	
	public static final ConfigUnit<Integer> CONFIG_SERVER_IO_THREAD_NUM =
			ConfigUnit.ofInt("regionnode.server.io.num", Runtime.getRuntime().availableProcessors());
	
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
	
	public static final ConfigUnit<Integer> CONFIG_CLOSER_THREAD_NUM =
			ConfigUnit.ofInt("regionnode.file.closer.thead_num", 1);

	public static final ConfigUnit<Long> CONFIG_BLOCK_SIZE =
			ConfigUnit.ofLong("regionnode.block.size", 64 * 1024 * 1024);

	public static final ConfigUnit<Integer> CONFIG_BLOCK_POOL_CAPACITY =
			ConfigUnit.ofInt("regionnode.block.pool.capacity", 20);

	public static final ConfigUnit<Integer> CONFIG_BLOCK_POOL_INIT_COUNT =
			ConfigUnit.ofInt("regionnode.block.pool.init.count", 1);

	public static final ConfigUnit<Integer> CLEAR_TIME_THRESHOLD =
			ConfigUnit.ofInt("regionnode.file.clear.time.threshold", 300000);

	public static final ConfigUnit<Integer> WAIT_FOR_BLOCK_TIME =
            ConfigUnit.ofInt("regionnode.waitfor.block.time", 10000);

	public static final ConfigUnit<Integer> FILE_WAIT_FOR_WRITE_TIME =
			ConfigUnit.ofInt("regionnode.waitfor.fileWrite.time", 300000);

	public static final ConfigUnit<Integer> CONFIG_DUPLICATION_SELECT_TYPE = ConfigUnit.ofInt("regionnode.duplication.select.type",1);

	private RegionNodeConfigs() {}
}
