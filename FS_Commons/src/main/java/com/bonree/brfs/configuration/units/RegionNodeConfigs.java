package com.bonree.brfs.configuration.units;

import com.bonree.brfs.configuration.ConfigUnit;

public final class RegionNodeConfigs {

    public static final ConfigUnit<Integer> CONFIG_FILE_CLEAN_COUNT =
            ConfigUnit.ofInt("regionnode.file.clean.count", 2);

    public static final ConfigUnit<Integer> CONFIG_MAX_FILE_COUNT =
            ConfigUnit.ofInt("regionnode.file.max.count", 3);

    public static final ConfigUnit<Double> CONFIG_FILE_CLEAN_USAGE_RATE =
        ConfigUnit.ofDouble("regionnode.file.clean.usage.rate", 0.95);

    public static final ConfigUnit<Integer> CONFIG_WRITER_WORKER_NUM =
        ConfigUnit.ofInt("regionnode.writer.worker.num", Runtime.getRuntime().availableProcessors());

    public static final ConfigUnit<Integer> CONFIG_DATA_POOL_CAPACITY =
        ConfigUnit.ofInt("regionnode.data.pool.capacity", 60);

    public static final ConfigUnit<String> CONFIG_DATA_ENGINE_IDLE_TIME =
        ConfigUnit.ofString("regionnode.dataengine.idle.time", "PT1H");

    public static final ConfigUnit<Integer> CONFIG_CLOSER_THREAD_NUM =
            ConfigUnit.ofInt("regionnode.file.closer.thead_num", 8);

    public static final ConfigUnit<Long> CONFIG_BLOCK_SIZE =
            ConfigUnit.ofLong("regionnode.block.size", 16 * 1024 * 1024);

    public static final ConfigUnit<Integer> CONFIG_BLOCK_POOL_CAPACITY =
            ConfigUnit.ofInt("regionnode.block.pool.capacity", 3);

    public static final ConfigUnit<Integer> CONFIG_BLOCK_POOL_INIT_COUNT =
        ConfigUnit.ofInt("regionnode.block.pool.init.count", 1);

    public static final ConfigUnit<Integer> CLEAR_TIME_THRESHOLD =
        ConfigUnit.ofInt("regionnode.file.clear.time.threshold", 180000);

    public static final ConfigUnit<Integer> WAIT_FOR_BLOCK_TIME =
        ConfigUnit.ofInt("regionnode.waitfor.block.time", 10000);

    public static final ConfigUnit<Integer> FILE_WAIT_FOR_WRITE_TIME =
        ConfigUnit.ofInt("regionnode.waitfor.fileWrite.time", 300000);

    public static final ConfigUnit<Integer> CONFIG_DUPLICATION_SELECT_TYPE =
        ConfigUnit.ofInt("regionnode.duplication.select.type", 2);

    private RegionNodeConfigs() {
    }
}
