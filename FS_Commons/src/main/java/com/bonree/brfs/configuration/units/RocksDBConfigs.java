package com.bonree.brfs.configuration.units;

import com.bonree.brfs.configuration.ConfigUnit;
import com.bonree.brfs.configuration.SystemProperties;

/*******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date 2020/3/17 15:32
 * @Author: <a href=mailto:zhangqi@bonree.com>张奇</a>
 * @Description:
 ******************************************************************************/
public class RocksDBConfigs {

    private RocksDBConfigs() {
    }

    /**
     * @description: RocksDB数据存储目录
     */
    public static final ConfigUnit<String> ROCKSDB_STORAGE_PATH =
        ConfigUnit.ofString("rocksdb.storage.path", System.getProperty(SystemProperties.PROP_BRFS_HOME) + "/rocksdb");

    /**
     * @description: RocksDB数据备份目录
     */
    public static final ConfigUnit<String> ROCKSDB_BACKUP_PATH =
        ConfigUnit.ofString("rocksdb.backup.path", System.getProperty(SystemProperties.PROP_BRFS_HOME) + "/rocksdb_backup");

    /**
     * @description: RocksDB用于接收其他节点的备份文件的恢复目录
     */
    public static final ConfigUnit<String> ROCKSDB_RESTORE_PATH =
        ConfigUnit.ofString("rocksdb.restore.path", System.getProperty(SystemProperties.PROP_BRFS_HOME) + "/rocksdb_restore");

    /**
     * @description: RocksDB用于临时数据恢复的目录，恢复完成后，将数据迁移到原RocksDB数据存储目录
     */
    public static final ConfigUnit<String> ROCKSDB_RESTORE_TEMPORARY_PATH =
        ConfigUnit.ofString("rocksdb.restore.temporary.path",
                            System.getProperty(SystemProperties.PROP_BRFS_HOME) + "/rocksdb_temporary");

    /**
     * @description: RocksDB数据过期时间配置，默认100天
     */
    public static final ConfigUnit<Integer> ROCKSDB_DATA_TTL =
        ConfigUnit.ofInt("rocksdb.data.ttl", 60 * 24 * 60 * 100);

    public static final ConfigUnit<Integer> ROCKSDB_BACKUP_FILE_TRANSFER_PORT =
        ConfigUnit.ofInt("rocksdb.backup.file.transfer.port", 9696);

    public static final ConfigUnit<Long> ROCKSDB_BACKUP_CYCLE =
        ConfigUnit.ofLong("rocksdb.backup.cycle", 10 * 60 * 1000);

    public static final ConfigUnit<Integer> ROCKSDB_MAX_BACKGROUND_FLUSH =
        ConfigUnit.ofInt("rocksdb.max.background.flush", 2);

    public static final ConfigUnit<Integer> ROCKSDB_MAX_BACKGROUND_COMPACTION =
        ConfigUnit.ofInt("rocksdb.max.background.compaction", 2);

    public static final ConfigUnit<Integer> ROCKSDB_MAX_OPEN_FILES =
        ConfigUnit.ofInt("rocksdb.max.open.files", -1);

    public static final ConfigUnit<Integer> ROCKSDB_MAX_SUBCOMPACTIONN =
        ConfigUnit.ofInt("rocksdb.max.subcompaction", 2);

    public static final ConfigUnit<Integer> ROCKSDB_BLOCK_CACHE =
        ConfigUnit.ofInt("rocksdb.block.cache", 512);

    public static final ConfigUnit<Integer> ROCKSDB_WRITE_BUFFER_SIZE =
        ConfigUnit.ofInt("rocksdb.write.buffer.size", 64);

    public static final ConfigUnit<Integer> ROCKSDB_MAX_WRITE_BUFFER_NUMBER =
        ConfigUnit.ofInt("rocksdb.max.write.buffer.number", 2);

    public static final ConfigUnit<Integer> ROCKSDB_MIN_WRITE_BUFFER_NUM_TO_MERGE =
        ConfigUnit.ofInt("rocksdb.min.write.buffer.num.to.merge", 1);

    public static final ConfigUnit<Integer> ROCKSDB_LEVEL0_FILE_NUM_COMPACTION_TRIGGER =
        ConfigUnit.ofInt("rocksdb.level0.file.num.compaction.trigger", 4);

    public static final ConfigUnit<Integer> ROCKSDB_TARGET_FILE_SIZE_BASE =
        ConfigUnit.ofInt("rocksdb.target.file.size.base", 64);

    public static final ConfigUnit<Integer> ROCKSDB_MAX_BYTES_LEVEL_BASE =
        ConfigUnit.ofInt("rocksdb.max.bytes.level.base", 256);

    public static final ConfigUnit<Integer> ROCKSDB_DATA_SYNCHRONIZE_COUNT_ONCE =
        ConfigUnit.ofInt("rocksdb.data.synchronize.count.once", 50);

    public static final ConfigUnit<Long> ROCKSDB_RATE_BYTES_PER_SECOND =
        ConfigUnit.ofLong("rocksdb.rate.bytes.per.second", 10240L);

    public static final ConfigUnit<Long> ROCKSDB_COMPACTION_READHEAD_SIZE =
        ConfigUnit.ofLong("rocksdb.compaction.readhead.size", 2048L);

}
