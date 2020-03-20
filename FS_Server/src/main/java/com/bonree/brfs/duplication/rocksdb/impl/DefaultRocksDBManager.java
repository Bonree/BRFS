package com.bonree.brfs.duplication.rocksdb.impl;

import com.alibaba.fastjson.JSONObject;
import com.bonree.brfs.common.supervisor.TimeWatcher;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.utils.Pair;
import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.RocksDBConfigs;
import com.bonree.brfs.duplication.rocksdb.RocksDBConfig;
import com.bonree.brfs.duplication.rocksdb.RocksDBManager;
import com.bonree.brfs.duplication.rocksdb.WriteStatus;
import org.apache.curator.framework.CuratorFramework;
import org.rocksdb.*;
import org.rocksdb.util.SizeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/*******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date 2020/3/19 16:39
 * @Author: <a href=mailto:zhangqi@bonree.com>张奇</a>
 * @Description: 默认RocksDB管理操作类
 ******************************************************************************/
public class DefaultRocksDBManager implements RocksDBManager {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultRocksDBManager.class);

    private DBOptions DB_OPTIONS;
    private ReadOptions READ_OPTIONS;
    private WriteOptions WRITE_OPTIONS_SYNC;
    private WriteOptions WRITE_OPTIONS_ASYNC;
    private BloomFilter BLOOM_FILTER;
    private BlockBasedTableConfig BLOCK_BASED_TABLE_CONFIG;
    private ColumnFamilyOptions COLUMN_FAMILY_OPTIONS;
    private List<CompressionType> COMPRESSION_TYPES;
    private Map<String, ColumnFamilyHandle> CF_HANDLES;

    private TtlDB DB;
    private String rocksDBPath;
    private RocksDBConfig config;
    private CuratorFramework client;

    public DefaultRocksDBManager(String rocksDBPath, CuratorFramework client) {
        this.rocksDBPath = rocksDBPath;
        this.client = client;
        DB_OPTIONS = new DBOptions();
        READ_OPTIONS = new ReadOptions();
        WRITE_OPTIONS_SYNC = new WriteOptions();
        WRITE_OPTIONS_ASYNC = new WriteOptions();
        BLOOM_FILTER = new BloomFilter(10);
        BLOCK_BASED_TABLE_CONFIG = new BlockBasedTableConfig();
        COLUMN_FAMILY_OPTIONS = new ColumnFamilyOptions();
        COMPRESSION_TYPES = new ArrayList<>();
        CF_HANDLES = new ConcurrentHashMap<>();
        config = RocksDBConfig.newBuilder()
                .setMaxBackgroundFlush(Configs.getConfiguration().GetConfig(RocksDBConfigs.ROCKSDB_MAX_BACKGROUND_FLUSH))
                .setMaxBackgroundCompaction(Configs.getConfiguration().GetConfig(RocksDBConfigs.ROCKSDB_MAX_BACKGROUND_COMPACTION))
                .setMaxOpenFiles(Configs.getConfiguration().GetConfig(RocksDBConfigs.ROCKSDB_MAX_OPEN_FILES))
                .setMaxSubCompaction(Configs.getConfiguration().GetConfig(RocksDBConfigs.ROCKSDB_MAX_SUBCOMPACTIONN))
                .setBlockCache(Configs.getConfiguration().GetConfig(RocksDBConfigs.ROCKSDB_BLOCK_CACHE))
                .setWriteBufferSize(Configs.getConfiguration().GetConfig(RocksDBConfigs.ROCKSDB_WRITE_BUFFER_SIZE))
                .setMaxWriteBufferNumber(Configs.getConfiguration().GetConfig(RocksDBConfigs.ROCKSDB_MAX_WRITE_BUFFER_NUMBER))
                .setMinWriteBufferNumToMerge(Configs.getConfiguration().GetConfig(RocksDBConfigs.ROCKSDB_MIN_WRITE_BUFFER_NUM_TO_MERGE))
                .setLevel0FileNumCompactionTrigger(Configs.getConfiguration().GetConfig(RocksDBConfigs.ROCKSDB_LEVEL0_FILE_NUM_COMPACTION_TRIGGER))
                .setTargetFileSizeBase(Configs.getConfiguration().GetConfig(RocksDBConfigs.ROCKSDB_TARGET_FILE_SIZE_BASE))
                .setMaxBytesLevelBase(Configs.getConfiguration().GetConfig(RocksDBConfigs.ROCKSDB_MAX_BYTES_LEVEL_BASE))
                .build();
    }

    @Override
    public void start() throws Exception {
        this.DB_OPTIONS.setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(true)
                .setMaxBackgroundFlushes(this.config.getMaxBackgroundFlush())
                .setMaxBackgroundCompactions(this.config.getMaxBackgroundCompaction())
                .setMaxOpenFiles(this.config.getMaxOpenFiles())
                .setRowCache(new LRUCache(this.config.getBlockCache() * SizeUnit.MB, 6, true, 5))
                .setMaxSubcompactions(4);
        READ_OPTIONS.setPrefixSameAsStart(true);
        WRITE_OPTIONS_SYNC.setSync(true);
        WRITE_OPTIONS_ASYNC.setSync(false);
        BLOCK_BASED_TABLE_CONFIG.setFilter(BLOOM_FILTER)
                .setCacheIndexAndFilterBlocks(true)
                .setPinL0FilterAndIndexBlocksInCache(true);

        COMPRESSION_TYPES.addAll(Arrays.asList(
                CompressionType.NO_COMPRESSION, CompressionType.NO_COMPRESSION,
                CompressionType.SNAPPY_COMPRESSION, CompressionType.SNAPPY_COMPRESSION,
                CompressionType.SNAPPY_COMPRESSION, CompressionType.SNAPPY_COMPRESSION,
                CompressionType.ZLIB_COMPRESSION
        ));

        COLUMN_FAMILY_OPTIONS.setTableFormatConfig(BLOCK_BASED_TABLE_CONFIG)
                .setCompactionStyle(CompactionStyle.LEVEL)
                .setWriteBufferSize(this.config.getWriteBufferSize() * SizeUnit.MB)
                .setMaxWriteBufferNumber(this.config.getMaxWriteBufferNumber())
                .setMinWriteBufferNumberToMerge(this.config.getMinWriteBufferNumToMerge())
                .setLevel0FileNumCompactionTrigger(this.config.getLevel0FileNumCompactionTrigger())
                .setCompressionPerLevel(COMPRESSION_TYPES)
                .setTargetFileSizeBase(this.config.getTargetFileSizeBase() * SizeUnit.MB)
                .setMaxBytesForLevelBase(this.config.getMaxBytesLevelBase() * SizeUnit.MB)
                .setOptimizeFiltersForHits(true);

        try {
            Pair<List<ColumnFamilyDescriptor>, List<Integer>> columnFamilyInfo = loadColumnFamilyInfo();
            List<ColumnFamilyDescriptor> cfDescriptors = columnFamilyInfo.getFirst();
            List<Integer> cfTtlList = columnFamilyInfo.getSecond();
            List<ColumnFamilyHandle> cfHandles = new ArrayList<>();

            TimeWatcher watcher = new TimeWatcher();
            DB = TtlDB.open(DB_OPTIONS, rocksDBPath, cfDescriptors, cfHandles, cfTtlList, false);
            int openTime = watcher.getElapsedTime();
            LOG.info("RocksDB init complete, open RocksDB cost time:{}", openTime);
            cacheCFHandles(cfHandles);
            LOG.info("load column family info:{}", this.CF_HANDLES.keySet());
        } catch (RocksDBException e) {
            LOG.error("RocksDB start error", e);
            throw e;
        }
    }

    @Override
    public byte[] read(String columnFamily, byte[] key) throws Exception {
        try {
            return DB.get(this.CF_HANDLES.get(columnFamily), READ_OPTIONS, key);
        } catch (RocksDBException e) {
            LOG.error("read occur error, cf:{}, key:{}", columnFamily, new String(key), e);
            return null;
        }
    }

    @Override
    public WriteStatus write(String columnFamily, byte[] key, byte[] value) throws Exception {
        return this.write(this.CF_HANDLES.get(columnFamily), WRITE_OPTIONS_ASYNC, key, value);
    }

    @Override
    public WriteStatus write(String columnFamily, byte[] key, byte[] value, boolean force) throws Exception {
        if (!force) {
            byte[] res = read(columnFamily, key);
            if (res != null) {
                return WriteStatus.KEY_EXISTS;
            }
        }
        return write(columnFamily, key, value);
    }

    @Override
    public void createColumnFamilyWithTtl(String columnFamily, int ttl) throws Exception {
        try {
            ColumnFamilyHandle handle = this.DB.createColumnFamilyWithTtl(new ColumnFamilyDescriptor(columnFamily.getBytes(), COLUMN_FAMILY_OPTIONS), ttl);
            this.CF_HANDLES.put(columnFamily, handle);
            LOG.info("create column family complete, name:{}, ttl:{}", columnFamily, ttl);
            // 更新ZK信息
        } catch (Exception e) {
            LOG.error("create column family error, cf:{}, ttl:{}", columnFamily, ttl, e);
            throw e;
        }
    }

    @Override
    public void deleteColumnFamily(String columnFamily) throws Exception {
        try {
            this.DB.dropColumnFamily(this.CF_HANDLES.get(columnFamily));
            this.CF_HANDLES.remove(columnFamily);
            LOG.info("remove column family complete, cf:{}", columnFamily);
            // 更新ZK信息
        } catch (Exception e) {
            LOG.error("create column family error", e);
            throw e;
        }
    }

    private WriteStatus write(final ColumnFamilyHandle cfh, final WriteOptions writeOptions, final byte[] key, final byte[] value) throws RocksDBException {
        try {
            this.DB.put(cfh, writeOptions, key, value);
            return WriteStatus.SUCCESS;
        } catch (RocksDBException e) {
            LOG.error("write occur error, cf:{}, key:{}, value:{}", new String(cfh.getName()), new String(key), new String(value), e);
            return WriteStatus.FAILED;
        }
    }

    /**
     * @description: 从ZK中加载列族信息
     */
    private Pair<List<ColumnFamilyDescriptor>, List<Integer>> loadColumnFamilyInfo() throws Exception {
        Pair<List<ColumnFamilyDescriptor>, List<Integer>> columnFamilyInfo = new Pair<>();
        try {
            byte[] bytes = this.client.getData().forPath(RocksDBZkPaths.DEFAULT_PATH_ROCKSDB_COLUMN_FAMILY_INFO);
            JSONObject jsonObject = JSONObject.parseObject(BrStringUtils.fromUtf8Bytes(bytes));

            List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();
            List<Integer> cfTtlList = new ArrayList<>();
            for (Map.Entry<String, Object> entry : jsonObject.entrySet()) {
                cfDescriptors.add(new ColumnFamilyDescriptor(entry.getKey().getBytes(), COLUMN_FAMILY_OPTIONS));
                cfTtlList.add(Integer.parseInt(entry.getValue().toString()));
            }

            columnFamilyInfo.setFirst(cfDescriptors);
            columnFamilyInfo.setSecond(cfTtlList);
        } catch (Exception e) {
            LOG.error("load column family error", e);
            throw e;
        }
        return columnFamilyInfo;
    }

    private void cacheCFHandles(List<ColumnFamilyHandle> cfHandles) throws RocksDBException {
        if (cfHandles == null || cfHandles.size() == 0) {
            throw new RocksDBException("init RocksDB columnFamilyHandle failure");
        }
        for (ColumnFamilyHandle cfHandle : cfHandles) {
            this.CF_HANDLES.put(new String(cfHandle.getName()), cfHandle);
        }
    }

    @Override
    public void stop() throws Exception {
        this.DB_OPTIONS.close();
        this.WRITE_OPTIONS_SYNC.close();
        this.WRITE_OPTIONS_ASYNC.close();
        this.READ_OPTIONS.close();
        this.COLUMN_FAMILY_OPTIONS.close();
        this.CF_HANDLES.forEach((x, y) -> {
            y.close();
        });
        if (DB != null) {
            DB.close();
        }
    }
}
