package com.bonree.brfs.rocksdb.impl;

import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.lifecycle.LifecycleStart;
import com.bonree.brfs.common.lifecycle.LifecycleStop;
import com.bonree.brfs.common.rocksdb.RocksDBDataUnit;
import com.bonree.brfs.common.rocksdb.RocksDBManager;
import com.bonree.brfs.common.rocksdb.WriteStatus;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.supervisor.TimeWatcher;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.common.utils.Pair;
import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.CommonConfigs;
import com.bonree.brfs.configuration.units.RocksDBConfigs;
import com.bonree.brfs.rocksdb.backup.BackupEngineFactory;
import com.bonree.brfs.rocksdb.connection.RegionNodeConnection;
import com.bonree.brfs.rocksdb.connection.RegionNodeConnectionPool;
import com.bonree.brfs.rocksdb.zk.ColumnFamilyInfoManager;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Sets;
import org.apache.curator.framework.CuratorFramework;
import org.rocksdb.*;
import org.rocksdb.util.SizeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.*;
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
    private Service service;
    private RocksDBConfig config;
    private CuratorFramework client;
    private ServiceManager serviceManager;
    private String regionGroupName;
    private RegionNodeConnectionPool regionNodeConnectionPool;
    private ColumnFamilyInfoManager columnFamilyInfoManager;
    private Pair<List<ColumnFamilyDescriptor>, List<Integer>> columnFamilyInfo;

    @Inject
    public DefaultRocksDBManager(CuratorFramework client, ZookeeperPaths zkPaths, Service service, ServiceManager serviceManager, RegionNodeConnectionPool regionNodeConnectionPool) {
        this.client = client.usingNamespace(zkPaths.getBaseRocksDBPath().substring(1));
        this.service = service;
        this.serviceManager = serviceManager;
        this.regionGroupName = Configs.getConfiguration().GetConfig(CommonConfigs.CONFIG_REGION_SERVICE_GROUP_NAME);
        this.regionNodeConnectionPool = regionNodeConnectionPool;
        this.columnFamilyInfoManager = new ColumnFamilyInfoManager(this.client);

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
        LOG.info("RocksDB configs:{}", this.config);
    }

    @LifecycleStart
    @Override
    public void start() throws Exception {
        this.DB_OPTIONS.setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(true)
                .setMaxBackgroundFlushes(this.config.getMaxBackgroundFlush())
                .setMaxBackgroundCompactions(this.config.getMaxBackgroundCompaction())
                .setMaxOpenFiles(this.config.getMaxOpenFiles())
                .setRowCache(new LRUCache(this.config.getBlockCache() * SizeUnit.MB, 6, true, 5))
                .setMaxSubcompactions(this.config.getMaxSubCompaction());
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
            columnFamilyInfo = loadColumnFamilyInfo();
            List<ColumnFamilyDescriptor> cfDescriptors = columnFamilyInfo.getFirst();
            List<Integer> cfTtlList = columnFamilyInfo.getSecond();
            List<ColumnFamilyHandle> cfHandles = new ArrayList<>();

            TimeWatcher watcher = new TimeWatcher();
            DB = TtlDB.open(DB_OPTIONS, Configs.getConfiguration().GetConfig(RocksDBConfigs.ROCKSDB_STORAGE_PATH), cfDescriptors, cfHandles, cfTtlList, false);
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
    public void createNewBackup(String backupPath) throws RocksDBException {
        BackupEngine backupEngine = BackupEngineFactory.getInstance().getBackupEngineByPath(backupPath);
        backupEngine.createNewBackup(this.DB);
    }

    @Override
    public byte[] read(String columnFamily, byte[] key) {
        if (null == columnFamily || columnFamily.isEmpty() || null == key) {
            LOG.warn("read column family is empty or key is null!");
            return null;
        }

        try {
            byte[] result = DB.get(this.CF_HANDLES.get(columnFamily), READ_OPTIONS, key);
            if (result == null) {
                List<Service> services = serviceManager.getServiceListByGroup(regionGroupName);
                RegionNodeConnection connection;
                String queryKey = new String(key);
                for (Service service : services) {
                    connection = this.regionNodeConnectionPool.getConnection(regionGroupName, service.getServiceId());
                    if (connection == null || connection.getClient() == null) {
                        LOG.warn("region node connection/client is null! serviceId:{}", service.getServiceId());
                        continue;
                    }

                    if (!this.service.getServiceId().equals(service.getServiceId())) {
                        result = connection.getClient().readData(columnFamily, queryKey);
                        if (result == null) {
                            continue;
                        }
                        LOG.info("read data from [{}] success, cf:{}, key:{}", service.getServiceId(), columnFamily, queryKey);
                        return result;
                    }
                }
            }
            return result;
        } catch (RocksDBException e) {
            LOG.error("read occur error, cf:{}, key:{}", columnFamily, new String(key), e);
            return null;
        }
    }

    @Override
    public byte[] read(String columnFamily, byte[] key, boolean readFormOther) {
        if (readFormOther) {
            return read(columnFamily, key);
        }

        if (null == columnFamily || columnFamily.isEmpty() || null == key) {
            LOG.warn("read column family is empty or key is null!");
            return null;
        }

        try {
            return DB.get(this.CF_HANDLES.get(columnFamily), READ_OPTIONS, key);
        } catch (RocksDBException e) {
            LOG.error("read occur error, cf:{}, key:{}", columnFamily, new String(key), e);
            return null;
        }
    }

    @Override
    public Map<byte[], byte[]> readByPrefix(String columnFamily, byte[] prefixKey) {
        if (null == columnFamily || columnFamily.isEmpty() || null == prefixKey) {
            LOG.warn("read by prefix column family is empty or prefixKey is null!");
            return null;
        }

        Map<byte[], byte[]> result = new HashMap<>();
        try {
            RocksIterator iterator = this.newIterator(this.CF_HANDLES.get(columnFamily));
            for (iterator.seek(prefixKey); iterator.isValid(); iterator.next()) {
                if (new String(iterator.key()).startsWith(new String(prefixKey))) {
                    result.put(iterator.key(), iterator.value());
                }
            }
        } catch (Exception e) {
            LOG.error("read by prefix occur error, cf:{}, prefix:{}", columnFamily, new String(prefixKey), e);
            return null;
        }

        return result;
    }

    @Override
    public WriteStatus write(RocksDBDataUnit unit) throws RocksDBException {
        return this.write(this.CF_HANDLES.get(unit.getColumnFamily()), WRITE_OPTIONS_ASYNC, unit.getKey(), unit.getValue());
    }

    @Override
    public WriteStatus write(String columnFamily, byte[] key, byte[] value) throws Exception {
        if (null == columnFamily || columnFamily.isEmpty() || null == key || null == value) {
            LOG.warn("write column family is empty or key/value is null!");
            return WriteStatus.FAILED;
        }

        WriteStatus writeStatus = this.write(this.CF_HANDLES.get(columnFamily), WRITE_OPTIONS_ASYNC, key, value);
        writeToAnotherService(columnFamily, key, value);
        return writeStatus;
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

    private WriteStatus write(final ColumnFamilyHandle cfh, final WriteOptions writeOptions, final byte[] key, final byte[] value) throws RocksDBException {
        try {
            this.DB.put(cfh, writeOptions, key, value);
        } catch (RocksDBException e) {
            LOG.error("write occur error, cf:{}, key:{}, value:{}", new String(cfh.getName()), new String(key), new String(value), e);
            return WriteStatus.FAILED;
        }
        return WriteStatus.SUCCESS;
    }

    private void writeToAnotherService(String columnFamily, byte[] key, byte[] value) throws Exception {
        List<Service> services = serviceManager.getServiceListByGroup(regionGroupName);
        RegionNodeConnection connection;

        RocksDBDataUnit dataUnit = new RocksDBDataUnit(columnFamily, key, value);

        for (Service service : services) {
            connection = this.regionNodeConnectionPool.getConnection(regionGroupName, service.getServiceId());
            if (connection == null || connection.getClient() == null) {
                LOG.warn("region node connection/client is null! serviceId:{}", service.getServiceId());
                continue;
            }

            if (!this.service.getServiceId().equals(service.getServiceId())) {
                connection.getClient().writeData(dataUnit);
            }
        }
    }

    private RocksIterator newIterator(ColumnFamilyHandle cfh) {
        return this.DB.newIterator(cfh, READ_OPTIONS);
    }

    @Override
    public void createColumnFamilyWithTtl(String columnFamily, int ttl) throws Exception {
        if (null == columnFamily || columnFamily.isEmpty()) {
            LOG.warn("column family is empty or null!");
            return;
        }

        try {
            if (this.CF_HANDLES.containsKey(columnFamily)) {
                LOG.warn("column family [{}] already exists!", columnFamily);
                return;
            }

            ColumnFamilyHandle handle = this.DB.createColumnFamilyWithTtl(new ColumnFamilyDescriptor(columnFamily.getBytes(), COLUMN_FAMILY_OPTIONS), ttl);
            this.CF_HANDLES.put(columnFamily, handle);
            LOG.info("create column family complete, name:{}, ttl:{}", columnFamily, ttl);
            // 更新ZK信息
            this.columnFamilyInfoManager.initOrAddColumnFamilyInfo(columnFamily, ttl);
        } catch (Exception e) {
            LOG.error("create column family error, cf:{}, ttl:{}", columnFamily, ttl, e);
            throw e;
        }
    }

    @Override
    public void deleteColumnFamily(String columnFamily) throws Exception {
        if (null == columnFamily || columnFamily.isEmpty()) {
            LOG.warn("column family is empty or null!");
            return;
        }

        try {
            if (!this.CF_HANDLES.containsKey(columnFamily)) {
                LOG.warn("column family [{}] not exists!", columnFamily);
                return;
            }

            this.DB.dropColumnFamily(this.CF_HANDLES.get(columnFamily));
            this.CF_HANDLES.remove(columnFamily);
            LOG.info("remove column family complete, cf:{}", columnFamily);
            // 更新ZK信息
            this.columnFamilyInfoManager.deleteColumnFamilyInfo(columnFamily);
        } catch (Exception e) {
            LOG.error("create column family error", e);
            throw e;
        }
    }

    @Override
    public boolean isOpen() {
        return true;
    }

    /**
     * @description: 从ZK中加载列族信息
     */
    private Pair<List<ColumnFamilyDescriptor>, List<Integer>> loadColumnFamilyInfo() throws Exception {
        Pair<List<ColumnFamilyDescriptor>, List<Integer>> columnFamilyInfo = new Pair<>();
        try {

            List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();
            List<Integer> cfTtlList = new ArrayList<>();
            // 添加默认列族信息，这是必须的
            cfDescriptors.add(new ColumnFamilyDescriptor("default".getBytes(), COLUMN_FAMILY_OPTIONS));
            cfTtlList.add(-1);

            if (this.client.checkExists().forPath(RocksDBZkPaths.DEFAULT_PATH_ROCKSDB_COLUMN_FAMILY_INFO) != null) {
                byte[] bytes = this.client.getData().forPath(RocksDBZkPaths.DEFAULT_PATH_ROCKSDB_COLUMN_FAMILY_INFO);
                Map<String, Integer> cfMap = JsonUtils.toObject(bytes, new TypeReference<Map<String, Integer>>() {
                });

                for (Map.Entry<String, Integer> entry : cfMap.entrySet()) {
                    cfDescriptors.add(new ColumnFamilyDescriptor(entry.getKey().getBytes(), COLUMN_FAMILY_OPTIONS));
                    cfTtlList.add(Integer.parseInt(entry.getValue().toString()));
                }
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
    public void mergeData(String srcPath) {
        List<ColumnFamilyHandle> cfHandles = new ArrayList<>();
        try (TtlDB srcDB = TtlDB.open(DB_OPTIONS, srcPath, this.columnFamilyInfo.getFirst(), cfHandles, this.columnFamilyInfo.getSecond(), true)) {
            for (ColumnFamilyHandle handle : cfHandles) {
                RocksIterator iterator = srcDB.newIterator(handle, READ_OPTIONS);
                for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                    write(handle, WRITE_OPTIONS_ASYNC, iterator.key(), iterator.value());
                }
            }
        } catch (Exception e) {
            LOG.error("data transfer err, srcPath: {}", srcPath, e);
        }
    }

    @Override
    public void updateColumnFamilyHandles(Map<String, Integer> columnFamilyMap) {
        try {
            columnFamilyMap.put("default", -1);  // CF_HANDLE中默认有default列族，所以比较前需要先put
            Set<String> cfSet = columnFamilyMap.keySet();
            Set<String> handleSet = this.CF_HANDLES.keySet();
            Sets.SetView<String> difference1 = Sets.difference(cfSet, handleSet);
            Sets.SetView<String> difference2 = Sets.difference(handleSet, cfSet);

            for (String d1 : difference1) {
                if (CF_HANDLES.containsKey(d1)) {
                    continue;
                }
                ColumnFamilyHandle handle = this.DB.createColumnFamilyWithTtl(new ColumnFamilyDescriptor(d1.getBytes(), COLUMN_FAMILY_OPTIONS), columnFamilyMap.get(d1));
                this.CF_HANDLES.put(d1, handle);
            }

            for (String d2 : difference2) {
                if (!CF_HANDLES.containsKey(d2)) {
                    continue;
                }
                this.DB.dropColumnFamily(this.CF_HANDLES.get(d2));
                this.CF_HANDLES.remove(d2);
            }

            LOG.info("update column family handle :{}", this.CF_HANDLES.keySet());
        } catch (RocksDBException e) {
            LOG.error("update column family handle err", e);
        }
    }

    @LifecycleStop
    @Override
    public void stop() throws Exception {
        this.DB_OPTIONS.close();
        this.WRITE_OPTIONS_SYNC.close();
        this.WRITE_OPTIONS_ASYNC.close();
        this.READ_OPTIONS.close();
        this.COLUMN_FAMILY_OPTIONS.close();
        Collection<ColumnFamilyHandle> handles = this.CF_HANDLES.values();
        for (ColumnFamilyHandle handle : handles) {
            handle.close();
        }
        if (DB != null) {
            DB.close();
        }
        LOG.info("rocksdb manager stop");
    }
}
