package com.bonree.brfs.rocksdb.impl;

import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.lifecycle.LifecycleStart;
import com.bonree.brfs.common.lifecycle.LifecycleStop;
import com.bonree.brfs.common.rocksdb.RocksDBManager;
import com.bonree.brfs.common.rocksdb.WriteStatus;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.service.ServiceStateListener;
import com.bonree.brfs.common.supervisor.TimeWatcher;
import com.bonree.brfs.common.utils.Pair;
import com.bonree.brfs.common.utils.PooledThreadFactory;
import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.CommonConfigs;
import com.bonree.brfs.configuration.units.RocksDBConfigs;
import com.bonree.brfs.duplication.storageregion.StorageRegion;
import com.bonree.brfs.duplication.storageregion.StorageRegionManager;
import com.bonree.brfs.rocksdb.backup.BackupEngineFactory;
import com.bonree.brfs.rocksdb.connection.RegionNodeConnection;
import com.bonree.brfs.rocksdb.connection.RegionNodeConnectionPool;
import com.bonree.brfs.rocksdb.zk.ColumnFamilyInfoManager;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import org.apache.curator.framework.CuratorFramework;
import org.rocksdb.BackupEngine;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.FlushOptions;
import org.rocksdb.LRUCache;
import org.rocksdb.Options;
import org.rocksdb.RateLimiter;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.TtlDB;
import org.rocksdb.WriteOptions;
import org.rocksdb.util.SizeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private static final int DEFAULT_QUEUE_FLUSH = 5000;

    private DBOptions dbOptions;
    private ReadOptions readOptions;
    private WriteOptions writeOptionsSync;
    private WriteOptions writeOptionsAsync;
    private BloomFilter bloomFilter;
    private BlockBasedTableConfig blockBasedTableConfig;
    private ColumnFamilyOptions columnFamilyOptions;
    private List<CompressionType> compressionTypes;
    private Map<String, ColumnFamilyHandle> cfHandles;

    private TtlDB db;
    private Service service;
    private RocksDBConfig config;
    private CuratorFramework client;
    private ServiceManager serviceManager;
    private StorageRegionManager srManager;
    private String regionGroupName;
    private RegionNodeConnectionPool regionNodeConnectionPool;
    private ColumnFamilyInfoManager columnFamilyInfoManager;
    private Pair<List<ColumnFamilyDescriptor>, List<Integer>> columnFamilyInfo;

    private int dataSynchronizeCountOnce;
    private List<Service> serviceCache = new CopyOnWriteArrayList<>();
    private TimeWatcher watcher = new TimeWatcher();
    private BlockingQueue<RocksDBDataUnit> queue = new ArrayBlockingQueue<>(500);

    private ScheduledExecutorService queueChecker =
        Executors.newSingleThreadScheduledExecutor(new PooledThreadFactory("queue_checker"));

    @Inject
    public DefaultRocksDBManager(CuratorFramework client, ZookeeperPaths zkPaths, Service service, ServiceManager serviceManager,
                                 StorageRegionManager srManager, RegionNodeConnectionPool regionNodeConnectionPool) {
        this.client = client.usingNamespace(zkPaths.getBaseRocksDBPath().substring(1));
        this.service = service;
        this.serviceManager = serviceManager;
        this.srManager = srManager;
        this.regionGroupName = Configs.getConfiguration().getConfig(CommonConfigs.CONFIG_REGION_SERVICE_GROUP_NAME);
        this.dataSynchronizeCountOnce = Configs.getConfiguration().getConfig(RocksDBConfigs.ROCKSDB_DATA_SYNCHRONIZE_COUNT_ONCE);
        this.regionNodeConnectionPool = regionNodeConnectionPool;
        this.columnFamilyInfoManager = new ColumnFamilyInfoManager(this.client);

        dbOptions = new DBOptions();
        readOptions = new ReadOptions();
        writeOptionsSync = new WriteOptions();
        writeOptionsAsync = new WriteOptions();
        writeOptionsSync.setSync(true);
        writeOptionsAsync.setSync(false);
        bloomFilter = new BloomFilter(10);
        blockBasedTableConfig = new BlockBasedTableConfig();
        columnFamilyOptions = new ColumnFamilyOptions();
        compressionTypes = new ArrayList<>();
        cfHandles = new ConcurrentHashMap<>();
        config = RocksDBConfig.newBuilder()
                              .setMaxBackgroundFlush(
                                  Configs.getConfiguration().getConfig(RocksDBConfigs.ROCKSDB_MAX_BACKGROUND_FLUSH))
                              .setMaxBackgroundCompaction(
                                  Configs.getConfiguration().getConfig(RocksDBConfigs.ROCKSDB_MAX_BACKGROUND_COMPACTION))
                              .setMaxOpenFiles(Configs.getConfiguration().getConfig(RocksDBConfigs.ROCKSDB_MAX_OPEN_FILES))
                              .setMaxSubCompaction(
                                  Configs.getConfiguration().getConfig(RocksDBConfigs.ROCKSDB_MAX_SUBCOMPACTIONN))
                              .setBlockCache(Configs.getConfiguration().getConfig(RocksDBConfigs.ROCKSDB_BLOCK_CACHE))
                              .setWriteBufferSize(Configs.getConfiguration().getConfig(RocksDBConfigs.ROCKSDB_WRITE_BUFFER_SIZE))
                              .setMaxWriteBufferNumber(
                                  Configs.getConfiguration().getConfig(RocksDBConfigs.ROCKSDB_MAX_WRITE_BUFFER_NUMBER))
                              .setMinWriteBufferNumToMerge(
                                  Configs.getConfiguration().getConfig(RocksDBConfigs.ROCKSDB_MIN_WRITE_BUFFER_NUM_TO_MERGE))
                              .setLevel0FileNumCompactionTrigger(
                                  Configs.getConfiguration().getConfig(RocksDBConfigs.ROCKSDB_LEVEL0_FILE_NUM_COMPACTION_TRIGGER))
                              .setTargetFileSizeBase(
                                  Configs.getConfiguration().getConfig(RocksDBConfigs.ROCKSDB_TARGET_FILE_SIZE_BASE))
                              .setMaxBytesLevelBase(
                                  Configs.getConfiguration().getConfig(RocksDBConfigs.ROCKSDB_MAX_BYTES_LEVEL_BASE))
                              .build();
        LOG.info("RocksDB configs:{}", this.config);
    }

    @LifecycleStart
    @Override
    public void start() throws Exception {
        this.serviceManager.addServiceStateListener(regionGroupName, new ServiceStateListener() {
            @Override
            public void serviceAdded(Service service) {
                if (!serviceCache.contains(service)
                    && !service.getServiceId().equals(DefaultRocksDBManager.this.service.getServiceId())) {
                    serviceCache.add(service);
                    LOG.info("service added:{}, current services:{}", service.getServiceId(), serviceCache);
                }
            }

            @Override
            public void serviceRemoved(Service service) {
                serviceCache.remove(service);
                LOG.info("service removed:{}, current services:{}", service.getServiceId(), serviceCache);
            }
        });

        this.dbOptions.setCreateIfMissing(true)
                      //.setRateLimiter(
                      //    new RateLimiter(Configs.getConfiguration().getConfig(RocksDBConfigs.ROCKSDB_RATE_BYTES_PER_SECOND)))
                      //.setCompactionReadaheadSize(
                      //    Configs.getConfiguration().getConfig(RocksDBConfigs.ROCKSDB_COMPACTION_READHEAD_SIZE))
                      .setCreateMissingColumnFamilies(true)
                      .setMaxBackgroundFlushes(this.config.getMaxBackgroundFlush())
                      .setMaxBackgroundCompactions(this.config.getMaxBackgroundCompaction())
                      .setMaxOpenFiles(this.config.getMaxOpenFiles())
                      .setRowCache(new LRUCache(this.config.getBlockCache() * SizeUnit.MB, 6, true, 5))
                      .setMaxSubcompactions(this.config.getMaxSubCompaction());
        readOptions.setPrefixSameAsStart(true);
        blockBasedTableConfig.setFilter(bloomFilter)
                             .setCacheIndexAndFilterBlocks(true)
                             .setPinL0FilterAndIndexBlocksInCache(true);

        compressionTypes.addAll(Arrays.asList(
            CompressionType.NO_COMPRESSION, CompressionType.NO_COMPRESSION,
            CompressionType.SNAPPY_COMPRESSION, CompressionType.SNAPPY_COMPRESSION,
            CompressionType.SNAPPY_COMPRESSION, CompressionType.SNAPPY_COMPRESSION,
            CompressionType.ZLIB_COMPRESSION
        ));

        columnFamilyOptions.setTableFormatConfig(blockBasedTableConfig)
                           .setCompactionStyle(CompactionStyle.LEVEL)
                           .setWriteBufferSize(this.config.getWriteBufferSize() * SizeUnit.MB)
                           .setMaxWriteBufferNumber(this.config.getMaxWriteBufferNumber())
                           .setMinWriteBufferNumberToMerge(this.config.getMinWriteBufferNumToMerge())
                           .setLevel0FileNumCompactionTrigger(this.config.getLevel0FileNumCompactionTrigger())
                           .setCompressionPerLevel(compressionTypes)
                           .setTargetFileSizeBase(this.config.getTargetFileSizeBase() * SizeUnit.MB)
                           .setMaxBytesForLevelBase(this.config.getMaxBytesLevelBase() * SizeUnit.MB)
                           .setOptimizeFiltersForHits(true);
        queueChecker.scheduleAtFixedRate(new QueueChecker(), 0L, 1L, TimeUnit.MILLISECONDS);

        try {
            columnFamilyInfo = loadColumnFamilyInfo();
            List<ColumnFamilyDescriptor> cfDescriptors = columnFamilyInfo.getFirst();
            List<Integer> cfTtlList = columnFamilyInfo.getSecond();
            List<ColumnFamilyHandle> cfHandles = new ArrayList<>();

            TimeWatcher watcher = new TimeWatcher();
            db = TtlDB.open(dbOptions, Configs.getConfiguration().getConfig(RocksDBConfigs.ROCKSDB_STORAGE_PATH), cfDescriptors,
                            cfHandles, cfTtlList, false);
            int openTime = watcher.getElapsedTime();
            LOG.info("RocksDB init complete, open RocksDB cost time:{}", openTime);
            cacheCFHandles(cfHandles);

            // 同步sr信息和列族信息，使其一致
            syncColumnFamilyByStorageRegionInfo();
            LOG.info("load column family info:{}", this.cfHandles.keySet());
        } catch (RocksDBException e) {
            LOG.error("RocksDB start error", e);
            throw e;
        }
    }

    @Override
    public void createNewBackup(String backupPath) throws RocksDBException {
        BackupEngine backupEngine = BackupEngineFactory.getInstance().getBackupEngineByPath(backupPath);
        backupEngine.createNewBackup(this.db);
    }

    @Override
    public byte[] read(String columnFamily, byte[] key) {
        if (null == columnFamily || columnFamily.isEmpty() || null == key) {
            LOG.warn("read column family is empty or key is null!");
            return null;
        }

        try {
            byte[] result = db.get(this.cfHandles.get(columnFamily), readOptions, key);
            if (result == null) {
                RegionNodeConnection connection;
                String queryKey = new String(key);
                for (Service service : serviceCache) {
                    connection = this.regionNodeConnectionPool.getConnection(regionGroupName, service.getServiceId());
                    if (connection == null || connection.getClient() == null) {
                        LOG.warn("region node connection/client is null! serviceId:{}", service.getServiceId());
                        continue;
                    }

                    result = connection.getClient().readData(columnFamily, queryKey);
                    if (result == null) {
                        continue;
                    }
                    LOG.info("read data from [{}] success, cf:{}, key:{}", service.getServiceId(), columnFamily, queryKey);
                    return result;
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
            return db.get(this.cfHandles.get(columnFamily), readOptions, key);
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

        Map<byte[], byte[]> result = new LinkedHashMap<>();
        try (RocksIterator iterator = this.newIterator(this.cfHandles.get(columnFamily))) {
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
    public WriteStatus syncData(String columnFamily, byte[] key, byte[] value) throws RocksDBException {
        return this.write(this.cfHandles.get(columnFamily), writeOptionsAsync, key, value);
    }

    @Override
    public WriteStatus write(String columnFamily, byte[] key, byte[] value) throws Exception {
        if (null == columnFamily || columnFamily.isEmpty() || null == key || null == value) {
            LOG.warn("write column family is empty or key/value is null!");
            return WriteStatus.FAILED;
        }

        WriteStatus writeStatus = this.write(this.cfHandles.get(columnFamily), writeOptionsAsync, key, value);
        boolean offer = queue.offer(new RocksDBDataUnit(columnFamily, key, value));
        if (!offer) {
            LOG.warn("offer data ro queue failed, size:{}", queue.size());
        }
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

    private WriteStatus write(final ColumnFamilyHandle cfh, final WriteOptions writeOptions, final byte[] key, final byte[] value)
        throws RocksDBException {
        try {
            this.db.put(cfh, writeOptions, key, value);
        } catch (RocksDBException e) {
            LOG.error("write occur error, cf:{}, key:{}, value:{}", new String(cfh.getName()), new String(key), new String(value),
                      e);
            return WriteStatus.FAILED;
        }
        return WriteStatus.SUCCESS;
    }

    private class QueueChecker implements Runnable {

        @Override
        public void run() {
            if (serviceCache.isEmpty()) {
                LOG.debug("service cache is empty!");
                return;
            }
            if (queue.isEmpty()) {
                LOG.debug("sync queue is empty!");
                return;
            }

            if (queue.size() >= dataSynchronizeCountOnce) {
                dataSynchronizer(dataSynchronizeCountOnce);
            } else if (watcher.getElapsedTime() >= DEFAULT_QUEUE_FLUSH) {
                dataSynchronizer(queue.size());
                watcher.getElapsedTimeAndRefresh();
            }
        }

        public void dataSynchronizer(int size) {
            List<RocksDBDataUnit> datas = new ArrayList<>(size);
            queue.drainTo(datas, size);
            LOG.info("current sync data count:{}, queue size:{}", size, queue.size());
            RegionNodeConnection connection;
            for (Service service : serviceCache) {
                connection =
                    DefaultRocksDBManager.this.regionNodeConnectionPool
                        .getConnection(regionGroupName, service.getServiceId());
                if (connection == null || connection.getClient() == null) {
                    LOG.debug("region node connection/client is null! serviceId:{}", service.getServiceId());
                    continue;
                }
                try {
                    connection.getClient().writeBatchData(datas);
                } catch (Exception e) {
                    LOG.error("rocksdb data writer occur error", e);
                }
            }
        }
    }

    private RocksIterator newIterator(ColumnFamilyHandle cfh) {
        return this.db.newIterator(cfh, readOptions);
    }

    @Override
    public void createColumnFamilyWithTtl(String columnFamily, int ttl) throws Exception {
        if (null == columnFamily || columnFamily.isEmpty()) {
            LOG.warn("column family is empty or null!");
            return;
        }

        try {
            if (this.cfHandles.containsKey(columnFamily)) {
                LOG.warn("column family [{}] already exists!", columnFamily);
                return;
            }

            ColumnFamilyHandle handle =
                this.db
                    .createColumnFamilyWithTtl(new ColumnFamilyDescriptor(columnFamily.getBytes(), columnFamilyOptions), ttl);
            this.cfHandles.put(columnFamily, handle);
            LOG.info("create column family complete, name:{}, ttl:{}, id:{}", columnFamily, ttl, handle.getID());
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
            if (!this.cfHandles.containsKey(columnFamily)) {
                LOG.warn("column family [{}] not exists!", columnFamily);
                return;
            }

            this.db.dropColumnFamily(this.cfHandles.get(columnFamily));
            this.cfHandles.remove(columnFamily);
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
     * @description: 从RocksDB中加载列族信息
     */
    private Pair<List<ColumnFamilyDescriptor>, List<Integer>> loadColumnFamilyInfo() throws Exception {
        Pair<List<ColumnFamilyDescriptor>, List<Integer>> columnFamilyInfo = new Pair<>();

        try (Options options = new Options()) {
            List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();
            List<Integer> cfTtlList = new ArrayList<>();
            // 添加默认列族信息，这是必须的
            cfDescriptors.add(new ColumnFamilyDescriptor("default".getBytes(), columnFamilyOptions));
            cfTtlList.add(-1);

            List<byte[]> columnFamilies =
                TtlDB.listColumnFamilies(options, Configs.getConfiguration().getConfig(RocksDBConfigs.ROCKSDB_STORAGE_PATH));
            Map<String, Integer> srNameAndDataTtl = getStorageRegionNameAndDataTtl();

            for (byte[] columnFamily : columnFamilies) {
                cfDescriptors.add(new ColumnFamilyDescriptor(columnFamily, columnFamilyOptions));
                cfTtlList.add(srNameAndDataTtl.getOrDefault(new String(columnFamily), -1));
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
            this.cfHandles.put(new String(cfHandle.getName()), cfHandle);
        }
    }

    private void syncColumnFamilyByStorageRegionInfo() {
        Map<String, Integer> srNameAndDataTtl = getStorageRegionNameAndDataTtl();
        // 先更新本地缓存
        updateColumnFamilyHandles(srNameAndDataTtl);
        // 再重置zk上保存的列族信息
        this.columnFamilyInfoManager.resetColumnFamilyInfo(srNameAndDataTtl);
        LOG.info("sync column family by storage region info complete, sr list:{}", srNameAndDataTtl);
    }

    private Map<String, Integer> getStorageRegionNameAndDataTtl() {
        List<StorageRegion> srList = this.srManager.getStorageRegionList();
        if (srList == null || srList.isEmpty()) {
            return Maps.newHashMap();
        }

        Map<String, Integer> srNameAndDataTtl = new HashMap<>();
        for (StorageRegion sr : srList) {
            srNameAndDataTtl.put(sr.getName(), (int) Duration.parse(sr.getDataTtl()).getSeconds());
        }
        return srNameAndDataTtl;
    }

    @Override
    public void mergeData(String srcPath) {
        List<ColumnFamilyHandle> cfHandles = new ArrayList<>();
        try (TtlDB srcDB = TtlDB
            .open(dbOptions, srcPath, this.columnFamilyInfo.getFirst(), cfHandles, this.columnFamilyInfo.getSecond(), true)) {
            for (ColumnFamilyHandle handle : cfHandles) {
                RocksIterator iterator = srcDB.newIterator(handle, readOptions);
                for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                    write(handle, writeOptionsAsync, iterator.key(), iterator.value());
                }
            }
        } catch (Exception e) {
            LOG.error("data transfer err, srcPath: {}", srcPath, e);
        }
    }

    @Override
    public void updateColumnFamilyHandles(Map<String, Integer> columnFamilyMap) {
        try {
            // CF_HANDLE中默认有default列族，所以比较前需要先put
            columnFamilyMap.put("default", -1);
            Set<String> updatedCfs = columnFamilyMap.keySet();
            Set<String> currentCfs = this.cfHandles.keySet();
            Sets.SetView<String> diffUpdatedCfs = Sets.difference(updatedCfs, currentCfs);
            Sets.SetView<String> diffCurrentCfs = Sets.difference(currentCfs, updatedCfs);

            for (String diff : diffUpdatedCfs) {
                if (!cfHandles.containsKey(diff)) {
                    ColumnFamilyHandle handle = this.db
                        .createColumnFamilyWithTtl(new ColumnFamilyDescriptor(diff.getBytes(), columnFamilyOptions),
                                                   columnFamilyMap.get(diff));
                    this.cfHandles.put(diff, handle);
                    LOG.info("add column family of sr [{}] to rocksdb, id:[{}]", diff, handle.getID());
                }
            }

            for (String diff : diffCurrentCfs) {
                if (cfHandles.containsKey(diff)) {
                    this.db.dropColumnFamily(this.cfHandles.get(diff));
                    this.cfHandles.remove(diff);
                    LOG.info("remove column family of sr [{}] from rocksdb", diff);
                }
            }

            LOG.info("update column family handle :{}", this.cfHandles.keySet());
        } catch (Exception e) {
            LOG.error("update column family handle err", e);
        }
    }

    @LifecycleStop
    @Override
    public void stop() throws Exception {
        this.dbOptions.close();
        this.writeOptionsSync.close();
        this.writeOptionsAsync.close();
        this.readOptions.close();
        this.columnFamilyOptions.close();
        Collection<ColumnFamilyHandle> handles = this.cfHandles.values();
        FlushOptions flushOptions = new FlushOptions();
        for (ColumnFamilyHandle handle : handles) {
            db.flush(flushOptions, handle);
            LOG.info("flush column family [{}] complete, id:[{}]", new String(handle.getName()), handle.getID());
            handle.close();
        }
        flushOptions.close();
        if (db != null) {
            db.close();
        }
        this.queueChecker.shutdown();
        LOG.info("rocksdb manager stop");
    }
}
