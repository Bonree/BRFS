package com.bonree.brfs.duplication.datastream.catalog;

import com.bonree.brfs.common.rocksdb.RocksDBDataUnit;
import com.bonree.brfs.common.rocksdb.RocksDBManager;
import com.bonree.brfs.common.rocksdb.WriteStatus;

import java.util.Map;

public class NonRocksDBManager implements RocksDBManager {

    @Override
    public byte[] read(String columnFamily, byte[] key) {
        return new byte[0];
    }

    @Override
    public byte[] read(String columnFamily, byte[] key, boolean readFormOther) {
        return new byte[0];
    }

    @Override
    public Map<byte[], byte[]> readByPrefix(String columnFamily, byte[] prefixKey) {
        return null;
    }

    @Override
    public WriteStatus write(String columnFamily, byte[] key, byte[] value) throws Exception {
        return null;
    }

    @Override
    public WriteStatus write(String columnFamily, byte[] key, byte[] value, boolean force) throws Exception {
        return null;
    }

    @Override
    public WriteStatus write(RocksDBDataUnit dataUnit) throws Exception {
        return null;
    }

    @Override
    public void createColumnFamilyWithTtl(String columnFamily, int ttl) throws Exception {

    }

    @Override
    public void deleteColumnFamily(String columnFamily) throws Exception {

    }

    @Override
    public void updateColumnFamilyHandles(Map<String, Integer> columnFamilyMap) throws Exception {

    }

    @Override
    public void mergeData(String srcPath) {

    }

    @Override
    public void createNewBackup(String backupPath) throws Exception {

    }

    @Override
    public void start() throws Exception {

    }

    @Override
    public void stop() throws Exception {

    }

    @Override
    public boolean isOpen() {
        return false;
    }
}
