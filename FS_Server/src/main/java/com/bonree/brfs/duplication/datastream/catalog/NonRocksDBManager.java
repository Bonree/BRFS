package com.bonree.brfs.duplication.datastream.catalog;

import com.bonree.brfs.rocksdb.RocksDBDataUnit;
import com.bonree.brfs.rocksdb.RocksDBManager;
import com.bonree.brfs.rocksdb.WriteStatus;
import org.rocksdb.RocksDB;

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
    public WriteStatus write(String columnFamily, String fullPath, String fid) throws Exception {
        return WriteStatus.FAILED;
    }

    @Override
    public WriteStatus write(RocksDBDataUnit dataUnit) throws Exception {
        return null;
    }

    @Override
    public boolean isWritalbe() {
        return false;
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
    public void dataTransfer(String srcPath) {

    }

    @Override
    public RocksDB getRocksDB() {
        return null;
    }

    @Override
    public void start() throws Exception {

    }

    @Override
    public void stop() throws Exception {

    }
}
