package com.bonree.brfs.duplication.datastream.connection;

public interface DiskNodeConnectionPool {
    DiskNodeConnection getConnection(String serviceGroup, String serviceId);
}
