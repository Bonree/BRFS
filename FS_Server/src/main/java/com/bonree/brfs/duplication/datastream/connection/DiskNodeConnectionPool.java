package com.bonree.brfs.duplication.datastream.connection;

import java.io.Closeable;

import com.bonree.brfs.duplication.coordinator.DuplicateNode;

public interface DiskNodeConnectionPool extends Closeable{
	DiskNodeConnection getConnection(DuplicateNode duplicateNode);
}
