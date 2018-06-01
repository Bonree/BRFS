package com.bonree.brfs.duplication.datastream.connection.virtual;

import java.io.IOException;

import com.bonree.brfs.duplication.coordinator.DuplicateNode;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnection;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnectionPool;

public class VirtualDiskNodeConnectionPool implements DiskNodeConnectionPool {

	@Override
	public void close() throws IOException {
	}

	@Override
	public DiskNodeConnection getConnection(DuplicateNode duplicateNode) {
		return new VirtualDiskNodeConnection();
	}

	@Override
	public DiskNodeConnection[] getConnections(DuplicateNode[] duplicateNodes) {
		DiskNodeConnection[] connections = new DiskNodeConnection[duplicateNodes.length];
		for(int i = 0; i < connections.length; i++) {
			connections[i] = getConnection(duplicateNodes[i]);
		}
		
		return connections;
	}

}
