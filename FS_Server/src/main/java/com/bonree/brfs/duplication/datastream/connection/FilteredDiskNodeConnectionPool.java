package com.bonree.brfs.duplication.datastream.connection;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.utils.CloseUtils;
import com.bonree.brfs.duplication.coordinator.DuplicateNode;

public class FilteredDiskNodeConnectionPool implements DiskNodeConnectionPool {
	private Map<String, DiskNodeConnectionPool> connectionPool = new HashMap<String, DiskNodeConnectionPool>();
	
	public void addFactory(String serviceGroup, DiskNodeConnectionPool pool) {
		connectionPool.put(serviceGroup, pool);
	}

	@Override
	public void close() throws IOException {
		for(DiskNodeConnectionPool pool : connectionPool.values()) {
			CloseUtils.closeQuietly(pool);
		}
	}

	@Override
	public DiskNodeConnection getConnection(DuplicateNode duplicateNode) {
		DiskNodeConnectionPool pool = connectionPool.get(duplicateNode.getGroup());
		if(pool == null) {
			return null;
		}
		
		return pool.getConnection(duplicateNode);
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
