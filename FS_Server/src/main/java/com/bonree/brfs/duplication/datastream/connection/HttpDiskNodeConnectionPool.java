package com.bonree.brfs.duplication.datastream.connection;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.utils.PooledThreadFactory;
import com.bonree.brfs.duplication.coordinator.DuplicateNode;
import com.google.common.io.Closeables;

public class HttpDiskNodeConnectionPool implements DiskNodeConnectionPool {
	
	private static final int DEFAULT_CONNECTION_STATE_CHECK_INTERVAL = 1;
	private ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor(new PooledThreadFactory("connection_checker"));
	private Map<DuplicateNode, DiskNodeConnection> connectionCache = new HashMap<DuplicateNode, DiskNodeConnection>();
	private DiskNodeConnectionFactory connectionFactory;
	
	private ServiceManager serviceManager;
	
	public HttpDiskNodeConnectionPool(ServiceManager serviceManager, DiskNodeConnectionFactory connectionFactory) {
		this.serviceManager = serviceManager;
		this.connectionFactory = connectionFactory;
		exec.scheduleAtFixedRate(new ConnectionStateChecker(), 0, DEFAULT_CONNECTION_STATE_CHECK_INTERVAL, TimeUnit.SECONDS);
	}
	
	@Override
	public void close() {
		exec.shutdown();
		connectionCache.values().forEach(new Consumer<DiskNodeConnection>() {

			@Override
			public void accept(DiskNodeConnection connection) {
				try {
					Closeables.close(connection, true);
				} catch (IOException ignore) {}
			}
		});
		connectionCache.clear();
	}

	@Override
	public DiskNodeConnection getConnection(DuplicateNode duplicateNode) {
		DiskNodeConnection connection = connectionCache.get(duplicateNode);
		
		if(connection != null) {
			return connection;
		}
		
		synchronized (connectionCache) {
			Service service = serviceManager.getServiceById(duplicateNode.getGroup(), duplicateNode.getId());
			if(service == null) {
				return null;
			}
			
			connection = connectionFactory.createConnection(service);
			connectionCache.put(duplicateNode, connection);
		}
		
		return connection;
	}
	
	@Override
	public DiskNodeConnection[] getConnections(DuplicateNode[] duplicateNodes) {
		DiskNodeConnection[] connections = new DiskNodeConnection[duplicateNodes.length];
		
		for(int i = 0; i < connections.length; i++) {
			connections[i] = getConnection(duplicateNodes[i]);
		}
		
		return connections;
	}

	private class ConnectionStateChecker implements Runnable {

		@Override
		public void run() {
			List<DuplicateNode> invalidKeys = new ArrayList<DuplicateNode>();
			for(DuplicateNode node : connectionCache.keySet()) {
				if(!connectionCache.get(node).isValid()) {
					invalidKeys.add(node);
				}
			}
			
			invalidKeys.forEach(new Consumer<DuplicateNode>() {

				@Override
				public void accept(DuplicateNode node) {
					connectionCache.remove(node);
				}
			});
		}
		
	}
}
