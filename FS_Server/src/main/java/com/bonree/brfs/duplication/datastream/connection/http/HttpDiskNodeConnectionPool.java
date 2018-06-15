package com.bonree.brfs.duplication.datastream.connection.http;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.utils.CloseUtils;
import com.bonree.brfs.common.utils.PooledThreadFactory;
import com.bonree.brfs.duplication.coordinator.DuplicateNode;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnection;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnectionPool;

public class HttpDiskNodeConnectionPool implements DiskNodeConnectionPool {
	private static final Logger LOG = LoggerFactory.getLogger(HttpDiskNodeConnectionPool.class);
	
	private static final int DEFAULT_CONNECTION_STATE_CHECK_INTERVAL = 3;
	private ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor(new PooledThreadFactory("connection_checker"));
	private Map<DuplicateNode, HttpDiskNodeConnection> connectionCache = new HashMap<DuplicateNode, HttpDiskNodeConnection>();
	
	private ServiceManager serviceManager;
	
	public HttpDiskNodeConnectionPool(ServiceManager serviceManager) {
		this.serviceManager = serviceManager;
		exec.scheduleAtFixedRate(new ConnectionStateChecker(), 0, DEFAULT_CONNECTION_STATE_CHECK_INTERVAL, TimeUnit.SECONDS);
	}
	
	@Override
	public void close() {
		exec.shutdown();
		connectionCache.values().forEach(new Consumer<DiskNodeConnection>() {

			@Override
			public void accept(DiskNodeConnection connection) {
				CloseUtils.closeQuietly(connection);
			}
		});
		connectionCache.clear();
	}

	@Override
	public DiskNodeConnection getConnection(DuplicateNode duplicateNode) {
		HttpDiskNodeConnection connection = connectionCache.get(duplicateNode);
		
		if(connection != null) {
			return connection;
		}
		
		synchronized (connectionCache) {
			Service service = serviceManager.getServiceById(duplicateNode.getGroup(), duplicateNode.getId());
			if(service == null) {
				return null;
			}
			
			connection = new HttpDiskNodeConnection(service.getHost(), service.getPort());
			connection.connect();
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
			for(Entry<DuplicateNode, HttpDiskNodeConnection> entry : connectionCache.entrySet()) {
				if(!entry.getValue().isValid()) {
					LOG.info("Connection to node{} is invalid!", entry.getKey());
					invalidKeys.add(entry.getKey());
				}
			}
			
			invalidKeys.forEach(new Consumer<DuplicateNode>() {

				@Override
				public void accept(DuplicateNode node) {
					HttpDiskNodeConnection connection = connectionCache.remove(node);
					if(connection != null) {
						CloseUtils.closeQuietly(connection);
					}
				}
			});
		}
		
	}
}
