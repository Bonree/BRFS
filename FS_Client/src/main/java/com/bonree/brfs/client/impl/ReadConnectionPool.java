package com.bonree.brfs.client.impl;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

import com.bonree.brfs.common.service.Service;

public class ReadConnectionPool implements Closeable {
	private Map<String, ReadConnection> connections = new HashMap<String, ReadConnection>();
	
	public ReadConnection getConnection(Service service) throws IOException {
		ReadConnection connection = connections.get(service.getServiceId());
		if(connection == null) {
			synchronized (connections) {
				connection = connections.get(service.getServiceId());
				if(connection == null) {
					Socket socket = new Socket();
			    	socket.connect(new InetSocketAddress(service.getHost(), service.getExtraPort()));
			    	
			    	connection = new ReadConnection(socket);
			    	connections.put(service.getServiceId(), connection);
				}
			}
		}
		
		return connection;
	}

	@Override
	public void close() throws IOException {
		for(ReadConnection conn : connections.values()) {
			try {
				conn.close();
			} catch (Exception e) {
			}
		}
	}

}
