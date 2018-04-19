package com.bonree.brfs.duplication.datastream.connection;

import com.bonree.brfs.common.service.Service;

public class HttpDiskNodeConnectionFactory implements DiskNodeConnectionFactory {

	@Override
	public DiskNodeConnection createConnection(Service service) {
		DiskNodeConnection connection = new HttpDiskNodeConnection(service);
		connection.connect();
		return connection;
	}

}
