package com.bonree.brfs.duplication.datastream.connection.mock;

import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnection;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnectionFactory;

public class MockDiskNodeConnectionFactory implements DiskNodeConnectionFactory {

	@Override
	public DiskNodeConnection createConnection(Service service) {
		return new MockDiskNodeConnection(service);
	}

}
