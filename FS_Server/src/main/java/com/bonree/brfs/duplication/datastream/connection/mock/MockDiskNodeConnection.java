package com.bonree.brfs.duplication.datastream.connection.mock;

import java.io.IOException;

import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.disknode.client.DiskNodeClient;
import com.bonree.brfs.disknode.client.mock.MockDiskNodeClient;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnection;

public class MockDiskNodeConnection implements DiskNodeConnection {
	private Service service;
	
	public MockDiskNodeConnection(Service service) {
		this.service = service;
	}

	@Override
	public void close() throws IOException {
	}

	@Override
	public Service getService() {
		return service;
	}

	@Override
	public void connect() {
	}

	@Override
	public boolean isValid() {
		return true;
	}

	@Override
	public DiskNodeClient getClient() {
		return new MockDiskNodeClient();
	}

}
