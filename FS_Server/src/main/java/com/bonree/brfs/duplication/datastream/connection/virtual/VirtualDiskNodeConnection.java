package com.bonree.brfs.duplication.datastream.connection.virtual;

import java.io.IOException;

import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.disknode.client.DiskNodeClient;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnection;

public class VirtualDiskNodeConnection implements DiskNodeConnection {
	private Service virtualService;
	
	public VirtualDiskNodeConnection(Service virtualService) {
		this.virtualService = virtualService;
	}

	@Override
	public void close() throws IOException {
	}

	@Override
	public Service getService() {
		return virtualService;
	}

	@Override
	public boolean isValid() {
		return true;
	}

	@Override
	public DiskNodeClient getClient() {
		return null;
	}

}
