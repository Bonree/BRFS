package com.bonree.brfs.duplication.datastream.connection.virtual;

import java.io.IOException;

import com.bonree.brfs.disknode.client.DiskNodeClient;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnection;

public class VirtualDiskNodeConnection implements DiskNodeConnection {

	@Override
	public void close() throws IOException {
	}
	
	@Override
	public String getRemoteAddress() {
		return null;
	}

	@Override
	public int getRemotePort() {
		return 0;
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
