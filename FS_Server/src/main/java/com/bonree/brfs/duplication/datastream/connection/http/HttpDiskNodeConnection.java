package com.bonree.brfs.duplication.datastream.connection.http;

import java.io.IOException;

import com.bonree.brfs.common.http.client.ClientConfig;
import com.bonree.brfs.disknode.client.DiskNodeClient;
import com.bonree.brfs.disknode.client.HttpDiskNodeClient;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnection;
import com.google.common.io.Closeables;

public class HttpDiskNodeConnection implements DiskNodeConnection {
	private String address;
	private int port;
	private DiskNodeClient client;
	
	public HttpDiskNodeConnection(String address, int port) {
		this.address = address;
		this.port = port;
	}
	
	public void connect() {
		client = new HttpDiskNodeClient(address, port, ClientConfig.builder().setResponseTimeout(3000).build());
	}

	@Override
	public String getRemoteAddress() {
		return address;
	}
	
	@Override
	public int getRemotePort() {
		return port;
	}

	@Override
	public boolean isValid() {
		return client != null && client.ping();
	}

	@Override
	public DiskNodeClient getClient() {
		return client;
	}

	@Override
	public void close() {
		try {
			Closeables.close(client, true);
		} catch (IOException ignore) {}
	}

}
