package com.bonree.brfs.duplication.datastream.connection.http;

import java.io.IOException;

import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.disknode.client.DiskNodeClient;
import com.bonree.brfs.disknode.client.HttpDiskNodeClient;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnection;
import com.google.common.io.Closeables;

public class HttpDiskNodeConnection implements DiskNodeConnection {
	private Service service;
	private DiskNodeClient client;
	
	public HttpDiskNodeConnection(Service service) {
		this.service = service;
	}
	
	public void connect() {
		client = new HttpDiskNodeClient(service.getHost(), service.getPort());
	}

	@Override
	public Service getService() {
		return service;
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
