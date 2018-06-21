package com.bonree.brfs.duplication.datastream.connection.http;

import com.bonree.brfs.common.net.http.client.ClientConfig;
import com.bonree.brfs.common.utils.CloseUtils;
import com.bonree.brfs.disknode.client.DiskNodeClient;
import com.bonree.brfs.disknode.client.HttpDiskNodeClient;
import com.bonree.brfs.duplication.datastream.connection.DiskNodeConnection;

public class HttpDiskNodeConnection implements DiskNodeConnection {
	
	private static final int DEFAULT_RESPONSE_TIMEOUT_MILLIS = 15 * 1000;
	
	private String address;
	private int port;
	private DiskNodeClient client;
	
	public HttpDiskNodeConnection(String address, int port) {
		this.address = address;
		this.port = port;
	}
	
	public void connect() {
		int defaultMaxConnectionPerRoute = Math.max(4, Runtime.getRuntime().availableProcessors() / 4);
		
		ClientConfig clientConfig = ClientConfig.builder()
				.setResponseTimeout(DEFAULT_RESPONSE_TIMEOUT_MILLIS)
				.setMaxConnectionPerRoute(defaultMaxConnectionPerRoute)
				.setMaxConnection(defaultMaxConnectionPerRoute * 3)
				.build();
		
		client = new HttpDiskNodeClient(address, port, clientConfig);
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
		CloseUtils.closeQuietly(client);
	}

}
