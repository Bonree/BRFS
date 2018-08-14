package com.bonree.brfs.client.impl;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.Executor;

import com.bonree.brfs.common.net.tcp.client.TcpClient;
import com.bonree.brfs.common.net.tcp.client.TcpClientCloseListener;
import com.bonree.brfs.common.net.tcp.client.TcpClientGroup;
import com.bonree.brfs.common.net.tcp.file.ReadObject;
import com.bonree.brfs.common.net.tcp.file.client.AsyncFileReaderCreateConfig;
import com.bonree.brfs.common.net.tcp.file.client.FileContentPart;
import com.bonree.brfs.common.service.Service;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;


public class ConnectionPool {
	private TcpClientGroup<ReadObject, FileContentPart, AsyncFileReaderCreateConfig> group;
	private Executor executor;
	private Table<String, String, TcpClient<ReadObject, FileContentPart>> clientCache;
	
	public ConnectionPool(TcpClientGroup<ReadObject, FileContentPart, AsyncFileReaderCreateConfig> group, Executor executor) {
		this.group = group;
		this.executor = executor;
		this.clientCache = HashBasedTable.create();
	}
	
	public TcpClient<ReadObject, FileContentPart> getConnection(Service service) {
		TcpClient<ReadObject, FileContentPart> client = clientCache.get(service.getServiceGroup(), service.getServiceId());
		
		if(client != null) {
			return client;
		}
		
		synchronized (clientCache) {
			try {
				client = group.createClient(new AsyncFileReaderCreateConfig() {
					
					@Override
					public SocketAddress remoteAddress() {
						return new InetSocketAddress(service.getHost(), service.getExtraPort());
					}
					
					@Override
					public int connectTimeoutMillis() {
						return 3000;
					}

					@Override
					public int maxPendingRead() {
						return 1000 * 100;
					}
					
				}, executor);
				
				if(client == null) {
					return null;
				}
				
				client.setClientCloseListener(new TcpClientCloseListener() {
					
					@Override
					public void clientClosed() {
						synchronized (clientCache) {
							clientCache.remove(service.getServiceGroup(), service.getServiceId());
						}
					}
				});
				
				clientCache.put(service.getServiceGroup(), service.getServiceId(), client);
				return client;
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		return null;
	}
}
