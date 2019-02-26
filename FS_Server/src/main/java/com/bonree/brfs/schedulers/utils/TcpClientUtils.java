package com.bonree.brfs.schedulers.utils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

import com.bonree.brfs.common.net.tcp.BaseMessage;
import com.bonree.brfs.common.net.tcp.BaseResponse;
import com.bonree.brfs.common.net.tcp.client.AsyncTcpClientGroup;
import com.bonree.brfs.common.net.tcp.client.TcpClient;
import com.bonree.brfs.common.net.tcp.client.TcpClientConfig;
import com.bonree.brfs.common.net.tcp.file.ReadObject;
import com.bonree.brfs.common.net.tcp.file.client.AsyncFileReaderCreateConfig;
import com.bonree.brfs.common.net.tcp.file.client.AsyncFileReaderGroup;
import com.bonree.brfs.common.net.tcp.file.client.FileContentPart;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.disknode.client.TcpDiskNodeClient;

public class TcpClientUtils {
	public static final AsyncTcpClientGroup group = new AsyncTcpClientGroup(4);
	public static final AsyncFileReaderGroup group2 = new AsyncFileReaderGroup(4);
	
	public static TcpDiskNodeClient getClient(String host,int port, int export, int timeout) throws InterruptedException, IOException {		
		TcpClient<BaseMessage, BaseResponse> tcpClient = group.createClient(new TcpClientConfig() {
			@Override
			public SocketAddress remoteAddress() {
				return new InetSocketAddress(host, port);
			}
			
			@Override
			public int connectTimeoutMillis() {
				return timeout;
			}
		});
	
		
		TcpClient<ReadObject, FileContentPart> readerClient = group2.createClient(new AsyncFileReaderCreateConfig() {
			@Override
			public SocketAddress remoteAddress() {
				return new InetSocketAddress(host, export);
			}
			@Override
			public int connectTimeoutMillis() {
				return timeout;
			}
			@Override
			public int maxPendingRead() {
				return 0;
			}
		});
		return new TcpDiskNodeClient(tcpClient, readerClient);
	}
}
