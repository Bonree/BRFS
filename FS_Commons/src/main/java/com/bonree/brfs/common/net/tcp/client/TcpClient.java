package com.bonree.brfs.common.net.tcp.client;

import java.io.Closeable;

public interface TcpClient<T, V> extends Closeable {
	String remoteHost();
	int remotePort();
	void sendMessage(T msg, ResponseHandler<V> handler) throws Exception;
	
	void setClientCloseListener(TcpClientCloseListener listener);
}
