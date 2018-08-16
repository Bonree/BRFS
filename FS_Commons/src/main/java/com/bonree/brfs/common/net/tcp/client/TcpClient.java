package com.bonree.brfs.common.net.tcp.client;

import java.io.Closeable;

public interface TcpClient<S, R> extends Closeable {
	String remoteHost();
	int remotePort();
	void sendMessage(S msg, ResponseHandler<R> handler) throws Exception;
	
	void setClientCloseListener(TcpClientCloseListener listener);
}
