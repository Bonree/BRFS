package com.bonree.brfs.common.net.tcp.client;

public interface TcpResponseHandler {
	void received(byte[] bytes, boolean finished);
	void fail();
}
