package com.br.disknode.server.handler;

public interface HttpRequestHandler<T> {
	void addMessageHandler(String method, MessageHandler messageHandler);
	void requestReceived(T request);
}
