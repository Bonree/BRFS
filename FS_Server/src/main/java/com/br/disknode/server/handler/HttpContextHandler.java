package com.br.disknode.server.handler;

public interface HttpContextHandler {
	String getContextPath();
	void addHttpRequestHandler(HttpRequestHandler<?> requestHandler);
}
