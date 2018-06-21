package com.bonree.brfs.common.net.http;

public class HttpConfig {
	private String host;
	private int port;
	
	private static final int DEFAULT_BACKLOG = 128;
	private int backlog;
	
	private static final int DEFAULT_CONNECT_TIMEOUT_MILLIES = 30000;
	private int connectTimeoutMillies;
	
	private boolean isKeepAlive;
	private boolean tcpNoDelay;
	
	private static final int DEFAULT_ACCEPT_WORKER_NUM = 2;
	private int acceptWorkerNum;
	
	private static final int DEFAULT_REQUEST_HANDLE_WORKER_NUM = 6;
	private int requestHandleWorkerNum;
	
	private static final int DEFAULT_MAX_HTTP_CONTENT_LENGTH = 65 * 1024 * 1024;
	private int maxHttpContentLength;
	
	public HttpConfig(int port) {
		this(null, port);
	}
	
	public HttpConfig(String host, int port) {
		this.host = host;
		this.port = port;
		this.backlog = DEFAULT_BACKLOG;
		this.connectTimeoutMillies = DEFAULT_CONNECT_TIMEOUT_MILLIES;
		this.isKeepAlive = false;
		this.tcpNoDelay = true;
		this.acceptWorkerNum = DEFAULT_ACCEPT_WORKER_NUM;
		this.requestHandleWorkerNum = DEFAULT_REQUEST_HANDLE_WORKER_NUM;
		this.maxHttpContentLength = DEFAULT_MAX_HTTP_CONTENT_LENGTH;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public int getBacklog() {
		return backlog;
	}

	public void setBacklog(int backlog) {
		this.backlog = backlog;
	}

	public int getConnectTimeoutMillies() {
		return connectTimeoutMillies;
	}

	public void setConnectTimeoutMillies(int connectTimeoutMillies) {
		this.connectTimeoutMillies = connectTimeoutMillies;
	}

	public boolean isKeepAlive() {
		return isKeepAlive;
	}

	public void setKeepAlive(boolean isKeepAlive) {
		this.isKeepAlive = isKeepAlive;
	}
	
	public boolean isTcpNoDelay() {
		return tcpNoDelay;
	}
	
	public void setTcpNoDelay(boolean tcpNoDelay) {
		this.tcpNoDelay = tcpNoDelay;
	}

	public int getAcceptWorkerNum() {
		return acceptWorkerNum;
	}

	public void setAcceptWorkerNum(int acceptWorkerNum) {
		this.acceptWorkerNum = acceptWorkerNum;
	}

	public int getRequestHandleWorkerNum() {
		return requestHandleWorkerNum;
	}

	public void setRequestHandleWorkerNum(int requestHandleWorkerNum) {
		this.requestHandleWorkerNum = requestHandleWorkerNum;
	}

	public int getMaxHttpContentLength() {
		return maxHttpContentLength;
	}

	public void setMaxHttpContentLength(int maxHttpContentLength) {
		this.maxHttpContentLength = maxHttpContentLength;
	}

}
