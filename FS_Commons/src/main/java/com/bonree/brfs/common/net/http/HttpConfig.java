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
	
	private HttpConfig() {
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

	public int getPort() {
		return port;
	}

	public int getBacklog() {
		return backlog;
	}

	public int getConnectTimeoutMillies() {
		return connectTimeoutMillies;
	}

	public boolean isKeepAlive() {
		return isKeepAlive;
	}
	
	public boolean isTcpNoDelay() {
		return tcpNoDelay;
	}

	public int getAcceptWorkerNum() {
		return acceptWorkerNum;
	}

	public int getRequestHandleWorkerNum() {
		return requestHandleWorkerNum;
	}

	public int getMaxHttpContentLength() {
		return maxHttpContentLength;
	}
	
	public static Builder newBuilder() {
		return new Builder();
	}
	
	public static class Builder {
		private HttpConfig config;
		
		private Builder() {
			this.config = new HttpConfig();
		}

		public Builder setHost(String host) {
			config.host = host;
			return this;
		}

		public Builder setPort(int port) {
			config.port = port;
			return this;
		}

		public Builder setBacklog(int backlog) {
			config.backlog = backlog;
			return this;
		}

		public Builder setConnectTimeoutMillies(int connectTimeoutMillies) {
			config.connectTimeoutMillies = connectTimeoutMillies;
			return this;
		}

		public Builder setKeepAlive(boolean isKeepAlive) {
			config.isKeepAlive = isKeepAlive;
			return this;
		}
		
		public Builder setTcpNoDelay(boolean tcpNoDelay) {
			config.tcpNoDelay = tcpNoDelay;
			return this;
		}

		public Builder setAcceptWorkerNum(int acceptWorkerNum) {
			config.acceptWorkerNum = acceptWorkerNum;
			return this;
		}

		public Builder setRequestHandleWorkerNum(int requestHandleWorkerNum) {
			config.requestHandleWorkerNum = requestHandleWorkerNum;
			return this;
		}

		public Builder setMaxHttpContentLength(int maxHttpContentLength) {
			config.maxHttpContentLength = maxHttpContentLength;
			return this;
		}
		
		public HttpConfig build() {
			return config;
		}
	}

}
