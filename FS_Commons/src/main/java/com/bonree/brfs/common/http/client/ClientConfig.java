package com.bonree.brfs.common.http.client;

public class ClientConfig {
	private static final int DEFAULT_MAX_CONNECTION = 4;
	private int maxConnection;
	
	private static final int DEFAULT_BUFFER_SIZE = 8 * 1024;
	private int bufferSize;
	
	private int idleTimeout;
	
	private static final int DEFAULT_SEND_BUFFER_SIZE = 64 * 1024;
	private int socketSendBufferSize;
	private static final int DEFAULT_RECV_BUFFER_SIZE = 512 * 1024;
	private int socketRecvBufferSize;
	private static final int DEFAULT_SOCKET_TIMEOUT = 10 * 1000;
	private int socketTimeout;
	
	public static ClientConfig DEFAULT = new ClientConfig();
	
	private ClientConfig() {
		this.maxConnection = DEFAULT_MAX_CONNECTION;
		this.bufferSize = DEFAULT_BUFFER_SIZE;
		this.idleTimeout = 0;
		this.socketSendBufferSize = DEFAULT_SEND_BUFFER_SIZE;
		this.socketRecvBufferSize = DEFAULT_RECV_BUFFER_SIZE;
		this.socketTimeout = DEFAULT_SOCKET_TIMEOUT;
	}

	public int getMaxConnection() {
		return maxConnection;
	}

	public int getBufferSize() {
		return bufferSize;
	}
	
	public int getIdleTimeout() {
		return idleTimeout;
	}
	
	public int getSocketSendBufferSize() {
		return socketSendBufferSize;
	}
	
	public int getSocketRecvBufferSize() {
		return socketRecvBufferSize;
	}
	
	public int getSocketTimeout() {
		return socketTimeout;
	}
	
	public static Builder builder() {
		return new Builder();
	}
	
	public static class Builder {
		private ClientConfig config = new ClientConfig();
		
		public Builder setMaxConnection(int maxConnection) {
			config.maxConnection = maxConnection > 0 ? maxConnection : DEFAULT_MAX_CONNECTION;
			return this;
		}
		
		public Builder setBufferSize(int bufferSize) {
			config.bufferSize = bufferSize > 0 ? bufferSize : DEFAULT_BUFFER_SIZE;
			return this;
		}
		
		public Builder setIdleTimeout(int idleTimeout) {
			config.idleTimeout = idleTimeout > 0 ? idleTimeout : 0;
			return this;
		}
		
		public Builder setSocketSendBufferSize(int sendBufferSize) {
			config.socketSendBufferSize = sendBufferSize > 0 ? sendBufferSize : DEFAULT_SEND_BUFFER_SIZE;
			return this;
		}
		
		public Builder setSocketRecvBufferSize(int recvBufferSize) {
			config.socketRecvBufferSize = recvBufferSize > 0 ? recvBufferSize : DEFAULT_RECV_BUFFER_SIZE;
			return this;
		}
		
		public Builder setSocketTimeout(int socketTimeout) {
			config.socketTimeout = socketTimeout > 0 ? socketTimeout : DEFAULT_SOCKET_TIMEOUT;
			return this;
		}
		
		public ClientConfig build() {
			return config;
		}
	}
}
