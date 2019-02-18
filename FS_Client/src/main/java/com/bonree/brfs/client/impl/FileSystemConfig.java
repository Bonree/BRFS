package com.bonree.brfs.client.impl;

import org.apache.curator.shaded.com.google.common.base.Preconditions;

public class FileSystemConfig {
	private String name;
	private String passwd;
	
	private String zkAddresses;
	private String clusterName;
	
	private static final String DEFAULT_URL_SCHEMA = "http";
	private String urlSchema;
	
	private static final String DEFAULT_STORAGE_URL_ROOT = "/sr";
	private String storageUrlRoot;
	
	private static final String DEFAULT_DUPLICATE_URL_ROOT = "/data";
	private String duplicateUrlRoot;
	
	private static final String DEFAULT_DISK_URL_ROOT = "/disk";
	private String diskUrlRoot;
	
	private static final int DEFAULT_CONNECTION_POOL_SIZE = 16;
	private int connectionPoolSize;
	
	private static final int DEFAULT_CONNECTION_HANDLE_THREAD_NUM = 4;
	private int handleThreadNum;
	
	private static final String DEFAULT_DUPLICATE_SERVICE_GROUP = "region_group";
	private String duplicateServiceGroup;
	private static final String DEFAULT_DISK_SERVICE_GROUP = "data_group";
	private String diskServiceGroup;
	
	private static final int DEFAULT_ZK_CONNECT_TIMEOUT_SECONDS = 15;
	private int zkConnectTimeoutSeconds;
	
	private FileSystemConfig() {
		this.urlSchema = DEFAULT_URL_SCHEMA;
		this.storageUrlRoot = DEFAULT_STORAGE_URL_ROOT;
		this.duplicateUrlRoot = DEFAULT_DUPLICATE_URL_ROOT;
		this.diskUrlRoot = DEFAULT_DISK_URL_ROOT;
		this.connectionPoolSize = DEFAULT_CONNECTION_POOL_SIZE;
		this.handleThreadNum = DEFAULT_CONNECTION_HANDLE_THREAD_NUM;
		this.duplicateServiceGroup = DEFAULT_DUPLICATE_SERVICE_GROUP;
		this.diskServiceGroup = DEFAULT_DISK_SERVICE_GROUP;
		this.zkConnectTimeoutSeconds = DEFAULT_ZK_CONNECT_TIMEOUT_SECONDS;
	}
	
	public String getName() {
		return name;
	}

	public String getPasswd() {
		return passwd;
	}

	public String getZkAddresses() {
		return zkAddresses;
	}

	public String getClusterName() {
		return clusterName;
	}
	
	public String getUrlSchema() {
		return urlSchema;
	}
	
	public String getStorageUrlRoot() {
		return storageUrlRoot;
	}

	public String getDuplicateUrlRoot() {
		return duplicateUrlRoot;
	}

	public String getDiskUrlRoot() {
		return diskUrlRoot;
	}

	public int getConnectionPoolSize() {
		return connectionPoolSize;
	}
	
	public int getHandleThreadNum() {
		return handleThreadNum;
	}
	
	public String getDuplicateServiceGroup() {
		return duplicateServiceGroup;
	}
	
	public String getDiskServiceGroup() {
		return diskServiceGroup;
	}
	
	public int getZkConnectTimeoutSeconds() {
		return zkConnectTimeoutSeconds;
	}
	
	public static Builder newBuilder() {
		return new Builder();
	}
	
	public static class Builder {
		private FileSystemConfig config;
		
		private Builder() {
			this.config = new FileSystemConfig();
		}
		
		public Builder setUsername(String userName) {
			config.name = userName;
			return this;
		}
		
		public Builder setPasswd(String passwd) {
			config.passwd = passwd;
			return this;
		}
		
		public Builder setZkAddresses(String addresses) {
			config.zkAddresses = addresses;
			return this;
		}
		
		public Builder setClusterName(String clusterName) {
			config.clusterName = clusterName;
			return this;
		}
		
		public Builder setUrlSchema(String urlSchema) {
			config.urlSchema = urlSchema;
			return this;
		}
		
		public Builder setStorageUrlRoot(String urlRoot) {
			config.storageUrlRoot = urlRoot;
			return this;
		}
		
		public Builder setDuplicateUrlRoot(String urlRoot) {
			config.duplicateUrlRoot = urlRoot;
			return this;
		}
		
		public Builder setDiskUrlRoot(String urlRoot) {
			config.diskUrlRoot = urlRoot;
			return this;
		}
		
		public Builder setConnectionPoolSize(int size) {
			config.connectionPoolSize = size;
			return this;
		}
		
		public Builder setHandleThreadNum(int num) {
			config.handleThreadNum = num;
			return this;
		}
		
		public Builder setDuplicateServiceGroup(String group) {
			config.duplicateServiceGroup = group;
			return this;
		}
		
		public Builder setDiskServiceGroup(String group) {
			config.diskServiceGroup = group;
			return this;
		}
		
		public Builder setZkConnectTimeoutSeconds(int timeoutSeconds) {
			config.zkConnectTimeoutSeconds = timeoutSeconds;
			return this;
		}
		
		public FileSystemConfig build() {
			Preconditions.checkNotNull(config.name);
			Preconditions.checkNotNull(config.passwd);
			Preconditions.checkNotNull(config.zkAddresses);
			Preconditions.checkNotNull(config.clusterName);
			
			return config;
		}
	}
}
