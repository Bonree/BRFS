package com.bonree.brfs.duplication.storageregion;

import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.StorageConfigs;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;


public class StorageRegion {
	public static final int DEFAULT_REPLIS = Configs.getConfiguration().GetConfig(StorageConfigs.CONFIG_STORAGE_REGION_REPLICATE_COUNT);
	public static final String DEFAULT_TTL = Configs.getConfiguration().GetConfig(StorageConfigs.CONFIG_STORAGE_REGION_DATA_TTL);
	public static final long DEFAULT_FILE_CAPACITY = Configs.getConfiguration().GetConfig(StorageConfigs.CONFIG_STORAGE_REGION_FILE_CAPACITY);
	public static final String DEFAULT_FILE_PATITION_DURATION = Configs.getConfiguration().GetConfig(StorageConfigs.CONFIG_FILE_PATITION_DURATION);
	
	//Storage Region的自身属性
	@JsonProperty("name")
	private String name;
	@JsonProperty("id")
	private int id;
	@JsonProperty("create_time")
	private long createTime;
	@JsonProperty("enable")
	private boolean enable;
	
	//Storage Region存储的数据相关属性
	@JsonProperty("replicate_num")
	private int replicateNum;
	@JsonProperty("data_ttl")
	private String dataTtl;
	@JsonProperty("file_capacity")
	private long fileCapacity;
	@JsonProperty("patition_duration")
	private String filePartitionDuration;
	
	private StorageRegion() {
		this.createTime = System.currentTimeMillis();
		this.enable = true;
		this.replicateNum = DEFAULT_REPLIS;
		this.dataTtl = DEFAULT_TTL;
		this.fileCapacity = DEFAULT_FILE_CAPACITY;
		this.filePartitionDuration = DEFAULT_FILE_PATITION_DURATION;
	}
	
	public String getName() {
		return name;
	}

	public int getId() {
		return id;
	}

	public long getCreateTime() {
		return createTime;
	}

	public boolean isEnable() {
		return enable;
	}

	public int getReplicateNum() {
		return replicateNum;
	}

	public String getDataTtl() {
		return dataTtl;
	}

	public long getFileCapacity() {
		return fileCapacity;
	}

	public String getFilePartitionDuration() {
		return filePartitionDuration;
	}
	
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append('{')
		.append("name=").append(name).append(',')
		.append("id=").append(id).append(',')
		.append("createTime=").append(createTime).append(',')
		.append("enable=").append(enable).append(',')
		.append("replicates=").append(replicateNum).append(',')
		.append("ttl=").append(dataTtl).append(',')
		.append("capacity=").append(fileCapacity).append(',')
		.append("duration=").append(filePartitionDuration)
		.append('}');
		
		return builder.toString();
	}
	
	public static Builder newBuilder() {
		return new Builder();
	}
	
	public static Builder newBuilder(StorageRegion region) {
		return new Builder(region);
	}
	
	public static class Builder {
		private StorageRegion region;
		
		private Builder() {
			this.region = new StorageRegion();
		}
		
		private Builder(StorageRegion region) {
			this.region = new StorageRegion();
			this.region.name = region.name;
			this.region.id = region.id;
			this.region.createTime = region.createTime;
			this.region.enable = region.enable;
			this.region.replicateNum = region.replicateNum;
			this.region.dataTtl = region.dataTtl;
			this.region.fileCapacity = region.fileCapacity;
			this.region.filePartitionDuration = region.filePartitionDuration;
		}
		
		public Builder setName(String name) {
			this.region.name = name;
			return this;
		}
		
		public Builder setId(int id) {
			this.region.id = id;
			return this;
		}
		
		public Builder setCreateTime(long time) {
			this.region.createTime = time;
			return this;
		}
		
		public Builder setEnable(boolean enable) {
			this.region.enable = enable;
			return this;
		}
		
		public Builder setReplicateNum(int num) {
			this.region.replicateNum = num;
			return this;
		}
		
		public Builder setDataTtl(String ttl) {
			this.region.dataTtl = ttl;
			return this;
		}
		
		public Builder setFileCapacity(long capacity) {
			this.region.fileCapacity = capacity;
			return this;
		}
		
		public Builder setFilePartitionDuration(String duration) {
			this.region.filePartitionDuration = duration;
			return this;
		}
		
		public StorageRegion build() {
			Preconditions.checkNotNull(this.region.name);
			Preconditions.checkArgument(this.region.id >= 0);
			return this.region;
		}
	}
}
