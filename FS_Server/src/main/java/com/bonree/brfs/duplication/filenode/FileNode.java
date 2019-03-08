package com.bonree.brfs.duplication.filenode;

import com.bonree.brfs.duplication.filenode.duplicates.DuplicateNode;
import com.fasterxml.jackson.annotation.JsonProperty;

public class FileNode {
	//文件名
	@JsonProperty("name")
	private String name;
	//所属的Storage ID
	@JsonProperty("id")
	private int storageId;
	//storageName
	@JsonProperty("storage_name")
	private String storageName;
	//文件节点创建时间
	@JsonProperty("create_time")
	private long createTime;
	//所在的时间间隔长度
	@JsonProperty("time_duration")
	private long timeDurationMillis;
	//文件转移时值会更新
	@JsonProperty("service_time")
	private long serviceTime;
	//所属服务组
	@JsonProperty("service_group")
	private String serviceGroup;
	//所属的服务ID
	@JsonProperty("service_id")
	private String serviceId;
	//副本所在节点
	@JsonProperty("duplicate_nodes")
	private DuplicateNode[] duplicateNodes;
	//文件的容量大小
	@JsonProperty("capacity")
	private long capacity;
	
	private FileNode() {
		this.createTime = System.currentTimeMillis();
		this.serviceTime = createTime;
	}

	public String getName() {
		return name;
	}

	public int getStorageId() {
		return storageId;
	}

	public String getStorageName() {
		return storageName;
	}

	public long getCreateTime() {
		return createTime;
	}
	public long getTimeDurationMillis() {
		return timeDurationMillis;
	}

	public long getServiceTime() {
		return serviceTime;
	}

	public String getServiceGroup() {
		return serviceGroup;
	}

	public String getServiceId() {
		return serviceId;
	}

	public DuplicateNode[] getDuplicateNodes() {
		return duplicateNodes;
	}

	public long getCapacity() {
		return capacity;
	}
	
	public static Builder newBuilder() {
		return new Builder();
	}
	
	public static Builder newBuilder(FileNode other) {
		return new Builder(other);
	}

	public static class Builder {
		private FileNode node;
		
		private Builder() {
			this.node = new FileNode();
		}
		
		private Builder(FileNode other) {
			this.node = new FileNode();
			node.name = other.name;
			node.storageId = other.storageId;
			node.storageName = other.storageName;
			node.createTime = other.createTime;
			node.timeDurationMillis = other.timeDurationMillis;
			node.serviceTime = other.serviceTime;
			node.serviceGroup = other.serviceGroup;
			node.serviceId = other.serviceId;
			node.duplicateNodes = other.duplicateNodes;
			node.capacity = other.capacity;
		}
		
		public Builder setName(String name) {
			node.name = name;
			return this;
		}

		public Builder setStorageId(int storageId) {
			node.storageId = storageId;
			return this;
		}

		public Builder setStorageName(String storageName) {
			node.storageName = storageName;
			return this;
		}

		public Builder setCreateTime(long createTime) {
			node.createTime = createTime;
			return this;
		}

		public Builder setTimeDuration(long timeDuration) {
			node.timeDurationMillis = timeDuration;
			return this;
		}

		public Builder setServiceTime(long serviceTime) {
			node.serviceTime = serviceTime;
			return this;
		}

		public Builder setServiceGroup(String serviceGroup) {
			node.serviceGroup = serviceGroup;
			return this;
		}

		public Builder setServiceId(String serviceId) {
			node.serviceId = serviceId;
			return this;
		}

		public Builder setDuplicateNodes(DuplicateNode[] duplicateNodes) {
			node.duplicateNodes = duplicateNodes;
			return this;
		}

		public Builder setCapacity(long capacity) {
			node.capacity = capacity;
			return this;
		}
		
		public FileNode build() {
			return node;
		}
	}
}
