package com.bonree.brfs.duplication.filenode;

import com.bonree.brfs.duplication.filenode.duplicates.DuplicateNode;

public class FileNode {
	//文件名
	private String name;
	//所属的Storage ID
	private int storageId;
	//storageName
	private String storageName;
	//文件节点创建时间
	private long createTime;
	//所在的时间间隔长度
	private String timeDuration;
	//文件转移时值会更新
	private long serviceTime;
	//所属服务组
	private String serviceGroup;
	//所属的服务ID
	private String serviceId;
	//副本所在节点
	private DuplicateNode[] duplicateNodes;
	//文件的容量大小
	private long capacity;
	
	public FileNode() {
		this(System.currentTimeMillis());
	}
	
	public FileNode(long createTime) {
		this.createTime = createTime;
		this.serviceTime = createTime;
	}
	
	public FileNode(FileNode other) {
		this.name = other.name;
		this.storageId = other.storageId;
		this.storageName = other.storageName;
		this.createTime = other.createTime;
		this.timeDuration = other.timeDuration;
		this.serviceTime = other.serviceTime;
		this.serviceGroup = other.serviceGroup;
		this.serviceId = other.serviceId;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getStorageName() {
		return storageName;
	}

	public void setStorageName(String storageName) {
		this.storageName = storageName;
	}

	public long getCreateTime() {
		return createTime;
	}

	public void setCreateTime(long createTime) {
		this.createTime = createTime;
	}

	public String getServiceId() {
		return serviceId;
	}

	public void setServiceId(String serviceId) {
		this.serviceId = serviceId;
	}
	
	public int getStorageId() {
		return storageId;
	}

	public void setStorageId(int storageId) {
		this.storageId = storageId;
	}

	public DuplicateNode[] getDuplicateNodes() {
		return duplicateNodes;
	}

	public void setDuplicateNodes(DuplicateNode[] duplicates) {
		this.duplicateNodes = duplicates;
	}

	public String getServiceGroup() {
		return serviceGroup;
	}

	public void setServiceGroup(String serviceGroup) {
		this.serviceGroup = serviceGroup;
	}

	public long getServiceTime() {
		return serviceTime;
	}

	public void setServiceTime(long serviceTime) {
		this.serviceTime = serviceTime;
	}

	public long getCapacity() {
		return capacity;
	}

	public void setCapacity(long capacity) {
		this.capacity = capacity;
	}

	public String getTimeDuration() {
		return timeDuration;
	}

	public void setTimeDuration(String timeDuration) {
		this.timeDuration = timeDuration;
	}

}
