package com.bonree.brfs.duplication.coordinator;

public class FileNode {
	//文件名
	private String name;
	//所属的Storage ID
	private int storageId;
	//storageName
	private String storageName;
	//文件节点创建时间
	private long createTime;
	//文件转移时值会更新
	private long serviceTime;
	//所属服务组
	private String serviceGroup;
	//所属的服务ID
	private String serviceId;
	//副本所在节点
	private DuplicateNode[] duplicateNodes;
	
	public FileNode() {
		this(System.currentTimeMillis());
	}
	
	public FileNode(long createTime) {
		this.createTime = createTime;
		this.serviceTime = createTime;
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

}
