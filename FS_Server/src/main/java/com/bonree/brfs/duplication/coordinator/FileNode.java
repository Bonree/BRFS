package com.bonree.brfs.duplication.coordinator;

public class FileNode {
	private String name;
	private String storageName;
	private long createTime;
	private String serviceId;
	private DuplicateNode[] duplicateNodes;
	
	public FileNode() {
		this(System.currentTimeMillis());
	}
	
	public FileNode(long createTime) {
		this.createTime = createTime;
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

	public DuplicateNode[] getDuplicateNodes() {
		return duplicateNodes;
	}

	public void setDuplicateNodes(DuplicateNode[] duplicates) {
		this.duplicateNodes = duplicates;
	}

}
