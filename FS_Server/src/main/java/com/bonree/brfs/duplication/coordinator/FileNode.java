package com.bonree.brfs.duplication.coordinator;

public class FileNode {
	private String name;
	private String storageName;
	private long createTime;
	private String serviceId;
	private int[] duplicates;
	
	public FileNode() {
		this.createTime = System.currentTimeMillis();
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

	public int[] getDuplicates() {
		return duplicates;
	}

	public void setDuplicates(int[] duplicates) {
		this.duplicates = duplicates;
	}

}
