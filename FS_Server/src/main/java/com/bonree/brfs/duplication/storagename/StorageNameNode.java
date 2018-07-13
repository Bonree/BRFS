package com.bonree.brfs.duplication.storagename;


public class StorageNameNode {
	/**
	 * StorageName的属性名
	 */
	//副本数属性名
	public static final String ATTR_REPLICATION = "replication";
	//数据有效期属性名
	public static final String ATTR_TTL = "ttl";
	public static final String ATTR_FILE_CAPACITY = "fileCapacity";
	public static final String ATTR_FILE_PATITION_DURATION = "filePatitionDuration";
	public static final String ATTR_ENABLE = "enable";
	
	private String name;
	private int id;
	private int replicateCount;
	private int ttl;
	private long createTime;
	
	private long fileCapacity;
	private String partitionDuration;
	
	private boolean enable = true;
	
	public StorageNameNode() {
	}
	
	public StorageNameNode(String name, int id, int replis, int ttl, long fileCapacity, String partitionDuration) {
		this.name = name;
		this.id = id;
		this.replicateCount = replis;
		this.ttl = ttl;
		this.fileCapacity = fileCapacity;
		this.partitionDuration = partitionDuration;
	}

	public String getName() {
		return name;
	}
	
	public void setName(String name) {
		this.name = name;
	}

	public int getId() {
		return id;
	}
	
	public void setId(int id) {
		this.id = id;
	}

	public int getReplicateCount() {
		return replicateCount;
	}
	
	public void setReplicateCount(int replis) {
		this.replicateCount = replis;
	}

	public int getTtl() {
		return ttl;
	}

	public void setTtl(int ttl) {
		this.ttl = ttl;
	}
	
	public long getCreateTime() {
		return createTime;
	}

	public void setCreateTime(long createTime) {
		this.createTime = createTime;
	}
	
	public long getFileCapacity() {
		return fileCapacity;
	}

	public void setFileCapacity(long fileCapacity) {
		this.fileCapacity = fileCapacity;
	}
	
	public boolean isEnable() {
        return enable;
    }

    public void setEnable(boolean enable) {
        this.enable = enable;
    }

	@Override
	public boolean equals(Object obj) {
		if(this == obj) {
			return true;
		}
		
		if(obj instanceof StorageNameNode) {
			StorageNameNode oNode = (StorageNameNode) obj;
			
			if(this.name.equals(oNode.name)
					&& this.id == oNode.id) {
				return true;
			}
		}
		
		return false;
	}
	
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("StorageName[")
		       .append(name).append(",")
		       .append(id).append(",")
		       .append(replicateCount).append(",")
		       .append(ttl).append(",")
		       .append(enable).append("]");
		
		return builder.toString();
	}

	public String getPartitionDuration() {
		return partitionDuration;
	}

	public void setPartitionDuration(String partitionDuration) {
		this.partitionDuration = partitionDuration;
	}
}
