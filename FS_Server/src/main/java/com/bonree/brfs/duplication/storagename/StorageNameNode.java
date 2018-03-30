package com.bonree.brfs.duplication.storagename;


public class StorageNameNode {
	private String name;
	private int id;
	private int replicateCount;
	private int ttl;
	
	public StorageNameNode() {
	}
	
	public StorageNameNode(String name, int id, int replis, int ttl) {
		this.name = name;
		this.id = id;
		this.replicateCount = replis;
		this.ttl = ttl;
	}

	public String getName() {
		return name;
	}

	public int getId() {
		return id;
	}

	public int getReplicateCount() {
		return replicateCount;
	}

	public int getTtl() {
		return ttl;
	}

	public void setTtl(int ttl) {
		this.ttl = ttl;
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
		       .append(ttl).append("]");
		
		return builder.toString();
	}
}
