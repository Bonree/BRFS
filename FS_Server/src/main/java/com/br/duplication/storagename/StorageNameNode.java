package com.br.duplication.storagename;


public class StorageNameNode {
	private String name;
	private int id;
	private int replicates;
	private int ttl;
	
	public StorageNameNode(String name, int id, int replis, int ttl) {
		this.name = name;
		this.id = id;
		this.replicates = replis;
		this.ttl = ttl;
	}

	public String getName() {
		return name;
	}

	public int getId() {
		return id;
	}

	public int getReplicates() {
		return replicates;
	}

	public int getTtl() {
		return ttl;
	}

	public void setTtl(int ttl) {
		this.ttl = ttl;
	}
	
	public byte[] toBytes() {
		//TODO by another way
		StringBuilder builder = new StringBuilder();
		builder.append(name).append(",").append(id).append(",").append(replicates).append(",").append(ttl);
		return builder.toString().getBytes();
	}
	
	public static StorageNameNode fromBytes(byte[] bytes) {
		//TODO by another way
		String[] parts = new String(bytes).split(",");
		return new StorageNameNode(parts[0], Integer.parseInt(parts[1]), Integer.parseInt(parts[2]), Integer.parseInt(parts[3]));
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
		       .append(replicates).append(",")
		       .append(ttl).append("]");
		
		return builder.toString();
	}
}
