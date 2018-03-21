package com.br.duplication.coordinator;

public class FileNode {
	private String name;
	private int storageId;
	private FileNodeData nodeData;
	
	public FileNode(String name, int storageId, FileNodeData data) {
		this.name = name;
		this.storageId = storageId;
		this.nodeData = data;
	}

	public String getName() {
		return name;
	}
	
	public int getStorageId() {
		return storageId;
	}

	public FileNodeData getFileNodeData() {
		return nodeData;
	}

}
