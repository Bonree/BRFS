package com.bonree.brfs.duplication.datastream.file;

import com.bonree.brfs.duplication.coordinator.FileNode;

public class FileInfo {
	//默认的文件大小上限
	public static final int DEFAULT_FILE_CAPACITY = 64 * 1024 * 1024;
	
	private FileNode fileNode;
	private int size;
	private final int capacity;
	
	public FileInfo() {
		this(DEFAULT_FILE_CAPACITY);
	}
	
	public FileInfo(int capacity) {
		this.capacity = capacity;
	}
	
	public int capacity() {
		return capacity;
	}

	public FileNode getFileNode() {
		return fileNode;
	}

	public void setFileNode(FileNode fileNode) {
		this.fileNode = fileNode;
	}

	public int size() {
		return size;
	}
	
	public void increment(int length) {
		size += length;
	}
	
	public int remaining() {
		return capacity - size;
	}
}
