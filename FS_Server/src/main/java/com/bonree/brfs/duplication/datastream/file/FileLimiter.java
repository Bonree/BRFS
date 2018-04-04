package com.bonree.brfs.duplication.datastream.file;

import java.util.concurrent.atomic.AtomicInteger;

import com.bonree.brfs.duplication.coordinator.FileNode;

public class FileLimiter {
	//默认的文件大小上限
	public static final int DEFAULT_FILE_CAPACITY = 64 * 1024 * 1024;
	
	private FileNode fileNode;
	private AtomicInteger contentLength;
	private final int capacity;
	
	public FileLimiter() {
		this(DEFAULT_FILE_CAPACITY);
	}
	
	public FileLimiter(int capacity) {
		this.capacity = capacity;
		this.contentLength = new AtomicInteger(0);
	}
	
	public int size() {
		return contentLength.get();
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

	public boolean obtain(int size) {
		while(true) {
			int current = contentLength.get();
			if(current + size > capacity) {
				return false;
			}
			
			if(contentLength.compareAndSet(current, current + size)) {
				return true;
			}
		}
	}
}
