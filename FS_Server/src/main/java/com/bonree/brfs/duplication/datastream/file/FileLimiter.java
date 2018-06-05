package com.bonree.brfs.duplication.datastream.file;

import java.util.concurrent.atomic.AtomicReference;

import com.bonree.brfs.duplication.coordinator.FileNode;

/**
 * 一个文件节点的包装类，增加了对文件的一些操作限制
 * 
 * 
 * @author yupeng
 *
 */
public class FileLimiter {
	private FileNode fileNode;
	private final long capacity;
	private volatile long logicLength;
	private volatile long realLength;
	private volatile int sequence;
	
	private AtomicReference<Object> lockObject = new AtomicReference<Object>();
	private Object attached;
	
	public FileLimiter(FileNode fileNode, int capacity) {
		this(fileNode, capacity, 0, 0);
	}
	
	public FileLimiter(FileNode fileNode, int capacity, int length, int dataSequence) {
		this.fileNode = fileNode;
		this.logicLength = length;
		this.realLength = length;
		this.sequence = dataSequence;
		this.capacity = capacity;
	}
	
	public FileNode getFileNode() {
		return fileNode;
	}
	
	public long capacity() {
		return capacity;
	}
	
	public void incrementSequenceBy(int incs) {
		sequence += incs;
	}
	
	public int getSequence() {
		return sequence;
	}
	
	public void setLength(long length) {
		this.realLength = length;
	}
	
	public long getLength() {
		return realLength;
	}
	
	public boolean lock(Object lock) {
		if(lockObject.get() == lock) {
			return true;
		}
		
		if(lockObject.compareAndSet(null, lock)) {
			logicLength = realLength;
			return true;
		}
		
		return false;
	}
	
	public void unlock() {
		lockObject.set(null);
	}
	
	public void attach(Object obj) {
		this.attached = obj;
	}
	
	public Object attach() {
		return attached;
	}
	
	/**
	 * <p>向文件申请存储空间</p>
	 * 
	 * @param size 申请的空间大小
	 * @return 申请成功返回true;否则返回false
	 */
	public boolean apply(long size) {
		if(size > capacity - logicLength) {
			return false;
		}
		
		logicLength += size;
		return true;
	}
}
