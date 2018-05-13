package com.bonree.brfs.duplication.datastream.file;

import java.util.concurrent.atomic.AtomicInteger;

import com.bonree.brfs.duplication.coordinator.FileNode;

/**
 * 一个文件节点的包装类，增加了对文件的一些操作限制
 * 
 * *****这个类是线程安全的*****
 * 
 * @author chen
 *
 */
public class FileLimiter {
	//默认的文件大小上限
	public static final int DEFAULT_FILE_CAPACITY = 64 * 1024 * 1024;
	
	private FileNode fileNode;
	private AtomicInteger contentLength;
	private final int capacity;
	
	private AtomicInteger sequence = new AtomicInteger(0);
	
	private byte[] header;
	private byte[] tailer;
	
	/**
	 * 用于跟踪对此文件的还未完成的写入操作的个数，清理文件时
	 * 不能清理还在写入的文件
	 * 
	 * 如果值为-1，则表示文件已经设置清理标记，不能再对其进行写入
	 */
	private AtomicInteger writing = new AtomicInteger(0);
	
	public FileLimiter(FileNode fileNode) {
		this(fileNode, 0, 0, DEFAULT_FILE_CAPACITY);
	}
	
	public FileLimiter(FileNode fileNode, int size, int sequence) {
		this(fileNode, size, sequence, DEFAULT_FILE_CAPACITY);
	}
	
	public FileLimiter(FileNode fileNode, int size, int sequence, int capacity) {
		this.fileNode = fileNode;
		this.capacity = capacity;
		this.contentLength = new AtomicInteger(size);
		this.sequence = new AtomicInteger(sequence);
	}
	
	public int sequence() {
		return sequence.get();
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
	
	/**
	 * 释放没有使用的空间大小
	 * 
	 * @param size 没有使用的空间大小
	 */
	public void release(int size) {
		if(writing.get() <= 0) {
			return;
		}
		
		if(size != 0) {
			while(true) {
				int length = contentLength.get();
				if(contentLength.compareAndSet(length, length - size)) {
					break;
				}
			}
		}
		
		writing.decrementAndGet();
	}
	
	/**
	 * 文件只在没有写入操作的时候才能对其设置清理标志
	 * 
	 * @return 是否成功设置清理标记
	 */
	public boolean clean() {
		return isCleaned() || writing.compareAndSet(0, -1);
	}
	
	/**
	 * 文件空闲时，大小超过一定比率则标记清理
	 * 
	 * @param sizeRatio
	 * @return
	 */
	public boolean cleanIfOverSize(double sizeRatio) {
		if(writing.compareAndSet(0, -2)) {
			if(Double.compare(contentLength.get(), capacity * sizeRatio) > 0) {
				writing.set(-1);
				return true;
			}
			
			writing.set(0);
		}
		
		return false;
	}
	
	public boolean isCleaned() {
		return writing.get() == -1;
	}

	/**
	 * 线程安全的方式申请文件空间
	 * 
	 * ***
	 * 这里只是申请空间，申请到的空间不一定会被写入数据。
	 * 
	 * 申请空间后必须调用release方法释放掉没有使用的空间大小
	 * ***
	 * 
	 * @param size 申请的空间大小
	 * @return 申请成功返回true；否则返回false
	 */
	public boolean obtain(int size) {
		while(true) {
			int current = contentLength.get();
			if(current + size > capacity) {
				return false;
			}
			
			int count = writing.get();
			if(count != 0) {
				//这里能有两种情况：
				//1、count < 0; 已经被clean了，或者处于clean检测阶段，暂时无法使用
				//2、count > 0; 当前文件正在被写入，不能同时对一个文件进行多个写入操作
				return false;
			}
			
			//增加文件使用长度前，先保证writing计数的增加
			if(writing.compareAndSet(count, count + 1)) {
				//writing计数增加成功，可以尝试增加文件的长度
				if(contentLength.compareAndSet(current, current + size)) {
					//如果文件长度也增加成功，则可以正常返回
					return true;
				} else {
					//文件长度发送变化，取消wrting计数的自增
					writing.decrementAndGet();
				}
			}
		}
	}
}
