package com.bonree.brfs.duplication.datastream.file;

import java.util.Comparator;

import com.bonree.brfs.duplication.filenode.FileNode;

public class FileObject {
	private volatile long length;
	private volatile long applyLength;
	private FileNode fileNode;
	
	public FileObject(FileNode node) {
		this.fileNode = node;
	}
	
	public FileNode node() {
		return this.fileNode;
	}
	
	public long capacity() {
		return this.fileNode.getCapacity();
	}
	
	public long free() {
		return capacity() - applyLength;
	}
	
	public long length() {
		return length;
	}
	
	public void setLength(long length) {
		this.length = length;
		this.applyLength = length;
	}
	
	public boolean apply(int size) {
		if(free() < size) {
			return false;
		}
		
		applyLength += size;
		return true;
	}
	
	public static final Comparator<FileObject> LENGTH_COMPARATOR = new Comparator<FileObject>() {

		@Override
		public int compare(FileObject f1, FileObject f2) {
			if(f1.length() > f2.length()) {
				return 1;
			}
			
			if(f1.length() < f2.length()) {
				return -1;
			}
			
			return 0;
		}
	};
}
