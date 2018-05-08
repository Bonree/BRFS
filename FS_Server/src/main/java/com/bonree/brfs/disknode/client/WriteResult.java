package com.bonree.brfs.disknode.client;

public class WriteResult {
	private long offset;
	private int size;

	public long getOffset() {
		return offset;
	}

	public void setOffset(long offset) {
		this.offset = offset;
	}

	public int getSize() {
		return size;
	}

	public void setSize(int size) {
		this.size = size;
	}

}
