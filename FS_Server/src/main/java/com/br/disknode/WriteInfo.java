package com.br.disknode;

public class WriteInfo {
	private int offset;
	private int size;
	
	public WriteInfo(int offset, int size) {
		this.offset = offset;
		this.size = size;
	}

	public int getOffset() {
		return offset;
	}

	public void setOffset(int offset) {
		this.offset = offset;
	}

	public int getSize() {
		return size;
	}

	public void setSize(int size) {
		this.size = size;
	}
	
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("offset=").append(offset)
		       .append(", size=").append(size);
		
		return builder.toString();
	}
}
