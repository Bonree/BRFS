package com.bonree.brfs.disknode.server.handler.data;

public class WriteResult {
	private final long offset;
	private final int size;
	
	public WriteResult(long offset, int size) {
		this.offset = offset;
		this.size = size;
	}
	
	public long getOffset() {
		return offset;
	}

	public int getSize() {
		return size;
	}
	
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("{").append("offset:").append(offset).append(",size:").append(size).append("}");
		
		return builder.toString();
	}

}
