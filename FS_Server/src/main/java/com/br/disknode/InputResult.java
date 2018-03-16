package com.br.disknode;

public class InputResult {
	private int offset;
	private int size;
	private long crc;
	
	public InputResult(int offset, int size) {
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
	
	public long getCrc() {
		return crc;
	}

	public void setCrc(long crc) {
		this.crc = crc;
	}
	
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("offset=").append(offset)
		       .append(", size=").append(size);
		
		return builder.toString();
	}
}
