package com.bonree.brfs.duplication.datastream.tasks;

public class WriteTaskResult {
	private int offset;
	private int size;

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
	public boolean equals(Object obj) {
		if(obj == null || !(obj instanceof WriteTaskResult)) {
			return false;
		}
		
		WriteTaskResult result = (WriteTaskResult) obj;
		return offset == result.getOffset() && size == result.getSize();
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("[offset=").append(offset).append(", size=").append(size).append("]");
		
		return builder.toString();
	}
}
