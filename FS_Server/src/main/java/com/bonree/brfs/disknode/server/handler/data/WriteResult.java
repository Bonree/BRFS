package com.bonree.brfs.disknode.server.handler.data;

public class WriteResult {
	private int sequence;
	private int size;

	public int getSequence() {
		return sequence;
	}

	public void setSequence(int sequence) {
		this.sequence = sequence;
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
		builder.append("{").append(sequence).append(", ").append(size).append("}");
		
		return builder.toString();
	}

}
