package com.bonree.brfs.disknode.server.handler.data;

public class WriteData {
	private int sequence;
	private byte[] bytes;

	public int getSequence() {
		return sequence;
	}

	public void setSequence(int sequence) {
		this.sequence = sequence;
	}

	public byte[] getBytes() {
		return bytes;
	}

	public void setBytes(byte[] bytes) {
		this.bytes = bytes;
	}

}
