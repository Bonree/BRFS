package com.bonree.brfs.disknode.server.handler.data;

public class WriteData {
	private int diskSequence;
	private byte[] bytes;

	public int getDiskSequence() {
		return diskSequence;
	}

	public void setDiskSequence(int diskSequence) {
		this.diskSequence = diskSequence;
	}
	
	public byte[] getBytes() {
		return bytes;
	}

	public void setBytes(byte[] bytes) {
		this.bytes = bytes;
	}

}
