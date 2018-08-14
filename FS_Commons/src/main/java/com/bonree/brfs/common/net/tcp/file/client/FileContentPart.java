package com.bonree.brfs.common.net.tcp.file.client;

public class FileContentPart {
	private final byte[] bytes;
	private final boolean endOfContent;
	
	public FileContentPart(byte[] bytes, boolean endOfContent) {
		this.bytes = bytes;
		this.endOfContent = endOfContent;
	}
	
	public byte[] content() {
		return bytes;
	}
	
	public boolean endOfContent() {
		return endOfContent;
	}
}
