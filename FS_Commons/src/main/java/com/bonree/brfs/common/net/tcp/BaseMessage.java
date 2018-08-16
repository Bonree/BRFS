package com.bonree.brfs.common.net.tcp;

public class BaseMessage {
	private final int type;
	private byte[] body;

	public BaseMessage(int type) {
		this.type = type;
	}
	
	public int getType() {
		return type;
	}

	public byte[] getBody() {
		return body;
	}

	public void setBody(byte[] body) {
		this.body = body;
	}
}
