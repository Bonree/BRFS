package com.bonree.brfs.common.net.tcp;

public class BaseMessage {
	private final int token;
	private final int type;
	private byte[] body;

	public BaseMessage(int token, int type) {
		this.token = token;
		this.type = type;
	}
	
	public int getToken() {
		return token;
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
