package com.bonree.brfs.common.net.tcp;

public class BaseMessage {
	private final int type;
	private int token;
	private byte[] body;

	public BaseMessage(int type) {
		this.type = type;
	}
	
	public BaseMessage(int type, int token) {
		this.type = type;
		this.token = token;
	}
	
	public int getType() {
		return type;
	}
	
	public void setToken(int token) {
		this.token = token;
	}
	
	public int getToken() {
		return token;
	}

	public byte[] getBody() {
		return body;
	}

	public void setBody(byte[] body) {
		this.body = body;
	}
}
