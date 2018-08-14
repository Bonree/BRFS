package com.bonree.brfs.common.net.tcp;

public class BaseResponse {
	private final int token;
	private final int code;
	private byte[] body;
	
	public BaseResponse(int token, int code) {
		this.token = token;
		this.code = code;
	}

	public int getToken() {
		return token;
	}
	
	public int getCode() {
		return code;
	}

	public byte[] getBody() {
		return body;
	}

	public void setBody(byte[] body) {
		this.body = body;
	}
}
