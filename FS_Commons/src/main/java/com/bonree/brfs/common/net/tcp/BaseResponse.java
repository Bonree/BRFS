package com.bonree.brfs.common.net.tcp;

public class BaseResponse {
	private final int code;
	private byte[] body;
	
	public BaseResponse(int code) {
		this.code = code;
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
