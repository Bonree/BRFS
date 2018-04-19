package com.bonree.brfs.common.http;

import java.util.Map;

public class HttpMessage {
	private String path;
	private Map<String, String> params;
	private byte[] content;

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public Map<String, String> getParams() {
		return params;
	}

	public void setParams(Map<String, String> params) {
		this.params = params;
	}

	public byte[] getContent() {
		return content;
	}

	public void setContent(byte[] content) {
		this.content = content;
	}
}
