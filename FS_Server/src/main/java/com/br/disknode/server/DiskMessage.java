package com.br.disknode.server;

import java.util.Map;

public class DiskMessage {
	private String filePath;
	private Map<String, Object> params;

	public String getFilePath() {
		return filePath;
	}

	public void setFilePath(String filePath) {
		this.filePath = filePath;
	}

	public Map<String, Object> getParams() {
		return params;
	}

	public void setParams(Map<String, Object> params) {
		this.params = params;
	}
}
