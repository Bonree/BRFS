package com.br.disknode;

public class WriteItem {
	private String filePath;
	private byte[] data;
	
	public WriteItem(String filePath) {
		this.filePath = filePath;
	}
	
	public WriteItem(String filePath, byte[] data) {
		this.filePath = filePath;
		this.data = data;
	}

	public String getFilePath() {
		return filePath;
	}

	public void setFilePath(String filePath) {
		this.filePath = filePath;
	}

	public byte[] getData() {
		return data;
	}

	public void setData(byte[] data) {
		this.data = data;
	}

}
