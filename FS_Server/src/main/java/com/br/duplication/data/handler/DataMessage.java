package com.br.duplication.data.handler;

public class DataMessage {
	private int storageNameId;
	private byte[] data;

	public int getStorageNameId() {
		return storageNameId;
	}

	public void setStorageNameId(int storageNameId) {
		this.storageNameId = storageNameId;
	}

	public byte[] getData() {
		return data;
	}

	public void setData(byte[] data) {
		this.data = data;
	}
}
