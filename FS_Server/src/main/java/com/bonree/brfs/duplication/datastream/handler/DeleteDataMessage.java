package com.bonree.brfs.duplication.datastream.handler;

public class DeleteDataMessage {
	private int storageNameId;
	private long startTime;
	private long endTime;

	public int getStorageNameId() {
		return storageNameId;
	}

	public void setStorageNameId(int storageNameId) {
		this.storageNameId = storageNameId;
	}

	public long getStartTime() {
		return startTime;
	}

	public void setStartTime(long startTime) {
		this.startTime = startTime;
	}

	public long getEndTime() {
		return endTime;
	}

	public void setEndTime(long endTime) {
		this.endTime = endTime;
	}
}
