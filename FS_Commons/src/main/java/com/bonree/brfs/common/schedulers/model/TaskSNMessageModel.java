package com.bonree.brfs.common.schedulers.model;

public class TaskSNMessageModel {
	/**
	 * storagename
	 */
	private String storageName;
	/**
	 * 操作内容
	 */
	private String operationContent;
	/**
	 * 数据的开始时间
	 */
	private long startTime;
	/**
	 * 数据的结束时间
	 */
	private long endTime;
	public String getStorageName() {
		return storageName;
	}
	public void setStorageName(String storageName) {
		this.storageName = storageName;
	}
	public String getOperationContent() {
		return operationContent;
	}
	public void setOperationContent(String operationContent) {
		this.operationContent = operationContent;
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
