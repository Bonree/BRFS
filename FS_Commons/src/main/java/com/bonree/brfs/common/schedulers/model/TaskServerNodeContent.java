package com.bonree.brfs.common.schedulers.model;

import java.util.ArrayList;
import java.util.List;

public class TaskServerNodeContent {
	private int taskType;
	private int taskState;
	private long createTime;
	private long operationStartTime;
	private long operationEndTime;
	private long taskStartTime;
	private long taskStopTime;
	private List<String> taskStorageNames = new ArrayList<String>();
	private String taskOperationContent;
	private String result;
	public int getTaskType() {
		return taskType;
	}
	public void setTaskType(int taskType) {
		this.taskType = taskType;
	}
	public int getTaskState() {
		return taskState;
	}
	public void setTaskState(int taskState) {
		this.taskState = taskState;
	}
	public long getCreateTime() {
		return createTime;
	}
	public void setCreateTime(long createTime) {
		this.createTime = createTime;
	}
	public long getOperationStartTime() {
		return operationStartTime;
	}
	public void setOperationStartTime(long operationStartTime) {
		this.operationStartTime = operationStartTime;
	}
	public long getOperationEndTime() {
		return operationEndTime;
	}
	public void setOperationEndTime(long operationEndTime) {
		this.operationEndTime = operationEndTime;
	}
	public long getTaskStartTime() {
		return taskStartTime;
	}
	public void setTaskStartTime(long taskStartTime) {
		this.taskStartTime = taskStartTime;
	}
	public long getTaskStopTime() {
		return taskStopTime;
	}
	public void setTaskStopTime(long taskStopTime) {
		this.taskStopTime = taskStopTime;
	}
	public List<String> getTaskStorageNames() {
		return taskStorageNames;
	}
	public void setTaskStorageNames(List<String> taskStorageNames) {
		this.taskStorageNames = taskStorageNames;
	}
	public String getTaskOperationContent() {
		return taskOperationContent;
	}
	public void setTaskOperationContent(String taskOperationContent) {
		this.taskOperationContent = taskOperationContent;
	}
	public String getResult() {
		return result;
	}
	public void setResult(String result) {
		this.result = result;
	}
	
}
