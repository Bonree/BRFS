package com.bonree.brfs.common.schedulers.model;

import java.util.ArrayList;
import java.util.List;

public class TaskServerNodeContent {
	private long taskStartTime;
	private long taskStopTime;
	private List<String> taskStorageNames = new ArrayList<String>();
	private String taskOperationContent;
	private String result;
	
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
