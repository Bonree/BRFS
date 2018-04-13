package com.bonree.brfs.common.schedulers.model;

import java.util.ArrayList;
import java.util.List;

import com.bonree.brfs.common.schedulers.task.TaskStat;
import com.bonree.brfs.common.utils.DateFormatUtils;

public class TaskMessageModel {
	/**
	 * 任务类型
	 */
	private int taskType;
	/**
	 * 操作范围 0：部分storageName，1：全部storageName
	 */
	private int incidence;
	private List<TaskSNMessageModel> storageMessages = new ArrayList<>();
	public int getTaskType() {
		return taskType;
	}
	public void setTaskType(int taskType) {
		this.taskType = taskType;
	}
	public int getIncidence() {
		return incidence;
	}
	public void setIncidence(int incidence) {
		this.incidence = incidence;
	}
	public List<TaskSNMessageModel> getStorageMessages() {
		return storageMessages;
	}
	public void setStorageMessages(List<TaskSNMessageModel> storageMessages) {
		this.storageMessages = storageMessages;
	}
}
