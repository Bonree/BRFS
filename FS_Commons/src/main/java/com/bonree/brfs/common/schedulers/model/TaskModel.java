package com.bonree.brfs.common.schedulers.model;

import java.util.ArrayList;
import java.util.List;

public class TaskModel {
	/**
	 * 任务类型taskType
	 */
	private int taskType;
	/**
	 * 任务状态，TaskStat
	 */
	private int taskState;
	/**
	 * 任务创建时间
	 */
	private long createTime;
	/**
	 * sn执行最小单元
	 */
	private List<AtomTaskModel> atomList = new ArrayList<AtomTaskModel>();
	/**
	 * 任务结果
	 */
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
	public String getResult() {
		return result;
	}
	public void setResult(String result) {
		this.result = result;
	}
	public List<AtomTaskModel> getAtomList() {
		return atomList;
	}
	public void setAtomList(List<AtomTaskModel> atomList) {
		this.atomList = atomList;
	}
	
}
