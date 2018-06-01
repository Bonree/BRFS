package com.bonree.brfs.schedulers.task.model;

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
	private String createTime;
	/**
	 * sn执行最小单元
	 */
	private List<AtomTaskModel> atomList = new ArrayList<AtomTaskModel>();
	/**
	 * 任务结果
	 */
	private TaskResultModel result;
	private int retryCount = 0;
	/**
	 * 处理数据的开始时间
	 */
	private String startDataTime;
	/**
	 * 处理数据的结束时间
	 */
	private String endDataTime;
	/**
	 * 任务操作
	 */
	private String taskOperation;
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
	public TaskResultModel getResult() {
		return result;
	}
	public void setResult(TaskResultModel result) {
		this.result = result;
	}
	public List<AtomTaskModel> getAtomList() {
		return atomList;
	}
	public void setAtomList(List<AtomTaskModel> atomList) {
		this.atomList = atomList;
	}
	public void putAtom(List<AtomTaskModel> atoms){
		this.atomList.addAll(atoms);
	}
	public void addAtom(AtomTaskModel atom){
		this.atomList.add(atom);
	}
	public int getRetryCount() {
		return retryCount;
	}
	public void setRetryCount(int retryCount) {
		this.retryCount = retryCount;
	}
	public String getCreateTime() {
		return createTime;
	}
	public void setCreateTime(String createTime) {
		this.createTime = createTime;
	}
	public String getStartDataTime() {
		return startDataTime;
	}
	public void setStartDataTime(String startDataTime) {
		this.startDataTime = startDataTime;
	}
	public String getEndDataTime() {
		return endDataTime;
	}
	public void setEndDataTime(String endDataTime) {
		this.endDataTime = endDataTime;
	}
	public String getTaskOperation() {
		return taskOperation;
	}
	public void setTaskOperation(String taskOperation) {
		this.taskOperation = taskOperation;
	}
}
