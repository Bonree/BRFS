package com.bonree.brfs.schedulers.task.model;

import java.util.ArrayList;
import java.util.List;

import com.bonree.brfs.common.task.TaskState;

/*****************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年4月13日 下午2:55:12
 * @Author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @Description: 任务服务节点
 *****************************************************************************
 */
public class TaskServerNodeModel {
	private String taskStartTime;
	private String taskStopTime;
	private int taskState = TaskState.INIT.code();
	private int retryCount = 0;
	private List<AtomTaskModel> sAtoms = new ArrayList<AtomTaskModel>();
	private TaskResultModel result;

	public TaskResultModel getResult() {
		return result;
	}
	public void setResult(TaskResultModel result) {
		this.result = result;
	}
	public int getTaskState() {
		return taskState;
	}
	public void setTaskState(int taskState) {
		this.taskState = taskState;
	}
	public List<AtomTaskModel> getsAtoms() {
		return sAtoms;
	}
	public void setsAtoms(List<AtomTaskModel> sAtoms) {
		this.sAtoms = sAtoms;
	}
	public void addAll(List<AtomTaskModel> sAtoms){
		if(this.sAtoms == null){
			this.sAtoms = new ArrayList<>();
		}
		this.sAtoms.addAll(sAtoms);
	}
	public void add(AtomTaskModel atom){
		if(this.sAtoms == null){
			this.sAtoms = new ArrayList<>();
		}
		this.sAtoms.add(atom);
	}
	public int getRetryCount() {
		return retryCount;
	}
	public void setRetryCount(int retryCount) {
		this.retryCount = retryCount;
	}
	public String getTaskStartTime() {
		return taskStartTime;
	}
	public void setTaskStartTime(String taskStartTime) {
		this.taskStartTime = taskStartTime;
	}
	public String getTaskStopTime() {
		return taskStopTime;
	}
	public void setTaskStopTime(String taskStopTime) {
		this.taskStopTime = taskStopTime;
	}
	/**
	 * 概述：获取初始化对象
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static TaskServerNodeModel getInitInstance() {
		TaskServerNodeModel task = new TaskServerNodeModel();
		task.setTaskState(TaskState.INIT.code());
		return task;
	}
	
}
