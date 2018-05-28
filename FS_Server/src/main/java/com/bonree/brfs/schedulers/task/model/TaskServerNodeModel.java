package com.bonree.brfs.schedulers.task.model;

import java.util.ArrayList;
import java.util.List;

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
	private long taskStartTime;
	private long taskStopTime;
	private int taskState;
	private List<AtomTaskModel> sAtoms = new ArrayList<AtomTaskModel>();
	private TaskResultModel result;
	
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
	
}
