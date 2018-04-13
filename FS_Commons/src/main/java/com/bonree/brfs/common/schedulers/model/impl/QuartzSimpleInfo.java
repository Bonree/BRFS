package com.bonree.brfs.common.schedulers.model.impl;

import java.util.HashMap;
import java.util.Map;

import com.bonree.brfs.common.schedulers.model.SumbitTaskInterface;
/******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年3月28日 下午3:57:42
 * @Author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @Description: Quartz simple任务信息 任务类型为1
 *****************************************************************************
 */
public class QuartzSimpleInfo implements SumbitTaskInterface {
	private String taskName;
	private String taskGroupName;
	private String classInstanceName;
	private Map<String,String> taskContent = new HashMap<String,String>();
	private long interval = 60000;
	private int repeateCount = 3;
	private int taskKind = 1;
	private long delayTime = 0;
	private boolean runNowFlag = false;
	private boolean cycleFlag = false;
	public void putContent(String key, String value){
		this.taskContent.put(key, value);
	}
	@Override
	public String getTaskName() {
		// TODO Auto-generated method stub
		return this.taskName;
	}

	@Override
	public String getTaskGroupName() {
		// TODO Auto-generated method stub
		return this.taskGroupName;
	}

	@Override
	public String getClassInstanceName() {
		// TODO Auto-generated method stub
		return this.classInstanceName;
	}

	@Override
	public String getCycleContent() {
		// TODO Auto-generated method stub
		StringBuilder str = new StringBuilder();
		str.append(this.interval)
		.append(",").append(this.repeateCount)
		.append(",").append(this.delayTime)
		.append(",").append(this.runNowFlag)
		.append(",").append(this.cycleFlag);
		return str.toString();
	}

	@Override
	public Map<String, String> getTaskContent() {
		// TODO Auto-generated method stub
		return taskContent;
	}

	@Override
	public int getTaskKind() {
		// TODO Auto-generated method stub
		return this.taskKind;
	}

	public void setTaskName(String taskName) {
		this.taskName = taskName;
	}

	public void setTaskGroupName(String taskGroupName) {
		this.taskGroupName = taskGroupName;
	}

	public void setClassInstanceName(String classInstanceName) {
		this.classInstanceName = classInstanceName;
	}

	public void setTaskContent(Map<String, String> taskContent) {
		this.taskContent.putAll(taskContent);
	}
	public void setInterval(long interval) {
		this.interval = interval;
	}
	public void setRepeateCount(int repeateCount) {
		this.repeateCount = repeateCount;
	}
	public long getDelayTime() {
		return delayTime;
	}
	public void setDelayTime(long delayTime) {
		this.delayTime = delayTime;
	}
	public boolean isRunNowFlag() {
		return runNowFlag;
	}
	public void setRunNowFlag(boolean runNowFlag) {
		this.runNowFlag = runNowFlag;
	}
	public boolean isCycleFlag() {
		return cycleFlag;
	}
	public void setCycleFlag(boolean cycleFlag) {
		this.cycleFlag = cycleFlag;
	}
	public long getInterval() {
		return interval;
	}
	public int getRepeateCount() {
		return repeateCount;
	}
	public void setTaskKind(int taskKind) {
		this.taskKind = taskKind;
	}
	

}
