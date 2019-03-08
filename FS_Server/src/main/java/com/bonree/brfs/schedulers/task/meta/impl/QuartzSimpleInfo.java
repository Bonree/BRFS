package com.bonree.brfs.schedulers.task.meta.impl;

import java.util.HashMap;
import java.util.Map;

import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.schedulers.task.meta.SumbitTaskInterface;

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
	private long interval = -1;
	private int repeateCount = 0;
	private int taskKind = 1;
	private long delayTime = 0;
	private boolean runNowFlag = false;
	private boolean cycleFlag = false;
	public void putContent(String key, String value){
		this.taskContent.put(key, value);
	}
	@Override
	public String getTaskName() {
		return this.taskName;
	}

	@Override
	public String getTaskGroupName() {
		return this.taskGroupName;
	}

	@Override
	public String getClassInstanceName() {
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
	
	/**
	 * 概述：创建立即执行循环任务
	 * @param intervalTime 间隔时间
	 * @param jobMap 传入的数据
	 * @param clazz 对应的Class
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static QuartzSimpleInfo createCycleTaskInfo(String name, long intervalTime,long delayTime, Map<String, String> jobMap, Class<?> clazz) {
		if(BrStringUtils.isEmpty(name)|| intervalTime <=0){
			return null;
		}
		QuartzSimpleInfo simple = new QuartzSimpleInfo();
		simple.setTaskName(name);
		simple.setTaskGroupName(name);
		simple.setClassInstanceName(clazz.getCanonicalName());
		simple.setCycleFlag(true);
		simple.setInterval(intervalTime);
		if(delayTime <0){
			simple.setRunNowFlag(true);
		}else{
			simple.setRunNowFlag(false);
			simple.setDelayTime(delayTime);
		}
		if(jobMap != null && !jobMap.isEmpty()){
			simple.setTaskContent(jobMap);
		}
		return simple;
	}


	@Override
	public Map<String, String> getTaskContent() {
		return taskContent;
	}

	@Override
	public int getTaskKind() {
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
