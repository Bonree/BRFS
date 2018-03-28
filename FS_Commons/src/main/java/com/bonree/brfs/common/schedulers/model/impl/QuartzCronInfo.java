package com.bonree.brfs.common.schedulers.model.impl;

import java.util.HashMap;
import java.util.Map;

import com.bonree.brfs.common.schedulers.model.TaskInterface;
/******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年3月28日 下午3:57:13
 * @Author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @Description: Quartz Cron任务信息
 *****************************************************************************
 */
public class QuartzCronInfo implements TaskInterface {
	private String taskName;
	private String taskGroupName;
	private String classInstanceName;
	private Map<String,String> taskContent = new HashMap<String,String>();
	private String cronTime;
	private int taskKind = 0;
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
		return this.cronTime;
	}

	@Override
	public Map<String, String> getTaskContent() {
		// TODO Auto-generated method stub
		return taskContent;
	}

	@Override
	public int getTaskKind() {
		// TODO Auto-generated method stub
		return 0;
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
		this.taskContent = taskContent;
	}

	public void setCronTime(String cronTime) {
		this.cronTime = cronTime;
	}
}
