package com.bonree.brfs.resourceschedule.config;

import org.quartz.Job;

/*****************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年3月15日 上午10:04:35
 * @Author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @Description: 定时任务配置格式
 *****************************************************************************
 */
public class JobConfig {
	/**
	 * job类名称，完全类路径
	 */
	private String jobClassName;
	/**
	 * cron 触发时间设定
	 */
	private String cronTime;
	/**
	 * job的名称，
	 */
	private String jobName;
	/**
	 * job的分组名称
	 */
	private String jobGroup;
	private Class<?> jobClass;
	public JobConfig(String cronTime, String jobName, String jobGroup, Class<? extends Job> jobClass) {
		this.cronTime = cronTime;
		this.jobName = jobName;
		this.jobGroup = jobGroup;
		this.jobClass = jobClass;
		this.jobClassName = this.jobClass.getCanonicalName();
	}
	public JobConfig(String jobClassName, String cronTime, String jobName, String jobGroup) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
		this.jobClassName = jobClassName;
		this.cronTime = cronTime;
		this.jobName = jobName;
		this.jobGroup = jobGroup;
		if(Class.forName(jobClassName).newInstance() instanceof Job){
			this.jobClass = (Class<? extends Job>) Class.forName(jobClassName);
		}else{
			throw new NullPointerException("className : "+ jobClassName + " is not Job");
		}
	}
	public JobConfig() {
	}
	public String getJobClassName() {
		return jobClassName;
	}
	public void setJobClassName(String jobClassName) {
		this.jobClassName = jobClassName;
	}
	public String getCronTime() {
		return cronTime;
	}
	public void setCronTime(String cronTime) {
		this.cronTime = cronTime;
	}
	public String getJobName() {
		return jobName;
	}
	public void setJobName(String jobName) {
		this.jobName = jobName;
	}
	public String getJobGroup() {
		return jobGroup;
	}
	public void setJobGroup(String jobGroup) {
		this.jobGroup = jobGroup;
	}
	public Class<?> getJobClass() {
		return jobClass;
	}
	public void setJobClass(Class<? extends Job> jobClass) {
		this.jobClass = jobClass;
	}
	public void setJobClass(String className) throws ClassNotFoundException, InstantiationException, IllegalAccessException, NullPointerException{
		this.jobClass =  Class.forName(className);
	}
}
