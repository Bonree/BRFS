
package com.bonree.brfs.resourceschedule.service;

import java.text.ParseException;
import java.util.Properties;

import org.quartz.CronTrigger;
import org.quartz.Job;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.JobBuilder;
import org.quartz.TriggerBuilder;
import org.quartz.CronScheduleBuilder;

import com.bonree.brfs.resourceschedule.utils.StringUtils;
import com.bonree.brfs.resourceschedule.gather.job.GatherBaseResourceInfoJob;

/*****************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年3月13日 下午4:20:38
 * @Author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @Description: 定时服务
 *****************************************************************************
 */
public enum QuarzeServer {
	instance;
	private StdSchedulerFactory ssf = new StdSchedulerFactory();
	private Properties props = null;
	private Scheduler scheduler = null;
	/**
	 * 概述：初始化quarz的配置信息
	 * @param properties
	 * @throws SchedulerException
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public void initProperties(Properties properties) throws SchedulerException{
		if(properties == null){
			props = new Properties();
			props.put(StdSchedulerFactory.PROP_THREAD_POOL_CLASS, "org.quartz.simpl.SimpleThreadPool");
			props.put("org.quartz.threadPool.threadCount", "3");
			props.put("org.quartz.scheduler.instanceName", "ResourceManager");
		}else{
			props = properties;
		}
		ssf.initialize(props);
	}
	/**
	 * 概述：加载job
	 * @param jobDetail
	 * @param cronTrigger
	 * @return
	 * @throws SchedulerException
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public boolean loadJob(JobDetail jobDetail, CronTrigger cronTrigger) throws SchedulerException{
		if(scheduler == null){
			scheduler = ssf.getScheduler();
		}
		if(scheduler.isStarted()){
			return false;
		}
		scheduler.scheduleJob(jobDetail, cronTrigger);
		return true;
	}
	/**
	 * 概述：加载job
	 * @param jobClass
	 * @param cronTime
	 * @param jobName
	 * @param jobGroup
	 * @return
	 * @throws ParseException
	 * @throws SchedulerException
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public boolean loadJob(Class <? extends Job> jobClass,String cronTime, String jobName, String jobGroup) throws NullPointerException,ParseException, SchedulerException{
		if(StringUtils.isEmpty(cronTime)){
			throw new NullPointerException("cronTime is empty");
		}
		if(StringUtils.isEmpty(jobName)){
			throw new NullPointerException("jobName is empty");
		}
		if(StringUtils.isEmpty(jobGroup)){
			throw new NullPointerException("jobGroup is empty");
		}
		JobDetail jobDetail = JobBuilder.newJob(jobClass)
				.withIdentity(jobName, jobGroup)
				.build();
		CronScheduleBuilder cronScheduleBuilder = CronScheduleBuilder.cronSchedule(cronTime);
		CronTrigger cronTrigger = TriggerBuilder.newTrigger()
				.withIdentity(jobName, jobGroup)
				.withSchedule(cronScheduleBuilder)
				.build();
		return loadJob(jobDetail, cronTrigger);
	}
	/**
	 * 概述：加载job
	 * @param jobClassName
	 * @param cronTime
	 * @param jobName
	 * @param jobGroup
	 * @return
	 * @throws ClassNotFoundException
	 * @throws NullPointerException
	 * @throws ParseException
	 * @throws SchedulerException
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public boolean loadJob(String jobClassName, String cronTime, String jobName, String jobGroup) throws ClassNotFoundException, NullPointerException, ParseException, SchedulerException{
		if(StringUtils.isEmpty(jobClassName)){
			throw new NullPointerException("jobClassName is empty");
		}
		Class<? extends Job> clz = (Class<? extends Job>) Class.forName(jobClassName);
		return loadJob(clz,cronTime,jobName,jobGroup);
	}
	
	public boolean startServer() throws SchedulerException {
		if(props == null){
			initProperties(null);
		}
		if(scheduler == null){
			scheduler = ssf.getScheduler();
		}
		if(scheduler.isStarted()){
			return false;
		}
		if(scheduler.getJobGroupNames().size() == 0){
			return false;
		}
		scheduler.start();
		return true;
	}
}
