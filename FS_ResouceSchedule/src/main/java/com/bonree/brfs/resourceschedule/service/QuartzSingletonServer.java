
package com.bonree.brfs.resourceschedule.service;

import java.text.ParseException;
import java.util.Properties;

import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;

import com.bonree.brfs.resourceschedule.config.JobConfig;
import com.bonree.brfs.resourceschedule.utils.StringUtils;

/*****************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年3月15日 下午6:02:52
 * @Author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @Description: quartz定时服务单例模式
 *****************************************************************************
 */
public class QuartzSingletonServer implements CycleServerInterface {

	private StdSchedulerFactory ssf = new StdSchedulerFactory();
	private Properties props = null;
	private Scheduler scheduler = null;
	private static class SingletonInstance {
		public static QuartzSingletonServer instance = new QuartzSingletonServer();
	}

	private QuartzSingletonServer() {
	}

	public static QuartzSingletonServer getInstance() {
		return SingletonInstance.instance;
	}

	@Override
	public void initProperties(Properties props) throws SchedulerException {
		if (props == null) {
			this.props = new Properties();
			this.props.put(StdSchedulerFactory.PROP_THREAD_POOL_CLASS, "org.quartz.simpl.SimpleThreadPool");
			this.props.put("org.quartz.threadPool.threadCount", "3");
			this.props.put("org.quartz.scheduler.instanceName", "defaultCycle");
		}
		else {
			this.props = props;
		}
		ssf.initialize(this.props);
	}

	@Override
	public boolean loadJob(JobConfig jobConfig) throws ParseException, SchedulerException, NullPointerException {
		if (this.props == null) {
			throw new NullPointerException("properties is empty");
		}
		if (scheduler == null) {
			scheduler = this.ssf.getScheduler();
		}
		if (jobConfig == null) {
			throw new NullPointerException("JobConfig is empty");
		}
		if (jobConfig.getJobClass() == null) {
			throw new NullPointerException("jobClass is empty");
		}
		if (StringUtils.isEmpty(jobConfig.getCronTime())) {
			throw new NullPointerException("cronTime is empty");
		}
		if (StringUtils.isEmpty(jobConfig.getJobName())) {
			throw new NullPointerException("jobName is empty");
		}
		if (StringUtils.isEmpty(jobConfig.getJobGroup())) {
			throw new NullPointerException("jobGroup is empty");
		}
		Class<? extends Job> clazz = (Class<? extends Job>) jobConfig.getJobClass();
		String jobName = jobConfig.getJobName();
		String jobCronTime = jobConfig.getCronTime();
		String jobGroup = jobConfig.getJobGroup();
		JobDetail jobDetail = JobBuilder.newJob(clazz).withIdentity(jobName, jobGroup).build();
		CronScheduleBuilder cronScheduleBuilder = CronScheduleBuilder.cronSchedule(jobCronTime);
		CronTrigger cronTrigger = TriggerBuilder.newTrigger().withIdentity(jobName, jobGroup).withSchedule(
			cronScheduleBuilder).build();
		scheduler.scheduleJob(jobDetail, cronTrigger);
		return false;
	}

	@Override
	public void start() throws SchedulerException {
		// TODO Auto-generated method stub
		if (this.props == null) {
			throw new NullPointerException("properties is empty");
		}
		if (scheduler == null) {
			scheduler = this.ssf.getScheduler();
		}
		this.scheduler.start();
	}

	@Override
	public void close() throws SchedulerException, NullPointerException {
		if (this.scheduler == null) {
			throw new NullPointerException("Scheduler is null");
		}
		this.scheduler.shutdown();
	}

	@Override
	public boolean isStart() throws SchedulerException, NullPointerException {
		if (this.scheduler == null) {
			throw new NullPointerException("Scheduler is null");
		}
		return this.scheduler.isStarted();
	}

	@Override
	public boolean isShuttdown() throws SchedulerException, NullPointerException {
		if (this.scheduler == null) {
			throw new NullPointerException("Scheduler is null");
		}
		return this.scheduler.isShutdown();
	}

	@Override
	public boolean killJob(JobConfig jobConfig) throws SchedulerException, NullPointerException {
		// TODO Auto-generated method stub
		if (jobConfig == null) {
			throw new NullPointerException("JobConfig is empty");
		}
		if (StringUtils.isEmpty(jobConfig.getJobName())) {
			throw new NullPointerException("jobName is empty");
		}
		if (StringUtils.isEmpty(jobConfig.getJobGroup())) {
			throw new NullPointerException("jobGroup is empty");
		}
		if (this.scheduler == null) {
			throw new NullPointerException("Scheduler is null");
		}
		JobKey jobKey = new JobKey(jobConfig.getJobName(), jobConfig.getJobGroup());
		return this.scheduler.deleteJob(jobKey);
	}

}
