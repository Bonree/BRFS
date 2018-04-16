package com.bonree.brfs.schedulers.task.manager.impl;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.SimpleTrigger;
import org.quartz.Trigger;
import org.quartz.Trigger.TriggerState;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.impl.matchers.GroupMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.schedulers.task.manager.QuartzSchedulerInterface;
import com.bonree.brfs.schedulers.task.meta.SumbitTaskInterface;

/******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年3月28日 下午4:21:21
 * @Author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @Description: quartz基础调度实现
 *****************************************************************************
 */
public class QuartzBaseSchedulers<T extends SumbitTaskInterface> implements QuartzSchedulerInterface<T> {
	private static final Logger LOG = LoggerFactory.getLogger("CycleTest");
	private StdSchedulerFactory ssf = new StdSchedulerFactory();
	private String instanceName = "server";
	private boolean pausePoolFlag = false;
	private int poolSize = 0;

	@Override
	public void initProperties(Properties props) throws Exception {
		Properties tmpprops = null;
		if (props == null) {
			tmpprops = new Properties();
			tmpprops.put(StdSchedulerFactory.PROP_THREAD_POOL_CLASS, "org.quartz.simpl.SimpleThreadPool");
			tmpprops.put("org.quartz.threadPool.threadCount", "3");
			tmpprops.put("quartz.jobStore.misfireThreshold", "1");
		}
		else {
			tmpprops = props;
		}
		tmpprops.put("org.quartz.scheduler.instanceName", instanceName);
		ssf.initialize(tmpprops);
		Scheduler ssh = ssf.getScheduler();
		this.instanceName = ssh.getSchedulerName();
		this.poolSize = Integer.valueOf(tmpprops.getProperty("org.quartz.threadPool.threadCount"));
	}

	@Override
	public boolean addTask(T task) throws Exception {
		// 1.检查任务的有效性
		if (!checkTask(task)) {
			return false;
		}
		// 2.线程池处于暂停时，不提交任务
		if(this.pausePoolFlag){
			return false;
		}
		// 1.设置job的名称及执行的class
		Class<? extends Job> clazz = (Class<? extends Job>) Class.forName(task.getClassInstanceName());
		String taskName = task.getTaskName();
		String taskGroup = task.getTaskGroupName();
		JobBuilder jobBuilder = JobBuilder.newJob(clazz).withIdentity(taskName, taskGroup);

		// 2.设置任务需要的数据
		Map<String, String> tmp = task.getTaskContent();
		if (tmp != null && !tmp.isEmpty()) {
			JobDataMap jobData = new JobDataMap();
			jobData.putAll(tmp);
			jobBuilder.usingJobData(jobData);
		}
		// 3.生成jobDetail
		JobDetail jobDetail = jobBuilder.build();

		// 4.判断触发器的类型 0 cron任务，1 simple任务
		int taskType = task.getTaskKind();
		String cycleContent = task.getCycleContent();
		Trigger trigger = null;
		if (taskType == 0) {
			CronScheduleBuilder cronScheduleBuilder = CronScheduleBuilder.cronSchedule(cycleContent);
			trigger = TriggerBuilder.newTrigger().withIdentity(taskName, taskGroup).withSchedule(
				cronScheduleBuilder).build();
		}
		else if (taskType == 1) {
			String[] cycles = BrStringUtils.getSplit(cycleContent, ",");
			if (cycles == null || cycles.length == 0) {
				throw new NullPointerException("simple trigger cycle time is empty !!! content : " + cycleContent);
			}
			if (cycles.length != 5) {
				throw new NullPointerException("simple trigger cycle time is error !!! content : " + cycleContent);
			}
			long interval = Long.valueOf(cycles[0]);
			int repeateCount = Integer.valueOf(cycles[1]);
			long delayTime = Long.valueOf(cycles[2]);
			boolean rightNow = Boolean.valueOf(cycles[3]);
			boolean cycleFlag = Boolean.valueOf(cycles[4]);
			SimpleScheduleBuilder builder = SimpleScheduleBuilder.simpleSchedule();
			builder.withIntervalInMilliseconds(interval);
			if(cycleFlag){
				builder.repeatForever();
			}else{
				builder.withRepeatCount(repeateCount);
			}
			TriggerBuilder trigBuilder = TriggerBuilder.newTrigger().withIdentity(taskName, taskGroup).withSchedule(builder);
			if(!rightNow && delayTime >0){
				long current = System.currentTimeMillis()+ delayTime;
				Date date = new Date(current);
				trigBuilder.startAt(date);
			}else{
				trigBuilder.startNow();
			}
			trigger = trigBuilder.build();
		}
		if (trigger == null || jobDetail == null) {
			return false;
		}
		Scheduler scheduler = this.ssf.getScheduler(this.instanceName);
		scheduler.scheduleJob(jobDetail, trigger);
		return true;
	}

	@Override
	public void start() throws Exception {
		Scheduler scheduler = this.ssf.getScheduler(this.instanceName);
		if (!scheduler.isStarted()) {
			scheduler.start();
		}
	}

	@Override
	public void close(boolean isWaitTaskComplete) throws Exception {
		Scheduler scheduler = this.ssf.getScheduler(this.instanceName);
		if (scheduler.isShutdown()) {
			return;
		}
		if (!isWaitTaskComplete) {
			scheduler.pauseAll();
			scheduler.clear();
		}
		scheduler.shutdown(isWaitTaskComplete);

	}

	@Override
	public boolean isStart() throws Exception {
		Scheduler scheduler = this.ssf.getScheduler(this.instanceName);
		return scheduler.isStarted();
	}

	@Override
	public boolean isShuttdown() throws Exception {
		Scheduler scheduler = this.ssf.getScheduler(this.instanceName);
		return scheduler.isShutdown();
	}

	@Override
	public boolean killTask(T task) throws Exception {
		TriggerKey triggerKey = TriggerKey.triggerKey(task.getTaskName(), task.getTaskGroupName());
		JobKey jobKey = new JobKey(task.getTaskName(), task.getTaskGroupName());
		Scheduler scheduler = this.ssf.getScheduler(this.instanceName);
		if (!scheduler.isShutdown()) {
			// 获取触发器的状态
			int stat = getTaskStat(task);
			if (stat == -1) {
				return false;
			}
			// 1.停止触发器
			scheduler.pauseTrigger(triggerKey);
			// 2.中断正在执行的任务
			if (isExecuting(task)) {
				scheduler.interrupt(jobKey);
			}
			// 3.移除触发器
			scheduler.unscheduleJob(triggerKey);
			// 4.删除任务
			scheduler.deleteJob(jobKey);// 删除任务 
		}
		return true;
	}

	@Override
	public boolean pauseTask(T task) throws Exception {
		TriggerKey triggerKey = TriggerKey.triggerKey(task.getTaskName(), task.getTaskGroupName());
		JobKey jobKey = new JobKey(task.getTaskName(), task.getTaskGroupName());
		Scheduler scheduler = this.ssf.getScheduler(this.instanceName);
		if (!scheduler.isShutdown()) {
			// 获取触发器的状态
			int stat = getTaskStat(task);
			if (stat == -1 || stat == 1 || stat == 2) {
				return false;
			}
			// 1.停止触发器
			scheduler.pauseTrigger(triggerKey);
			// 2.停止任务
			scheduler.pauseJob(jobKey);
			// 3.中断正在执行的程序
			if (isExecuting(task)) {
				scheduler.interrupt(jobKey);
			}
		}
		return true;
	}

	@Override
	public boolean resumeTask(T task) throws Exception {
		TriggerKey triggerKey = TriggerKey.triggerKey(task.getTaskName(), task.getTaskGroupName());
		JobKey jobKey = new JobKey(task.getTaskName(), task.getTaskGroupName());
		Scheduler scheduler = this.ssf.getScheduler(this.instanceName);
		if (!scheduler.isShutdown()) {
			// 获取触发器的状态
			int stat = getTaskStat(task);
			if (stat != 1) {
				return false;
			}
			scheduler.resumeTrigger(triggerKey);
			scheduler.resumeJob(jobKey);
		}
		return true;
	}

	@Override
	public boolean pauseAllTask() throws Exception {
		Scheduler scheduler = this.ssf.getScheduler(this.instanceName);
		if (!scheduler.isShutdown()) {
			if (!this.pausePoolFlag) {
				return false;
			}
			// 1.停止触发器
			scheduler.pauseAll();
			JobKey currentJob = null;
			// 2.中断所有执行的任务
			for (JobExecutionContext jobExecut : scheduler.getCurrentlyExecutingJobs()) {
				currentJob = jobExecut.getJobDetail().getKey();
				scheduler.interrupt(currentJob);
			}
			this.pausePoolFlag = true;
		}
		return false;
	}

	@Override
	public List<T> getPauseTask() throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<T> getAllTask() throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean resumeAllTask() throws Exception {
		Scheduler scheduler = this.ssf.getScheduler(this.instanceName);
		if (!scheduler.isShutdown()) {
			Set<String> pauseGroup = scheduler.getPausedTriggerGroups();
			if (pauseGroup == null || pauseGroup.isEmpty()) {
				return false;
			}
			scheduler.resumeAll();
		}
		return true;
	}

	@Override
	public boolean checkTask(T task) {
		if (BrStringUtils.isEmpty(task.getClassInstanceName())) {
			return false;
		}
		if (BrStringUtils.isEmpty(task.getTaskName())) {
			return false;
		}
		if (BrStringUtils.isEmpty(task.getTaskGroupName())) {
			return false;
		}
		if (BrStringUtils.isEmpty(task.getCycleContent())) {
			return false;
		}

		return true;
	}

	@Override
	public int getPoolStat() throws Exception {
		Scheduler scheduler = this.ssf.getScheduler(this.instanceName);
		if (scheduler.isShutdown()) {
			return 1; //关闭
		}
		if (this.pausePoolFlag) {
			return 2;//暂停
		}
		if (scheduler.isStarted()) {
			return 3;
		}
		return 0; //正常
	}

	@Override
	public String getInstanceName() throws Exception {
		return this.instanceName;
	}

	public boolean isPausePoolFlag() {
		return pausePoolFlag;
	}

	public void setPausePoolFlag(boolean pausePoolFlag) {
		this.pausePoolFlag = pausePoolFlag;
	}

	public void setInstanceName(String instanceName) {
		this.instanceName = instanceName;
	}

	/**
	 * 概述：获取任务的状态 -1：不存在，0：正常，1：暂停，2：完成，3：错误，4：阻塞--正在执行
	 * @param task
	 * @return
	 * @throws SchedulerException
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public int getTaskStat(T task) throws SchedulerException {
		TriggerKey triggerKey = TriggerKey.triggerKey(task.getTaskName(), task.getTaskGroupName());
		Scheduler scheduler = this.ssf.getScheduler(this.instanceName);
		TriggerState tmp = scheduler.getTriggerState(triggerKey);
		if (TriggerState.NONE.equals(tmp)) {
			return -1;
		}
		else if (Trigger.TriggerState.NORMAL.equals(tmp)) {
			return 0;
		}
		else if (Trigger.TriggerState.PAUSED.equals(tmp)) {
			return 1;
		}
		else if (Trigger.TriggerState.COMPLETE.equals(tmp)) {
			return 2;
		}
		else if (Trigger.TriggerState.ERROR.equals(tmp)) {
			return 3;
		}
		else if (Trigger.TriggerState.BLOCKED.equals(tmp)) {
			return 4;
		}
		else {
			return -2;
		}

	}

	/**
	 * 概述：判断任务是否正在执行
	 * @param task
	 * @return
	 * @throws SchedulerException
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public boolean isExecuting(T task) throws SchedulerException {
		JobKey jobKey = new JobKey(task.getTaskName(), task.getTaskGroupName());
		Scheduler scheduler = this.ssf.getScheduler(this.instanceName);
		JobKey currentJob = null;
		for (JobExecutionContext jobExecut : scheduler.getCurrentlyExecutingJobs()) {
			currentJob = jobExecut.getJobDetail().getKey();
			if (currentJob.equals(jobKey)) {
				return true;
			}
		}
		return false;
	}

	@Override
	public int getPoolThreadCount() throws Exception {
		// TODO Auto-generated method stub
		return this.poolSize;
	}

	@Override
	public int getTaskThreadCount() throws Exception {
		Scheduler scheduler = this.ssf.getScheduler(this.instanceName);
		int count = 0;
		for (String groupName : scheduler.getJobGroupNames()) {
			for (JobKey jobKey : scheduler.getJobKeys((GroupMatcher<JobKey>)GroupMatcher.groupEquals(groupName))) {
				count ++;
			}
		}
		return count;
	}

}
