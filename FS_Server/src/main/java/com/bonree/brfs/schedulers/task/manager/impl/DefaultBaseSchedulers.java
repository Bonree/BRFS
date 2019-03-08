
package com.bonree.brfs.schedulers.task.manager.impl;

import java.text.ParseException;
import java.util.Date;
import java.util.Map;
import java.util.Properties;

import org.quartz.CronScheduleBuilder;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.Trigger.TriggerState;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;
import org.quartz.UnableToInterruptJobException;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.impl.matchers.GroupMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.schedulers.exception.ParamsErrorException;
import com.bonree.brfs.schedulers.task.manager.BaseSchedulerInterface;
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
public class DefaultBaseSchedulers implements BaseSchedulerInterface {
	private static final Logger LOG = LoggerFactory.getLogger(DefaultBaseSchedulers.class);
	private StdSchedulerFactory ssf = new StdSchedulerFactory();
	private String instanceName = "server";
	private boolean pausePoolFlag = false;
	private int poolSize = 0;
	private Properties prop= null;

	@Override
	public void initProperties(Properties props) {
		if(props != null && prop !=null){
			prop.putAll(props);
		}else if (props == null && prop == null) {
			prop = new Properties();
			prop.put(StdSchedulerFactory.PROP_THREAD_POOL_CLASS, "org.quartz.simpl.SimpleThreadPool");
			prop.put("org.quartz.threadPool.threadCount", "3");
			prop.put("quartz.jobStore.misfireThreshold", "1000");
		} else if(prop == null){
			prop = new Properties();
			prop.putAll(props);
		}
		try {
			ssf.initialize(prop);
			Scheduler ssh = ssf.getScheduler();
			this.instanceName = ssh.getSchedulerName();
			this.poolSize = Integer.parseInt(prop.getProperty("org.quartz.threadPool.threadCount"));
		} catch (NumberFormatException | SchedulerException e) {
			LOG.error("{}",e);
		}
	}

	@Override
	public boolean addTask(SumbitTaskInterface task) throws ParamsErrorException {
		// 1.检查任务的有效性
		checkTask(task);
		// 2.线程池处于暂停时，不提交任务
		if (this.pausePoolFlag) {
			LOG.warn("Thread pool is paused !!!");
			return false;
		}
		try {
			Scheduler scheduler = this.ssf.getScheduler(this.instanceName);
			// 当线程池不处于运行时，将不添加任务
			if (!isNormal()) {
				LOG.warn("Thread pool is not normal");
				return false;
			}
			// 当线程池满了也不会添加任务
			if(this.poolSize <= getSumbitTaskCount()){
				LOG.warn("thread pool is full !!!");
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
				long interval = Long.parseLong(cycles[0]);
				int repeateCount = Integer.parseInt(cycles[1]);
				repeateCount = repeateCount -1;
				long delayTime = Long.parseLong(cycles[2]);
				boolean rightNow = Boolean.valueOf(cycles[3]);
				boolean cycleFlag = Boolean.valueOf(cycles[4]);
				SimpleScheduleBuilder builder = SimpleScheduleBuilder.simpleSchedule();
				builder.withIntervalInMilliseconds(interval);
				if (cycleFlag) {
					builder.repeatForever();
				}
				else if(repeateCount >=0) {
					builder.withRepeatCount(repeateCount);
				}else{
					LOG.warn("repeated count is zero !!!!");
					return false;
				}
				TriggerBuilder trigBuilder = TriggerBuilder.newTrigger().withIdentity(taskName, taskGroup).withSchedule(
					builder);
				if (!rightNow && delayTime > 0) {
					long current = System.currentTimeMillis() + delayTime;
					Date date = new Date(current);
					trigBuilder.startAt(date);
				}
				else {
					trigBuilder.startNow();
				}
				trigger = trigBuilder.build();
			}
			if (trigger == null || jobDetail == null) {
				LOG.warn(" create task message is null");
				return false;
			}
			scheduler.scheduleJob(jobDetail, trigger);
			return true;
		}
		catch (NumberFormatException | ClassNotFoundException | ParseException | SchedulerException e) {
			LOG.error("add task {}",e);
		}
		return false;
	}

	@Override
	public void start() throws RuntimeException {
		if(prop == null){
			throw new NullPointerException("configuration is  null ");
		}
		try {
			Scheduler scheduler = this.ssf.getScheduler(this.instanceName);
			if (!scheduler.isStarted()) {
				scheduler.start();
			}
		}
		catch (Exception e) {
			// TODO Auto-generated catch block
			LOG.error("start pool {}",e);
			throw new RuntimeException(this.instanceName + " start fail !!!");
		}
	}

	@Override
	public void close(boolean isWaitTaskComplete) throws RuntimeException {
		try {
			Scheduler scheduler = this.ssf.getScheduler(this.instanceName);
			if (scheduler.isShutdown()) {
				return;
			}
			scheduler.pauseAll();
			scheduler.clear();
			scheduler.shutdown(isWaitTaskComplete);
			this.pausePoolFlag = false;
		}
		catch (Exception e) {
			LOG.error("close pool {}",e);
			throw new RuntimeException(this.instanceName + " close fail !!!");
		}

	}

	@Override
	public boolean isStart() {
		try {
			Scheduler scheduler = this.ssf.getScheduler(this.instanceName);
			// 当调用scheduler.shutdown()时，相应的调度对象被销毁，故在判断isShutdown时，应先判断对象是否销毁了
			if (scheduler == null) {
				return false;
			}
			// 判断线程池被关闭了。
			if (scheduler.isShutdown()) {
				return false;
			}
			return scheduler.isStarted();
		}
		catch (Exception e) {
			LOG.error("get start status error {}",e);
		}
		return false;
	}

	@Override
	public boolean isDestory() {
		try {
			Scheduler scheduler = this.ssf.getScheduler(this.instanceName);
			if (scheduler == null) {
				return true;
			}
			return scheduler.isShutdown();
		}
		catch (SchedulerException e) {
			LOG.error("destory error {}",e);
		}
		return false;
	}

	@Override
	public boolean deleteTask(SumbitTaskInterface task) throws ParamsErrorException {
		checkTask(task);
		try {
			// 不在正常运行时，不进行删除任务操作
			if (!isNormal()) {
				return false;
			}
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
		catch (UnableToInterruptJobException e) {
			LOG.error("delete task error{}",e);
		}
		catch (SchedulerException e) {
			LOG.error("scheduler error {}",e);
		}
		return false;
	}

	@Override
	public boolean pauseTask(SumbitTaskInterface task) throws ParamsErrorException {
		checkTask(task);
		try {
			// 不在正常运行时，不进行任何操作
			if (!isNormal()) {
				return false;
			}
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
		} catch (SchedulerException e) {
			LOG.error("pause task error {}",e);
		}
		return false;
	}

	@Override
	public boolean resumeTask(SumbitTaskInterface task) throws ParamsErrorException {
		checkTask(task);
		try {
			// 不在正常运行时，不进行任何操作
			if (!isNormal()) {
				return false;
			}
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
		catch (SchedulerException e) {
			LOG.error("resume task error {}",e);
		}
		return false;
	}

	@Override
	public boolean pauseAllTask() {
		try {
			// 不在正常运行时，不进行任何操作
			if (!isNormal()) {
				return false;
			}
			Scheduler scheduler = this.ssf.getScheduler(this.instanceName);
			if (!scheduler.isShutdown()) {
				// 1.停止触发器
				scheduler.pauseAll();
				JobKey currentJob;
				// 2.中断所有执行的任务
				for (JobExecutionContext jobExecut : scheduler.getCurrentlyExecutingJobs()) {
					currentJob = jobExecut.getJobDetail().getKey();
					scheduler.interrupt(currentJob);
				}
			}
			return true;
		}
		catch (UnableToInterruptJobException e) {
			LOG.error("{}",e);
		}
		catch (SchedulerException e) {
			LOG.error("{}",e);
		}
		return false;
	}

	@Override
	public boolean resumeAllTask() {
		try {
			// 不在正常运行时，不进行任何操作
			if (!isNormal()) {
				return false;
			}
			Scheduler scheduler = this.ssf.getScheduler(this.instanceName);
			if (!scheduler.isShutdown()) {
				scheduler.resumeAll();
			}
			return true;
		}
		catch (SchedulerException e) {
			LOG.error("{}",e);
		}
		return false;
	}

	@Override
	public void checkTask(SumbitTaskInterface task) throws ParamsErrorException {
		if (task == null) {
			throw new ParamsErrorException("task is empty");
		}
		if (BrStringUtils.isEmpty(task.getClassInstanceName())) {
			throw new ParamsErrorException("task class instanceName is empty");
		}
		if (BrStringUtils.isEmpty(task.getTaskName())) {
			throw new ParamsErrorException("task name is empty");
		}
		if (BrStringUtils.isEmpty(task.getTaskGroupName())) {
			throw new ParamsErrorException("task group name is empty");
		}
		if (BrStringUtils.isEmpty(task.getCycleContent())) {
			throw new ParamsErrorException("task cycle is empty");
		}
	}

	@Override
	public String getInstanceName() {
		return this.instanceName;
	}

	@Override
	public boolean isPaused() {
		return pausePoolFlag;
	}

	/**
	 * 概述：获取任务的状态 -1：不存在，0：正常，1：暂停，2：完成，3：错误，4：阻塞--正在执行
	 * @param task
	 * @return
	 * @throws SchedulerException
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	@Override
	public int getTaskStat(SumbitTaskInterface task) throws ParamsErrorException {
		checkTask(task);
		try {
			TriggerKey triggerKey = TriggerKey.triggerKey(task.getTaskName(), task.getTaskGroupName());
			Scheduler scheduler = this.ssf.getScheduler(this.instanceName);
			if(scheduler == null){
				return -1;
			}
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
		catch (SchedulerException e) {
			LOG.error("get status error {}",e);
			throw new ParamsErrorException(e.getLocalizedMessage());
		}

	}

	/**
	 * 概述：判断任务是否正在执行
	 * @param task
	 * @return
	 * @throws SchedulerException
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	@Override
	public boolean isExecuting(SumbitTaskInterface task) throws ParamsErrorException {
		checkTask(task);
		return getTaskStat(task) == 4;
	}

	@Override
	public int getPoolSize() {
		return this.poolSize;
	}

	@Override
	public int getSumbitTaskCount() {
		try {
			Scheduler scheduler = this.ssf.getScheduler(this.instanceName);
			if(scheduler == null){
				return 0;
			}
			int count = 0;
			for (String groupName : scheduler.getJobGroupNames()) {
				count += scheduler.getJobKeys((GroupMatcher<JobKey>) GroupMatcher.groupEquals(groupName)).size();
			}
			return count;
		}
		catch (SchedulerException e) {
			LOG.error("{}",e);
		}
		return -1;
	}

	/**
	 * 概述：判断调度是否已经正常运行
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	private boolean isNormal() {
		Scheduler scheduler;
		try {
			scheduler = this.ssf.getScheduler(this.instanceName);
			// 当调用scheduler.shutdown()时，相应的调度对象被销毁，故在判断isShutdown时，应先判断对象是否销毁了
			if (scheduler == null) {
				return false;
			}
			// 判断线程池被关闭了。
			if (scheduler.isShutdown()) {
				return false;
			}
			// 判断线程池未启动
			if (!scheduler.isStarted()) {
				return false;
			}
			return true;
		}
		catch (SchedulerException e) {
			LOG.error("{}",e);
		}
		return false;
	}

	@Override
	public void pausePool() {
		this.pausePoolFlag = true;
	}

	@Override
	public void resumePool() {
		this.pausePoolFlag = false;
	}

	@Override
	public void reStart() throws RuntimeException, NullPointerException {
		// 配置文件为空抛异常
		if(prop == null){
			throw new NullPointerException("configuration is  null ");
		}
		// 程序未销毁抛异常
		if(!isDestory()){
			close(true);
		}
		initProperties(null);
		start();
	}
	/**
	 * 概述：创建简单的调度配置
	 * @param poolSize
	 * @param misfireTime
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static Properties createSimplePrope(int poolSize, long misfireTime) {
		Properties prop = new Properties();
		prop.put(StdSchedulerFactory.PROP_THREAD_POOL_CLASS, "org.quartz.simpl.SimpleThreadPool");
		prop.put("org.quartz.threadPool.threadCount", poolSize + "");
		prop.put("quartz.jobStore.misfireThreshold", misfireTime + "");
		return prop;
	}
}
