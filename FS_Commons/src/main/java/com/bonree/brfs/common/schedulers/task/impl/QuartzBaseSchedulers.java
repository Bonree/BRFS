package com.bonree.brfs.common.schedulers.task.impl;

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
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.SimpleTrigger;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.schedulers.model.TaskInterface;
import com.bonree.brfs.common.schedulers.task.QuartzSchedulerInterface;
import com.bonree.brfs.common.utils.StringUtils;
/******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年3月28日 下午4:21:21
 * @Author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @Description: quartz基础调度实现
 *****************************************************************************
 */
public class QuartzBaseSchedulers<T extends TaskInterface> implements QuartzSchedulerInterface<T>{
	private static final Logger logger = LoggerFactory.getLogger("CycleTest");
	private StdSchedulerFactory ssf = new StdSchedulerFactory();
	private String instanceName = "server";
	private boolean pausePoolFlag = false;
	
	@Override
	public void initProperties(Properties props) throws Exception {
		Properties tmpprops = null;
		if(props == null){
			tmpprops = new Properties();
			tmpprops.put(StdSchedulerFactory.PROP_THREAD_POOL_CLASS, "org.quartz.simpl.SimpleThreadPool");
			tmpprops.put("org.quartz.threadPool.threadCount", "3");
		}else{
			tmpprops = props;
		}
		tmpprops.put("org.quartz.scheduler.instanceName", instanceName);
		ssf.initialize(tmpprops);
		Scheduler ssh = ssf.getScheduler();
		this.instanceName = ssh.getSchedulerName();		
	}

	@Override
	public boolean addTask(T task) throws Exception {
		if(!checkTask(task)){
			return false;
		}
		// 1.设置job的名称及执行的class
		Class<? extends Job> clazz = (Class<? extends Job>) Class.forName(task.getClassInstanceName());
		String taskName = task.getTaskName();
		String taskGroup = task.getTaskGroupName();
		JobBuilder jobBuilder = JobBuilder.newJob(clazz).withIdentity(taskName, taskGroup);
		
		// 2.设置任务需要的数据
		Map<String,String> tmp = task.getTaskContent();
		if(tmp !=null && !tmp.isEmpty()){			
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
		if(taskType == 0){
			CronScheduleBuilder cronScheduleBuilder = CronScheduleBuilder.cronSchedule(cycleContent);
			trigger =  TriggerBuilder.newTrigger()
					.withIdentity(taskName, taskGroup)
					.withSchedule(cronScheduleBuilder)
					.build();
		}else if(taskType ==1){
			String[] cycles = StringUtils.getSplit(cycleContent, ",");
			if(cycles == null || cycles.length == 0){
				throw new NullPointerException("simple trigger cycle time is empty !!! content : "+cycleContent);
			}
			if(cycles.length != 2){
				throw new NullPointerException("simple trigger cycle time is error !!! content : "+cycleContent);
			}
			long interval = Long.valueOf(cycles[0]);
			int repeateCount = Integer.valueOf(cycles[1]);
			SimpleScheduleBuilder sSched = SimpleScheduleBuilder.simpleSchedule()
					.withIntervalInMilliseconds(interval)
					.withRepeatCount(repeateCount);
			trigger = TriggerBuilder.newTrigger()
					.withIdentity(taskName, taskGroup)
					.startNow()
					.withSchedule(sSched)
					.build();
		}
		if(trigger == null || jobDetail == null){
			return false;
		}
		Scheduler scheduler = this.ssf.getScheduler(this.instanceName);
		scheduler.scheduleJob(jobDetail, trigger);
		return true;
	}

	@Override
	public void start() throws Exception {
		Scheduler scheduler = this.ssf.getScheduler(this.instanceName);
		if(!scheduler.isStarted()){
			scheduler.start();
		}
	}

	@Override
	public void close(boolean isWaitTaskComplete) throws Exception {
		Scheduler scheduler = this.ssf.getScheduler(this.instanceName);
		if(scheduler.isShutdown()){
			return;
		}
		if(!isWaitTaskComplete){
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
		if(!scheduler.isShutdown()){
			// 1.停止触发器
			scheduler.pauseTrigger(triggerKey);
			// 2.移除触发器
			scheduler.unscheduleJob(triggerKey);
			// 3.删除任务
			scheduler.deleteJob(jobKey);// 删除任务 
		}
		return true;
	}

	@Override
	public boolean pauseTask(T task) throws Exception {
		TriggerKey triggerKey = TriggerKey.triggerKey(task.getTaskName(), task.getTaskGroupName());
		JobKey jobKey = new JobKey(task.getTaskName(), task.getTaskGroupName());
		Scheduler scheduler = this.ssf.getScheduler(this.instanceName);
		if(!scheduler.isShutdown()){
			// 1.停止触发器
			scheduler.pauseTrigger(triggerKey);
			// 2.停止任务
			scheduler.pauseJob(jobKey);
		}
		return true;
	}
	@Override
	public boolean resumeTask(T task) throws Exception {
		TriggerKey triggerKey = TriggerKey.triggerKey(task.getTaskName(), task.getTaskGroupName());
		JobKey jobKey = new JobKey(task.getTaskName(), task.getTaskGroupName());
		Scheduler scheduler = this.ssf.getScheduler(this.instanceName);
		if(!scheduler.isShutdown()){
			Set<String> pauseGroup = scheduler.getPausedTriggerGroups();
			if( pauseGroup == null || (pauseGroup != null && !pauseGroup.contains(task.getTaskGroupName()))){
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
		if(!scheduler.isShutdown() ){
			if(!this.pausePoolFlag){
				return false;
			}
			// 1.停止触发器
			scheduler.pauseAll();
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
		if(!scheduler.isShutdown()){
			Set<String> pauseGroup = scheduler.getPausedTriggerGroups();
			if( pauseGroup == null || pauseGroup.isEmpty()){
				return false;
			}
			scheduler.resumeAll();
		}
		return true;
	}

	@Override
	public boolean checkTask(T task) {
		if(StringUtils.isEmpty(task.getClassInstanceName())){
			return false;
		}
		if(StringUtils.isEmpty(task.getTaskName())){
			return false;
		}
		if(StringUtils.isEmpty(task.getTaskGroupName())){
			return false;
		}
		if(StringUtils.isEmpty(task.getCycleContent())){
			return false;
		}
		
		return true;
	}

	@Override
	public int getPoolStat() throws Exception {
		Scheduler scheduler = this.ssf.getScheduler(this.instanceName);
		if(scheduler.isShutdown()){
			return 1; //关闭
		}
		if(this.pausePoolFlag){
			return 2;//暂停
		}
		if(scheduler.isStarted()){
			return 3;
		}
		return 0; //正常
	}

	@Override
	public String getInstanceName() throws Exception {
		// TODO Auto-generated method stub
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

}
