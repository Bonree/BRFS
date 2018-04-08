package com.bonree.brfs.resourceschedule.service.impl;

import java.util.Map;

import com.bonree.brfs.common.schedulers.model.TaskContent;
import com.bonree.brfs.common.schedulers.model.TaskInterface;
import com.bonree.brfs.common.schedulers.task.SchedulerManagerInterface;
import com.bonree.brfs.common.schedulers.task.TaskType;
import com.bonree.brfs.common.schedulers.task.impl.QuartzBaseSchedulers;
import com.bonree.brfs.resourceschedule.model.StatServerModel;
import com.bonree.brfs.resourceschedule.model.TaskExecutablePattern;
import com.bonree.brfs.resourceschedule.model.TaskRunPattern;
import com.bonree.brfs.resourceschedule.service.RunnableTaskInterface;

public class DefaultRunnableTask implements RunnableTaskInterface {
	private long updateTime = 0;
	private StatServerModel stat = null;
	private TaskExecutablePattern  limit= null;
	private Map<Integer,Integer> taskLevelMap = null;
	private final static int REPEAT_COUNT [] = {1,2,4,8,10};
	private final static long INTERVAL_TIME[] = {1000L, 2000L, 4000L, 8000L, 10000L};
	private DefaultRunnableTask(){
		
	}
	private static class simpleInstance{
		public static  DefaultRunnableTask  instance = new DefaultRunnableTask();
	}
	public static DefaultRunnableTask getInstance(){
		return simpleInstance.instance;
	}
	
	@Override
	public void update(StatServerModel resources) {
		this.stat = resources;
	}

	@Override
	public long getLastUpdateTime() {
		// TODO Auto-generated method stub
		return this.updateTime;
	}

	@Override
	public void setLimitParameter(TaskExecutablePattern limits) {
		this.limit = limits;
	}
	@Override
	public void setTaskLevel(Map<Integer, Integer> taskLevel) {
		this.taskLevelMap = taskLevel;
	}
	@Override
	public TaskRunPattern taskRunnPattern(TaskContent task) throws Exception {
		TaskRunPattern runPattern = new TaskRunPattern();
		int type = task.getTaskType();
		if(TaskType.SYSTEM_DELETE.code() == type || TaskType.USER_DELETE.code() == type){
			runPattern.setRepeateCount(REPEAT_COUNT[0]);
			runPattern.setSleepTime(INTERVAL_TIME[0]);
		}else{
			runPattern.setRepeateCount(REPEAT_COUNT[REPEAT_COUNT.length -1]);
			runPattern.setSleepTime(INTERVAL_TIME[INTERVAL_TIME.length -1]);
		}
		return runPattern;
	}

	@Override
	public <String, QuartzBaseSchedulers, TaskInterface> boolean taskRunnable(int taskType, SchedulerManagerInterface<String, QuartzBaseSchedulers, TaskInterface> taskManager) throws Exception {
		if(taskManager == null){
			return false;
		}
		int taskCount = taskManager.getRunningTaskCount((String) Integer.valueOf(taskType).toString());
		if(taskCount < 0){
			return false;
		}
		int poolSize =  taskManager.getTaskPoolThreadCount((String) Integer.valueOf(taskType).toString());
		if(poolSize <=0 ||poolSize <= taskCount){
			return false;
		}
		if(TaskType.SYSTEM_DELETE.code() == taskType || TaskType.USER_DELETE.code() == taskType){
			return true;
		}
		if(TaskType.SYSTEM_CHECK.code() == taskType){
			if(stat.getMemoryRate() < limit.getMemoryRate()){
				return true;
			}else{
				return false;
			}
		}
		if(TaskType.SYSTEM_MERGER.code() == taskType){
			if(stat.getMemoryRate() < limit.getMemoryRate()){
				return true;
			}else{
				return false;
			}
		}
		return false;
	}

}
