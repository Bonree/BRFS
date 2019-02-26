package com.bonree.brfs.schedulers.task.manager.impl;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.task.TaskType;
import com.bonree.brfs.resourceschedule.model.StatServerModel;
import com.bonree.brfs.schedulers.task.manager.RunnableTaskInterface;
import com.bonree.brfs.schedulers.task.manager.SchedulerManagerInterface;
import com.bonree.brfs.schedulers.task.model.TaskExecutablePattern;
import com.bonree.brfs.schedulers.task.model.TaskModel;
import com.bonree.brfs.schedulers.task.model.TaskRunPattern;

public class DefaultRunnableTask implements RunnableTaskInterface {
	private static final Logger LOG = LoggerFactory.getLogger("DefaultRunnableTask");
	private long updateTime = 0;
	private StatServerModel stat = null;
	private TaskExecutablePattern  limit= null;
//	private Map<Integer,Integer> taskLevelMap = null;

	private final static int batchCount = 5;
	private final static long batchSleepTime = 5000L;
	private final static int maxbatchTimes = 10;
	private final static long maxBatchSleepTime = 30000L;
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
	}
	@Override
	public TaskRunPattern taskRunnPattern(TaskModel task) throws Exception {
		TaskRunPattern runPattern = new TaskRunPattern();
		int dataSize = task.getAtomList().size();
		int repeadCount = 1;
		repeadCount = ( dataSize % batchCount == 0 ) ? dataSize/batchCount :(dataSize/batchCount + 1);
		repeadCount = repeadCount > maxbatchTimes ? maxbatchTimes : repeadCount <= 0 ? 1 : repeadCount;
		long sleepTime = task.getTaskType() * batchSleepTime > maxBatchSleepTime ? maxBatchSleepTime : task.getTaskType() * batchSleepTime;
		sleepTime = sleepTime == 0 ? batchSleepTime :sleepTime;
		runPattern.setRepeateCount(repeadCount);
		runPattern.setSleepTime(sleepTime);
		return runPattern;
	}

	@Override
	public boolean taskRunnable(int taskType, int poolSize, int threadCount) throws Exception {
		
		int taskCount = threadCount;
		if(taskCount < 0){
			LOG.warn("there is no thread in the {} pool ",taskType);
			return false;
		}
		if(poolSize <=0 ||poolSize <= taskCount){
			LOG.warn("task pool size is full !!! pool size :{}, thread count :{}",poolSize,threadCount);
			return false;
		}
		if(TaskType.SYSTEM_DELETE.code() == taskType || TaskType.USER_DELETE.code() == taskType){
			return true;
		}
		if(stat == null){
			LOG.warn("Runnable's state is empty");
			return true;
		}
		if(limit == null){
			LOG.warn("Runnable's limit state is empty");
			return true;
		}
		if(TaskType.SYSTEM_CHECK.code() == taskType){
			return stat.getMemoryRate() < limit.getMemoryRate();
		}
		if(TaskType.SYSTEM_MERGER.code() == taskType){
			return stat.getMemoryRate() < limit.getMemoryRate();
		}
		return true;
	}

}
