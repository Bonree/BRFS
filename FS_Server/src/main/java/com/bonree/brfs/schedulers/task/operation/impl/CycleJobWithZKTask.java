package com.bonree.brfs.schedulers.task.operation.impl;

import java.util.Map;

import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.UnableToInterruptJobException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.task.TaskState;
import com.bonree.brfs.common.task.TaskType;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.utils.Pair;
import com.bonree.brfs.schedulers.ManagerContralFactory;
import com.bonree.brfs.schedulers.jobs.JobDataMapConstract;
import com.bonree.brfs.schedulers.jobs.biz.BatchTaskFactory;
import com.bonree.brfs.schedulers.task.manager.MetaTaskManagerInterface;
import com.bonree.brfs.schedulers.task.model.TaskModel;
import com.bonree.brfs.schedulers.task.model.TaskResultModel;
import com.bonree.brfs.schedulers.task.operation.QuartzOperationStateInterface;

public abstract class CycleJobWithZKTask implements QuartzOperationStateInterface {
	private static final Logger LOG = LoggerFactory.getLogger("CycleJobWithZKTask");
	@Override
	public abstract void operation(JobExecutionContext context) throws Exception;

	@Override
	public abstract void caughtException(JobExecutionContext context);

	@Override
	public abstract void interrupt() throws UnableToInterruptJobException;

	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		int taskTypeCode = 0;
		int batchSize = 10;
		String prexName = null;
		String currentTaskName = null;
		int batchIndex = 0;
		int stat = -1;
		TaskType taskType = null;
		String serverId = null;
		JobDataMap data = context.getJobDetail().getJobDataMap();
		boolean isSuccess = true;
		try {
			// 获取当前的任务信息
			taskTypeCode = data.getInt(JobDataMapConstract.TASK_TYPE);
			taskType = TaskType.valueOf(taskTypeCode);
			
			batchSize = data.getInt(JobDataMapConstract.BATCH_SIZE);
					
			if(!data.containsKey(JobDataMapConstract.PREX_TASK_NAME)){
				data.put(JobDataMapConstract.PREX_TASK_NAME, "");
			}
			prexName = data.getString(JobDataMapConstract.PREX_TASK_NAME);
			
			if(!data.containsKey(JobDataMapConstract.CURRENT_TASK_NAME)){
				data.put(JobDataMapConstract.CURRENT_TASK_NAME, "");
			}
			serverId = data.getString(JobDataMapConstract.SERVER_ID);
			currentTaskName = data.getString(JobDataMapConstract.CURRENT_TASK_NAME);
			if(!data.containsKey(JobDataMapConstract.CURRENT_INDEX)){
				data.put(JobDataMapConstract.CURRENT_INDEX, "0");
			}
			batchIndex = data.getInt(JobDataMapConstract.CURRENT_INDEX);
			LOG.info("current :{}, batchId : {}, prex:{}", currentTaskName, batchIndex, prexName);
			if(batchSize == 0){
				batchSize = 10;
			}
			if(batchIndex >=1){
				LOG.info(">>>>>>>>> work>>>work");
				operation(context);
			}
		}catch (Exception e) {
			LOG.info("------------- Exception");
			context.put("ExceptionMessage", e.getMessage());
			caughtException(context);
			isSuccess = false;
			e.printStackTrace();
		}finally{

			LOG.info("batchId {}", batchIndex);
			if(batchIndex >= 1){
				LOG.info("------------------- work>>>work");
				TaskResultModel resultTask = new TaskResultModel();
				resultTask.setSuccess(isSuccess);
				TaskStateLifeContral.updateMapTaskMessage(context, resultTask);
			}
			//最后一次执行更新任务状态并处理任务
			if(batchIndex == 1){
				LOG.info("------------------- work>>>work");
				String result = data.getString(JobDataMapConstract.TASK_RESULT);
				stat = data.getInt(JobDataMapConstract.TASK_MAP_STAT);
				LOG.info("batch ID :{} {} {} {} {}",batchIndex,taskType,currentTaskName,serverId,stat);
				TaskStateLifeContral.updateTaskStatusByCompelete(serverId, currentTaskName, taskType.name(), result, stat);
				data.put(JobDataMapConstract.CURRENT_INDEX, (batchIndex-1)+"" );
				data.put(JobDataMapConstract.TASK_MAP_STAT, TaskState.INIT.code());
				data.put(JobDataMapConstract.TASK_RESULT, "");
				data.put(JobDataMapConstract.PREX_TASK_NAME, currentTaskName);
				data.put(JobDataMapConstract.CURRENT_TASK_NAME, "");
				LOG.info("work------------  batchId:{}",batchIndex);
				data.put(JobDataMapConstract.CURRENT_INDEX, (batchIndex-1)+"" );
			}else if(batchIndex <=0){
				ManagerContralFactory mcf = ManagerContralFactory.getInstance();
				MetaTaskManagerInterface release = mcf.getTm();
				//创建任务
				createBatchData(release, data, serverId, taskType, prexName, batchSize);
			}else{
				//更新任务状态
				LOG.info(">>>>>>>>>>  batchId:{}",batchIndex);
				data.put(JobDataMapConstract.CURRENT_INDEX, (batchIndex-1)+"" );
			}
			
		}
	}
	
	public void createBatchData(MetaTaskManagerInterface release,JobDataMap data,String serverId, TaskType taskType, String prexName, int batchSize){
		// 从zk获取任务信息最后一次执行成功的  若任务为空则返回
		Pair<String,TaskModel> taskPair = TaskStateLifeContral.getTaskModel(release,taskType, prexName, serverId);
		if(taskPair == null){
			LOG.info("{} task queue is empty !!!",taskType.name());
			return;
		}
		// 将当前的任务分成批次执行
		TaskModel task = taskPair.getValue();
		String currentTaskName = taskPair.getKey();
		if(BrStringUtils.isEmpty(currentTaskName)){
			LOG.info("{} {} task behind is empty !!!",taskType.name(),prexName);
			return ;
		}
		Map<String,String> batchDatas = BatchTaskFactory.createBatch(release, task, currentTaskName, serverId, batchSize);
		// 若批次为空则更新任务状态
		if(batchDatas == null || batchDatas.isEmpty()){
			LOG.info("batch data is empty !! update task :{} {}",taskType.name(),currentTaskName);
			TaskStateLifeContral.updateTaskStatusByCompelete(serverId, currentTaskName, taskType.name(), "", TaskState.FINISH.code());
			data.put(JobDataMapConstract.PREX_TASK_NAME, currentTaskName);
			data.put(JobDataMapConstract.CURRENT_TASK_NAME, "");
			data.put(JobDataMapConstract.CURRENT_INDEX, 0+"");
			return;
		}
		data.putAll(batchDatas);
		data.put(JobDataMapConstract.CURRENT_TASK_NAME, currentTaskName);
		
		//更新zk任务状态
		TaskStateLifeContral.updateTaskRunState(serverId, currentTaskName, taskType.name());
	}

}
