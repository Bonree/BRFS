package com.bonree.brfs.schedulers.task.operation.impl;

import java.util.Map;

import com.bonree.brfs.email.EmailPool;
import com.bonree.brfs.schedulers.utils.JobDataMapConstract;
import com.bonree.brfs.schedulers.utils.TaskStateLifeContral;
import com.bonree.mail.worker.MailWorker;
import com.bonree.mail.worker.ProgramInfo;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.UnableToInterruptJobException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.task.TaskState;
import com.bonree.brfs.common.task.TaskType;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.common.utils.Pair;
import com.bonree.brfs.schedulers.ManagerContralFactory;
import com.bonree.brfs.schedulers.jobs.biz.BatchTaskFactory;
import com.bonree.brfs.schedulers.jobs.biz.WatchSomeThingJob;
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
		String currentTaskName = null;
		int batchIndex = 0;
		TaskType taskType = null;
		String serverId = null;
		JobDataMap data = context.getJobDetail().getJobDataMap();
		boolean isSuccess = true;
		try {
			// 获取当前的任务信息
			taskTypeCode = data.getInt(JobDataMapConstract.TASK_TYPE);
			taskType = TaskType.valueOf(taskTypeCode);
			
			batchSize = data.getInt(JobDataMapConstract.BATCH_SIZE);
					
			if(!data.containsKey(JobDataMapConstract.CURRENT_TASK_NAME)){
				data.put(JobDataMapConstract.CURRENT_TASK_NAME, "");
			}
			serverId = data.getString(JobDataMapConstract.SERVER_ID);
			currentTaskName = data.getString(JobDataMapConstract.CURRENT_TASK_NAME);
			if(!data.containsKey(JobDataMapConstract.CURRENT_INDEX)){
				data.put(JobDataMapConstract.CURRENT_INDEX, "0");
				LOG.info("task:{}-{} start",taskType.name(),currentTaskName);
			}
			batchIndex = data.getInt(JobDataMapConstract.CURRENT_INDEX);
			LOG.debug("current :{}, batchId : {}", currentTaskName, batchIndex);
			if(batchSize == 0){
				batchSize = 10;
			}
			if(batchIndex >=1){
				operation(context);
			}
		}catch (Exception e) {
			LOG.info("happend Exception :{}",e);
			context.put("ExceptionMessage", e.getMessage());
			caughtException(context);
			isSuccess = false;
			EmailPool emailPool = EmailPool.getInstance();
			MailWorker.Builder builder = MailWorker.newBuilder(emailPool.getProgramInfo());
			builder.setModel(this.getClass().getSimpleName()+"模块服务发生问题");
			builder.setException(e);
			ManagerContralFactory mcf = ManagerContralFactory.getInstance();
			builder.setMessage(mcf.getGroupName()+"("+mcf.getServerId()+")服务 执行任务时发生问题");
			builder.setVariable(data.getWrappedMap());
			emailPool.sendEmail(builder);
		}finally{
			//判断是否有恢复任务，有恢复任务则不进行创建
			if(WatchSomeThingJob.getState(WatchSomeThingJob.RECOVERY_STATUSE)){
				LOG.warn("rebalance task is running !! skip check copy task");
				return;
			}
			if(batchIndex >= 1){
				LOG.debug("batch ID :{} {} {} {} {}",batchIndex,taskType,currentTaskName,serverId,isSuccess ? TaskState.RUN :TaskState.EXCEPTION);
				TaskResultModel resultTask = new TaskResultModel();
				resultTask.setSuccess(isSuccess);
				TaskStateLifeContral.updateMapTaskMessage(context, resultTask);
			}
			//最后一次执行更新任务状态并处理任务
			if(batchIndex == 1){
				String result = data.getString(JobDataMapConstract.TASK_RESULT);
				TaskResultModel tResult = new TaskResultModel(); 
				if(!BrStringUtils.isEmpty(result)) {
					tResult = JsonUtils.toObjectQuietly(result, TaskResultModel.class);
				}
				TaskState state = isSuccess&& tResult.isSuccess() ? TaskState.FINISH :TaskState.EXCEPTION;
				LOG.info("task:{}-{}-{}  end !",taskType.name(),currentTaskName,state.name());
				LOG.debug("batch ID :{} {} {} {} {}",batchIndex,taskType,currentTaskName,serverId,state);
				TaskStateLifeContral.updateTaskStatusByCompelete(serverId, currentTaskName, taskType.name(), tResult);
				data.put(JobDataMapConstract.CURRENT_INDEX, (batchIndex-1)+"" );
				data.put(JobDataMapConstract.TASK_RESULT, "");
				data.put(JobDataMapConstract.CURRENT_TASK_NAME, "");
				data.put(JobDataMapConstract.CURRENT_INDEX, (batchIndex-1)+"" );

			}else if(batchIndex <=0){
				ManagerContralFactory mcf = ManagerContralFactory.getInstance();
				MetaTaskManagerInterface release = mcf.getTm();
				//创建任务
				createBatchData(release, data, serverId, taskType, batchSize, 3);
			}else{
				//更新任务状态
				data.put(JobDataMapConstract.CURRENT_INDEX, (batchIndex-1)+"" );
			}
			
		}
	}
	
	public void createBatchData(MetaTaskManagerInterface release,JobDataMap data,String serverId, TaskType taskType, int batchSize, int limitCount){
		// 从zk获取任务信息最后一次执行成功的  若任务为空则返回
		Pair<String,TaskModel> taskPair = TaskStateLifeContral.getCurrentOperationTask(release, taskType.name(), serverId, limitCount);
		if(taskPair == null){
			LOG.info("{} task queue is empty !!!",taskType.name());
			return;
		}
		// 将当前的任务分成批次执行
		TaskModel task = TaskStateLifeContral.changeRunTaskModel(taskPair.getSecond());
		String currentTaskName = taskPair.getFirst();
		if(BrStringUtils.isEmpty(currentTaskName)){
			LOG.info("{} {} task behind is empty !!!",taskType.name());
			return ;
		}
		Map<String,String> batchDatas = BatchTaskFactory.createBatch(task, batchSize);
		// 若批次为空则更新任务状态
		if(batchDatas == null || batchDatas.isEmpty()){
			LOG.info("batch data is empty !! update task :{} {}",taskType.name(),currentTaskName);
			TaskStateLifeContral.updateTaskStatusByCompelete(serverId, currentTaskName, taskType.name(), new TaskResultModel());
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
