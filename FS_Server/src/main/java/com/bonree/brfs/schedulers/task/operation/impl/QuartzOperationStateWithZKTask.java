package com.bonree.brfs.schedulers.task.operation.impl;

import java.util.List;

import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.UnableToInterruptJobException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.task.TaskState;
import com.bonree.brfs.common.task.TaskType;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.common.utils.Pair;
import com.bonree.brfs.schedulers.ManagerContralFactory;
import com.bonree.brfs.schedulers.jobs.JobDataMapConstract;
import com.bonree.brfs.schedulers.task.manager.MetaTaskManagerInterface;
import com.bonree.brfs.schedulers.task.model.TaskModel;
import com.bonree.brfs.schedulers.task.model.TaskResultModel;
import com.bonree.brfs.schedulers.task.model.TaskServerNodeModel;
import com.bonree.brfs.schedulers.task.operation.QuartzOperationStateInterface;

public abstract class QuartzOperationStateWithZKTask implements QuartzOperationStateInterface {
	private static final Logger LOG = LoggerFactory.getLogger(QuartzOperationStateWithZKTask.class);
	@Override
	public void execute(JobExecutionContext context) {
		int currentIndex = -1;
		JobDataMap data = null;
		String serverId = null;
		String taskTypeName = null;
		String taskName = null;
		int stat = -1;
		boolean isSuccess = true;
		try{
			data = context.getJobDetail().getJobDataMap();
			int repeatCount = data.getInt(JobDataMapConstract.TASK_REPEAT_RUN_COUNT);
			taskName = data.getString(JobDataMapConstract.TASK_NAME);
			serverId = data.getString(JobDataMapConstract.SERVER_ID);
			int taskType = data.getInt(JobDataMapConstract.TASK_TYPE);
			taskTypeName = TaskType.valueOf(taskType).name();
			// 设置当前任务执行
			if(!data.containsKey(JobDataMapConstract.CURRENT_INDEX)){
				data.put(JobDataMapConstract.CURRENT_INDEX, repeatCount + "");
				TaskStateLifeContral.updateTaskRunState(serverId, taskName, taskTypeName);
			}
			currentIndex = data.getInt(JobDataMapConstract.CURRENT_INDEX);
			operation(context);
			
		}catch(Exception e){
			context.put("ExceptionMessage", e.getMessage());
			caughtException(context);
			isSuccess = false;
			LOG.info("{}",e);
		}finally{
			if(data == null){
				return;
			}
			LOG.info("operation batch id {}",currentIndex);
			// 更新任务状态
			TaskResultModel resultTask = new TaskResultModel();
			resultTask.setSuccess(isSuccess);
			TaskStateLifeContral.updateMapTaskMessage(context, resultTask);
			// 更新要操作的批次
			if(currentIndex > 1){
				data.put(JobDataMapConstract.CURRENT_INDEX, (currentIndex-1)+"" );
			// 最后一次更新任务信息
			}else if(currentIndex == 1){
				String result = data.getString(JobDataMapConstract.TASK_RESULT);
				stat = data.getInt(JobDataMapConstract.TASK_MAP_STAT);
				TaskStateLifeContral.updateTaskStatusByCompelete(serverId, taskName, taskTypeName, result, stat);
				data.put(JobDataMapConstract.CURRENT_INDEX, (currentIndex-1)+"" );
			}
		}
		
	}
	@Override
	public abstract  void caughtException(JobExecutionContext context);

	@Override
	public abstract void interrupt() throws UnableToInterruptJobException;

	@Override
	public abstract void operation(JobExecutionContext context) throws Exception;

}
