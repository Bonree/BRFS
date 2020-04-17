package com.bonree.brfs.schedulers.jobs.biz;

import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.UnableToInterruptJobException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.schedulers.task.model.TaskResultModel;
import com.bonree.brfs.schedulers.task.operation.impl.CycleJobWithZKTask;
import com.bonree.brfs.schedulers.utils.CopyRecovery;
import com.bonree.brfs.schedulers.utils.JobDataMapConstract;
import com.bonree.brfs.schedulers.utils.TaskStateLifeContral;

public class CopyRecoveryJob extends CycleJobWithZKTask {
	private static final Logger LOG = LoggerFactory.getLogger(CopyRecoveryJob.class);
	@Override
	public void operation(JobExecutionContext context){

		JobDataMap data = context.getJobDetail().getJobDataMap();
		String currentIndex = data.getString(JobDataMapConstract.CURRENT_INDEX);
		String baseRoutPath = data.getString(JobDataMapConstract.BASE_ROUTE_PATH);
		String dataPath = data.getString(JobDataMapConstract.DATA_PATH);
		TaskResultModel result = null;
		String content = data.getString(currentIndex);
		// todo :  缺少partitionid 需要补充 。。。。。。。。。。。。。。。。。。。。。。。。。。
		String partitionId = "40";
		result = CopyRecovery.recoveryDirs(content,dataPath,partitionId);

		LOG.debug("CURRENT_INDEX: {} status:{}",currentIndex, result.isSuccess());
		TaskStateLifeContral.updateMapTaskMessage(context, result);
	}

	@Override
	public void caughtException(JobExecutionContext context) {

	}

	@Override
	public void interrupt(){

	}

}
