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
	public void operation(JobExecutionContext context) throws Exception {
		
		JobDataMap data = context.getJobDetail().getJobDataMap();
		String currentIndex = data.getString(JobDataMapConstract.CURRENT_INDEX);
		String zkHosts = data.getString(JobDataMapConstract.ZOOKEEPER_ADDRESS);
		String baseRoutPath = data.getString(JobDataMapConstract.BASE_ROUTE_PATH);
		String dataPath = data.getString(JobDataMapConstract.DATA_PATH);
		TaskResultModel result = null;
		String content = data.getString(currentIndex);
		result = CopyRecovery.recoveryDirs(content,zkHosts, baseRoutPath,dataPath);
		LOG.info("CURRENT_INDEX: {} status:{}",currentIndex, result.isSuccess());
		TaskStateLifeContral.updateMapTaskMessage(context, result);
	}

	@Override
	public void caughtException(JobExecutionContext context) {

	}

	@Override
	public void interrupt(){

	}

}
