package com.bonree.brfs.schedulers.task.operation.impl;

import java.util.List;
import java.util.concurrent.TimeUnit;

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
import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.RegionNodeConfigs;
import com.bonree.brfs.schedulers.ManagerContralFactory;
import com.bonree.brfs.schedulers.jobs.JobDataMapConstract;
import com.bonree.brfs.schedulers.task.manager.MetaTaskManagerInterface;
import com.bonree.brfs.schedulers.task.model.TaskModel;
import com.bonree.brfs.schedulers.task.model.TaskResultModel;
import com.bonree.brfs.schedulers.task.model.TaskServerNodeModel;
import com.bonree.brfs.schedulers.task.operation.QuartzOperationStateInterface;

public abstract class QuartzOperationStateTask implements QuartzOperationStateInterface {
	private static final Logger LOG = LoggerFactory.getLogger(QuartzOperationStateTask.class);
	@Override
	public void execute(JobExecutionContext context) {
		try{
			operation(context);
		}catch(Exception e){
			context.put("ExceptionMessage", e.getMessage());
			caughtException(context);
			LOG.info("{}",e.toString());
		}
		
	}
	@Override
	public abstract  void caughtException(JobExecutionContext context);

	@Override
	public abstract void interrupt() throws UnableToInterruptJobException;

	@Override
	public abstract void operation(JobExecutionContext context) throws Exception;

}
