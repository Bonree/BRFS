package com.bonree.brfs.schedulers.task.operation.impl;

import org.quartz.JobExecutionContext;
import org.quartz.UnableToInterruptJobException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
			LOG.info("{}",e);
		}
		
	}

	@Override
	public abstract  void caughtException(JobExecutionContext context);

	@Override
	public abstract void interrupt() throws UnableToInterruptJobException;

	@Override
	public abstract void operation(JobExecutionContext context) throws Exception;

}
