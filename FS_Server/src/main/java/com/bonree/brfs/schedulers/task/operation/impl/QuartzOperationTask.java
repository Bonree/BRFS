package com.bonree.brfs.schedulers.task.operation.impl;

import org.quartz.JobExecutionContext;
import org.quartz.UnableToInterruptJobException;

import com.bonree.brfs.schedulers.task.operation.QuartzOperationInterface;


public abstract class QuartzOperationTask implements QuartzOperationInterface {

	@Override
	public void execute(JobExecutionContext context) {
		try{
			operation(context);
		}catch(Exception e){
			context.put("ExceptionMessage", e.getMessage());
			caughtException(context);
		}
		
	}

	@Override
	public abstract  void caughtException(JobExecutionContext context);

	@Override
	public abstract void interrupt() throws UnableToInterruptJobException;

	@Override
	public abstract void operation(JobExecutionContext context) throws Exception;

}
