package com.bonree.brfs.schedulers.task.operation.impl;


import com.bonree.brfs.email.EmailPool;
import com.bonree.mail.worker.MailWorker;
import com.bonree.mail.worker.ProgramInfo;
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
			LOG.info("Run task error {}",e);
			EmailPool emailPool = EmailPool.getInstance();
			MailWorker.Builder builder = MailWorker.newBuilder(emailPool.getProgramInfo());
			builder.setModel(this.getClass().getSimpleName()+"模块服务发生问题");
			builder.setException(e);
			builder.setMessage("执行发生错误");
			builder.setVariable(context.getMergedJobDataMap().getWrappedMap());
			emailPool.sendEmail(builder);
		}
		
	}
	@Override
	public abstract  void caughtException(JobExecutionContext context);

	@Override
	public abstract void interrupt() throws UnableToInterruptJobException;

	@Override
	public abstract void operation(JobExecutionContext context) throws Exception;

}
