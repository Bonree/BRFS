package com.bonree.brfs.schedulers.task.operation.impl;

import com.bonree.brfs.email.EmailPool;
import com.bonree.brfs.schedulers.ManagerContralFactory;
import com.bonree.brfs.schedulers.task.operation.QuartzOperationStateInterface;
import com.bonree.mail.worker.MailWorker;
import org.quartz.JobExecutionContext;
import org.quartz.UnableToInterruptJobException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class QuartzOperationStateTask implements QuartzOperationStateInterface {
    private Logger log;

    public QuartzOperationStateTask(Logger log) {
        this.log = log;
    }

    @Override
    public void execute(JobExecutionContext context) {
        try {
            operation(context);
        } catch (Exception e) {
            context.put("ExceptionMessage", e.getMessage());
            log.info("Run task error {}", e);
            EmailPool emailPool = EmailPool.getInstance();
            MailWorker.Builder builder = MailWorker.newBuilder(emailPool.getProgramInfo());
            builder.setModel(this.getClass().getSimpleName() + "模块服务发生问题");
            builder.setException(e);
            ManagerContralFactory mcf = ManagerContralFactory.getInstance();
            builder.setMessage(mcf.getGroupName() + "(" + mcf.getServerId() + ")服务 执行任务时发生问题");
            builder.setVariable(context.getMergedJobDataMap().getWrappedMap());
            emailPool.sendEmail(builder);
        }

    }

    @Override
    public abstract void interrupt() throws UnableToInterruptJobException;

    @Override
    public abstract void operation(JobExecutionContext context) throws Exception;

}
