package com.bonree.brfs.schedulers.task.operation.impl;

import com.bonree.brfs.common.task.TaskType;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.email.EmailPool;
import com.bonree.brfs.schedulers.ManagerContralFactory;
import com.bonree.brfs.schedulers.task.model.TaskResultModel;
import com.bonree.brfs.schedulers.task.operation.QuartzOperationStateInterface;
import com.bonree.brfs.schedulers.utils.JobDataMapConstract;
import com.bonree.brfs.schedulers.utils.TaskStateLifeContral;
import com.bonree.mail.worker.MailWorker;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.UnableToInterruptJobException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class QuartzOperationStateWithZKTask implements QuartzOperationStateInterface {
    private Logger log;

    public QuartzOperationStateWithZKTask(Logger log) {
        this.log = log;
    }

    @Override
    public void execute(JobExecutionContext context) {
        int currentIndex = -1;
        JobDataMap data = null;
        String serverId = null;
        String taskTypeName = null;
        String taskName = null;
        boolean isSuccess = true;
        try {
            data = context.getJobDetail().getJobDataMap();
            int repeatCount = data.getInt(JobDataMapConstract.TASK_REPEAT_RUN_COUNT);
            taskName = data.getString(JobDataMapConstract.TASK_NAME);
            serverId = data.getString(JobDataMapConstract.SERVER_ID);
            int taskType = data.getInt(JobDataMapConstract.TASK_TYPE);
            taskTypeName = TaskType.valueOf(taskType).name();
            // 设置当前任务执行
            if (!data.containsKey(JobDataMapConstract.CURRENT_INDEX)) {
                data.put(JobDataMapConstract.CURRENT_INDEX, repeatCount + "");
                TaskStateLifeContral.updateTaskRunState(serverId, taskName, taskTypeName);
                log.info("task {}-{} run", taskTypeName, taskName);
            }
            currentIndex = data.getInt(JobDataMapConstract.CURRENT_INDEX);
            log.debug("taskType [{}],taskname [{}],batch id[{}], data :[{}]", taskTypeName, taskName, currentIndex,
                      data.getString(currentIndex + ""));
            operation(context);

        } catch (Exception e) {
            context.put("ExceptionMessage", e.getMessage());
            isSuccess = false;
            log.error("task {}-{} happen exception:{}", taskTypeName, taskName, e);
            EmailPool emailPool = EmailPool.getInstance();
            MailWorker.Builder builder = MailWorker.newBuilder(emailPool.getProgramInfo());
            builder.setModel(this.getClass().getSimpleName() + " execute 模块服务发生问题");
            builder.setException(e);
            ManagerContralFactory mcf = ManagerContralFactory.getInstance();
            builder.setMessage(mcf.getGroupName() + "(" + mcf.getServerId() + ")服务 执行任务时发生问题");
            builder.setVariable(data.getWrappedMap());
            emailPool.sendEmail(builder);
        } finally {
            if (data == null) {
                return;
            }
            log.debug("operation batch id {}", currentIndex);
            try {
                // 更新任务状态
                TaskResultModel resultTask = new TaskResultModel();
                resultTask.setSuccess(isSuccess);
                TaskStateLifeContral.updateMapTaskMessage(context, resultTask);
                // 更新要操作的批次
                if (currentIndex > 1) {
                    data.put(JobDataMapConstract.CURRENT_INDEX, (currentIndex - 1) + "");
                    // 最后一次更新任务信息
                } else if (currentIndex == 1) {
                    String result = data.getString(JobDataMapConstract.TASK_RESULT);
                    TaskResultModel resultModel = new TaskResultModel();
                    if (!BrStringUtils.isEmpty(result)) {
                        resultModel = JsonUtils.toObjectQuietly(result, TaskResultModel.class);
                    }
                    TaskStateLifeContral.updateTaskStatusByCompelete(serverId, taskName, taskTypeName, resultModel);
                    data.put(JobDataMapConstract.CURRENT_INDEX, (currentIndex - 1) + "");
                    log.info("task {}-{}:{} end!!", taskTypeName, taskName, resultModel.isSuccess());
                }
            } catch (Exception e) {
                log.error("execute error", e);
                EmailPool emailPool = EmailPool.getInstance();
                MailWorker.Builder builder = MailWorker.newBuilder(emailPool.getProgramInfo());
                builder.setModel(this.getClass().getSimpleName() + "模块服务发生问题");
                builder.setException(e);
                builder.setMessage("更新任务发生错误");
                builder.setVariable(data.getWrappedMap());
                emailPool.sendEmail(builder);
            }
        }

    }

    @Override
    public abstract void interrupt() throws UnableToInterruptJobException;

    @Override
    public abstract void operation(JobExecutionContext context) throws Exception;

}
