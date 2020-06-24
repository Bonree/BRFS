package com.bonree.brfs.schedulers.jobs.system;

import com.bonree.brfs.common.task.TaskType;
import com.bonree.brfs.common.utils.JsonUtils.JsonException;
import com.bonree.brfs.common.utils.Pair;
import com.bonree.brfs.email.EmailPool;
import com.bonree.brfs.schedulers.ManagerContralFactory;
import com.bonree.brfs.schedulers.jobs.biz.SystemCheckJob;
import com.bonree.brfs.schedulers.jobs.biz.SystemDeleteJob;
import com.bonree.brfs.schedulers.jobs.biz.UserDeleteJob;
import com.bonree.brfs.schedulers.task.manager.MetaTaskManagerInterface;
import com.bonree.brfs.schedulers.task.manager.SchedulerManagerInterface;
import com.bonree.brfs.schedulers.task.meta.SumbitTaskInterface;
import com.bonree.brfs.schedulers.task.meta.impl.QuartzSimpleInfo;
import com.bonree.brfs.schedulers.task.model.TaskModel;
import com.bonree.brfs.schedulers.task.operation.impl.QuartzOperationStateTask;
import com.bonree.brfs.schedulers.utils.JobDataMapConstract;
import com.bonree.brfs.schedulers.utils.TaskStateLifeContral;
import com.bonree.brfs.tasks.worker.VirtaulRecoveryTask;
import com.bonree.mail.worker.MailWorker;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OperationTaskJob extends QuartzOperationStateTask {
    private static final Logger LOG = LoggerFactory.getLogger(OperationTaskJob.class);
    private static final ExecutorService pool =
        Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder().setNameFormat(TaskType.VIRTUAL_ID_RECOVERY.name()).build());

    @Override
    public void interrupt() {
    }

    @Override
    public void operation(JobExecutionContext context) {
        ManagerContralFactory mcf = ManagerContralFactory.getInstance();
        MetaTaskManagerInterface release = mcf.getTm();
        if (release == null) {
            throw new NullPointerException("MetaTaskManager is empty !!!");
        }
        Collection<TaskType> switchList = mcf.getTaskConfig().getTaskTypeSwitch();
        if (switchList == null || switchList.isEmpty()) {
            LOG.warn("switch task is empty !!!");
            return;
        }
        SchedulerManagerInterface schd = mcf.getStm();
        if (schd == null) {
            throw new NullPointerException("SchedulerManagerInterface is empty !!!");
        }

        String typeName;
        String currentTaskName;
        TaskModel task;
        int poolSize;
        int sumbitSize;
        String serverId = mcf.getServerId();
        //判断是否有恢复任务，有恢复任务则不进行创建
        boolean rebalanceFlag = mcf.getTaskMonitor().isExecute();
        for (TaskType taskType : switchList) {
            SumbitTaskInterface sumbitTask = null;
            try {
                if (TaskType.SYSTEM_COPY_CHECK.equals(taskType)) {
                    continue;
                }
                typeName = taskType.name();
                //判断任务是否可以执行
                if (!TaskType.VIRTUAL_ID_RECOVERY.equals(taskType)) {
                    poolSize = schd.getTaskPoolSize(typeName);
                    sumbitSize = schd.getSumbitedTaskCount(typeName);
                    if (!taskRunnable(taskType.code(), poolSize, sumbitSize)) {
                        LOG.warn("resource is limit !!! skip {} !!!", typeName);
                        continue;
                    }
                }

                int retryCount = 3;
                if (TaskType.SYSTEM_CHECK.equals(taskType) || TaskType.SYSTEM_MERGER.equals(taskType)
                    || TaskType.SYSTEM_DELETE.equals(taskType)) {
                    retryCount = 0;
                }
                Pair<String, TaskModel> taskPair =
                    TaskStateLifeContral.getCurrentOperationTask(release, typeName, serverId, retryCount);
                if (taskPair == null) {
                    LOG.debug("taskType :{} taskName: null is vaild ,skiping !!!", typeName);
                    continue;
                }
                currentTaskName = taskPair.getFirst();

                task = TaskStateLifeContral.changeRunTaskModel(taskPair.getSecond(), mcf.getDaemon());
                // 创建任务提交信息
                if (TaskType.SYSTEM_DELETE.equals(taskType)) {
                    sumbitTask = createSimpleTask(task, currentTaskName, mcf.getServerId(),
                                                  SystemDeleteJob.class.getCanonicalName(), 1, 1000);
                } else if (TaskType.SYSTEM_CHECK.equals(taskType) && !rebalanceFlag) {
                    sumbitTask = createSimpleTask(task, currentTaskName, mcf.getServerId(),
                                                  SystemCheckJob.class.getCanonicalName(), 10, 1000);
                } else if (TaskType.USER_DELETE.equals(taskType)) {
                    sumbitTask = createSimpleTask(task, currentTaskName, mcf.getServerId(),
                                                  UserDeleteJob.class.getCanonicalName(), 1, 1000);
                } else if (TaskType.VIRTUAL_ID_RECOVERY.equals(taskType)) {
                    pool.execute(new VirtaulRecoveryTask(currentTaskName));
                    continue;
                } else {
                    if (rebalanceFlag) {
                        LOG.warn("rebalance task running !! Skip {} sumbit", taskType);
                    }
                    continue;
                }
                if (sumbitTask == null) {
                    LOG.debug("sumbit type:{}, taskName :{}, taskcontent is null", typeName, currentTaskName);
                    continue;
                }
                boolean isSumbit = schd.addTask(typeName, sumbitTask);
                LOG.info("sumbit type:{}, taskName :{}, state:{}", typeName, currentTaskName, isSumbit);
            } catch (Exception e) {
                LOG.error("{}", e);
                EmailPool emailPool = EmailPool.getInstance();
                MailWorker.Builder builder = MailWorker.newBuilder(emailPool.getProgramInfo());
                builder.setModel(this.getClass().getSimpleName() + "模块服务发生问题");
                builder.setException(e);
                builder.setMessage("执行任务发生错误");
                emailPool.sendEmail(builder);
            }
        }
    }

    /**
     * 概述：生成任务信息
     *
     * @param taskModel
     * @param taskName
     * @param serverId
     * @param clazzName
     *
     * @return
     *
     * @throws JsonException
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    private SumbitTaskInterface createSimpleTask(TaskModel taskModel, String taskName, String serverId,
                                                 String clazzName, int repeatCount, int sleep) throws Exception {
        QuartzSimpleInfo task = new QuartzSimpleInfo();
        task.setRunNowFlag(true);
        task.setCycleFlag(false);
        task.setTaskName(taskName);
        task.setTaskGroupName(TaskType.valueOf(taskModel.getTaskType()).name());
        task.setRepeateCount(repeatCount);
        task.setInterval(sleep);
        Map<String, String> dataMap =
            JobDataMapConstract.createOperationDataMap(taskName, serverId, taskModel, repeatCount, sleep);
        if (dataMap != null && !dataMap.isEmpty()) {
            task.setTaskContent(dataMap);
        }
        task.setClassInstanceName(clazzName);
        return task;
    }

    private boolean taskRunnable(int taskType, int poolSize, int threadCount) throws Exception {

        int taskCount = threadCount;
        if (taskCount < 0) {
            LOG.debug("there is no thread in the {} pool ", taskType);
            return false;
        }
        if (poolSize <= 0 || poolSize <= taskCount) {
            LOG.debug("task pool size is full !!! pool size :{}, thread count :{}", poolSize, threadCount);
            return false;
        }
        if (TaskType.SYSTEM_DELETE.code() == taskType || TaskType.USER_DELETE.code() == taskType) {
            return true;
        }
        return true;
    }

}
