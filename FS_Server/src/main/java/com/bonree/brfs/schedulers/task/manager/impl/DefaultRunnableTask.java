package com.bonree.brfs.schedulers.task.manager.impl;

import com.bonree.brfs.common.task.TaskType;
import com.bonree.brfs.configuration.ResourceTaskConfig;
import com.bonree.brfs.schedulers.task.manager.RunnableTaskInterface;
import com.bonree.brfs.schedulers.task.model.TaskExecutablePattern;
import com.bonree.brfs.schedulers.task.model.TaskModel;
import com.bonree.brfs.schedulers.task.model.TaskRunPattern;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultRunnableTask implements RunnableTaskInterface {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultRunnableTask.class);
    private TaskExecutablePattern limit = null;

    private static final int batchCount = 5;
    private static final long batchSleepTime = 5000L;
    private static final int maxbatchTimes = 10;
    private static final long maxBatchSleepTime = 30000L;

    @Inject
    public DefaultRunnableTask(ResourceTaskConfig config) {
        this.limit = TaskExecutablePattern.parse(config);
    }

    @Override
    public void setLimitParameter(TaskExecutablePattern limits) {
        this.limit = limits;
    }

    @Override
    public TaskRunPattern taskRunnPattern(TaskModel task) throws Exception {
        TaskRunPattern runPattern = new TaskRunPattern();
        int dataSize = task.getAtomList().size();
        int repeadCount = 1;
        repeadCount = (dataSize % batchCount == 0) ? dataSize / batchCount : (dataSize / batchCount + 1);
        repeadCount = repeadCount > maxbatchTimes ? maxbatchTimes : repeadCount <= 0 ? 1 : repeadCount;
        long sleepTime =
            task.getTaskType() * batchSleepTime > maxBatchSleepTime ? maxBatchSleepTime : task.getTaskType() * batchSleepTime;
        sleepTime = sleepTime == 0 ? batchSleepTime : sleepTime;
        runPattern.setRepeateCount(repeadCount);
        runPattern.setSleepTime(sleepTime);
        return runPattern;
    }

    @Override
    public boolean taskRunnable(int taskType, int poolSize, int threadCount) throws Exception {

        int taskCount = threadCount;
        if (taskCount < 0) {
            LOG.warn("there is no thread in the {} pool ", taskType);
            return false;
        }
        if (poolSize <= 0 || poolSize <= taskCount) {
            LOG.warn("task pool size is full !!! pool size :{}, thread count :{}", poolSize, threadCount);
            return false;
        }
        if (TaskType.SYSTEM_DELETE.code() == taskType || TaskType.USER_DELETE.code() == taskType) {
            return true;
        }
        return true;
    }

}
