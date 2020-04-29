package com.bonree.brfs.tasks.resource.impl;

import com.bonree.brfs.common.task.TaskState;
import com.bonree.brfs.tasks.resource.ResourceTask;
import org.slf4j.Logger;

/**
 * 资源采集任务父类
 */
public abstract class SuperResourceTask implements ResourceTask {
    private Logger log;
    private int intervalTime;
    private TaskState status = TaskState.INIT;

    public SuperResourceTask(Logger log, int intervalTime) {
        this.log = log;
        this.intervalTime = intervalTime;
    }

    @Override
    public int getIntervalSecond() {
        return this.intervalTime;
    }

    @Override
    public void setStatus(TaskState status) {
        this.status = status;
    }

    @Override
    public TaskState getStatus() {
        return this.status;
    }

    @Override
    public void run() {
        if(TaskState.PAUSE.equals(status)){
            log.info("task is pause !");
           return;
        }
        atomRun();
    }
    protected abstract void atomRun();
}
