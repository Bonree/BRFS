package com.bonree.brfs.tasks.resource;

import com.bonree.brfs.common.task.TaskState;

public interface ResourceTask extends Runnable{
    int getIntervalSecond();
    void setStatus(TaskState status);
    TaskState getStatus();
}
