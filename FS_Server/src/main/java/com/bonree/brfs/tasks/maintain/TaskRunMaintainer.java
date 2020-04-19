package com.bonree.brfs.tasks.maintain;

import com.bonree.brfs.common.process.LifeCycle;
import com.bonree.brfs.common.task.TaskType;
import com.bonree.brfs.schedulers.task.model.TaskModel;

/**
 * 版权信息: 北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007-2020 北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 *
 * @date: 2020年04月19日 14:50
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @description: 任务执行维护者 负责任务执行部分，主要负责周期执行，任务状态更新
 **/
public interface TaskRunMaintainer extends LifeCycle {
    /**
     * 获取任务类型
     * @return
     */
    TaskType getTaskType();

    /**
     * 执行任务
     * @param taskModel
     */
    void executTask(TaskModel taskModel);

}
