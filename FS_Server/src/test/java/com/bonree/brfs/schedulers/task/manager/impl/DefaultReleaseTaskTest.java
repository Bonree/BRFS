package com.bonree.brfs.schedulers.task.manager.impl;

import com.bonree.brfs.common.task.TaskType;
import com.bonree.brfs.duplication.storageregion.StorageRegion;
import com.bonree.brfs.schedulers.task.model.TaskModel;
import com.bonree.brfs.schedulers.task.model.TaskServerNodeModel;
import com.bonree.brfs.schedulers.utils.TasksUtils;
import java.util.Arrays;
import org.junit.Test;

public class DefaultReleaseTaskTest {
    String zkAddress = "localhost:2181";
    String taskRoot = "/release/task";
    String taskLocl = "/release/task/lock";

    @Test
    public void testStart() {
        DefaultReleaseTask releaseTask = new DefaultReleaseTask(zkAddress, taskRoot, taskLocl);
        StorageRegion region = new StorageRegion("user",0,System.currentTimeMillis()-1000000,false,2,"PT10M",100,"PT10M");
        TaskModel taskModel = TasksUtils.createUserDelete(region,TaskType.USER_DELETE,null,region.getCreateTime(),System.currentTimeMillis());
        taskModel.setTaskState(3);
        String name = releaseTask.updateTaskContentNode(taskModel, TaskType.USER_DELETE.name(), null);
        TaskServerNodeModel serverNodeModel = new TaskServerNodeModel();
        serverNodeModel.setTaskState(3);
        serverNodeModel.setRetryCount(2);
        releaseTask.updateServerTaskContentNode("10",name, TaskType.USER_DELETE.name(),serverNodeModel);
        System.out.println(name);
        releaseTask.reviseTaskStat(TaskType.USER_DELETE.name(), 7*24*60*60*1000, Arrays.asList("10"));

    }
}
