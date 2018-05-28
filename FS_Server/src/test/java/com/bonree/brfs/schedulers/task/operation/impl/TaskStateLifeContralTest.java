package com.bonree.brfs.schedulers.task.operation.impl;

import static org.junit.Assert.*;

import org.junit.Test;

import com.bonree.brfs.common.task.TaskState;
import com.bonree.brfs.common.task.TaskType;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.common.utils.Pair;
import com.bonree.brfs.schedulers.task.manager.MetaTaskManagerInterface;
import com.bonree.brfs.schedulers.task.manager.impl.DefaultReleaseTask;
import com.bonree.brfs.schedulers.task.model.TaskModel;
import com.bonree.brfs.schedulers.task.model.TaskServerNodeModel;

public class TaskStateLifeContralTest {

	@Test
	public void test() {
		MetaTaskManagerInterface release = DefaultReleaseTask.getInstance();
		release.setPropreties("192.168.101.86:2181", "/brfs/zcgTest/tasks", "/brfs/zcgTest/locks");
		TaskModel task = new TaskModel();
		task.setCreateTime(System.currentTimeMillis());
		task.setTaskState(TaskState.FINISH.code());
		task.setTaskType(TaskType.SYSTEM_COPY_CHECK.code());
		TaskServerNodeModel server = new TaskServerNodeModel();
		server.setTaskState(TaskState.FINISH.code());
		for(int i = 0; i<=10;i++){
			String taskName = release.updateTaskContentNode(task, TaskType.SYSTEM_COPY_CHECK.name(), null);
			release.updateServerTaskContentNode("10", taskName, TaskType.SYSTEM_COPY_CHECK.name(), server);
		}
		task.setTaskState(TaskState.RERUN.code());
		String taskName = release.updateTaskContentNode(task, TaskType.SYSTEM_COPY_CHECK.name(), null);
		release.updateServerTaskContentNode("10", taskName, TaskType.SYSTEM_COPY_CHECK.name(), server);
		task.setTaskState(TaskState.INIT.code());
		server.setTaskState(TaskState.INIT.code());
		for(int i = 0; i<=10;i++){
			String taskName1 = release.updateTaskContentNode(task, TaskType.SYSTEM_COPY_CHECK.name(), null);
			release.updateServerTaskContentNode("10", taskName1, TaskType.SYSTEM_COPY_CHECK.name(), server);
		}
		Pair<String, TaskModel> pair = TaskStateLifeContral.getTaskModel(release, TaskType.SYSTEM_COPY_CHECK, null, "10");
		if(pair == null){
			System.out.println("dadadadadadadadadadadadadadada");
			return;
		}
		System.out.println(pair.getKey());
		System.out.println(pair.getValue());
	}

}
