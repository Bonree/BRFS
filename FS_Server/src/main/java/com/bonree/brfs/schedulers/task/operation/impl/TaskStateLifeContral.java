package com.bonree.brfs.schedulers.task.operation.impl;

import java.util.List;

import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.task.TaskState;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.common.utils.Pair;
import com.bonree.brfs.schedulers.ManagerContralFactory;
import com.bonree.brfs.schedulers.jobs.JobDataMapConstract;
import com.bonree.brfs.schedulers.task.manager.MetaTaskManagerInterface;
import com.bonree.brfs.schedulers.task.model.TaskModel;
import com.bonree.brfs.schedulers.task.model.TaskResultModel;
import com.bonree.brfs.schedulers.task.model.TaskServerNodeModel;

public class TaskStateLifeContral {
	private static final Logger LOG = LoggerFactory.getLogger("TaskLife");
	/**
	 * 概述：更新任务结果
	 * @param serverId
	 * @param taskname
	 * @param taskType
	 * @param result
	 * @param stat
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static boolean updateServerNodeResultOnly(String taskType, String taskname,String serverId,TaskResultModel taskResult){
		if(taskResult == null){
			return false;
		}
		ManagerContralFactory mcf = ManagerContralFactory.getInstance();
		MetaTaskManagerInterface release = mcf.getTm();
		TaskServerNodeModel sTask = release.getTaskServerContentNodeInfo(taskType, taskname, serverId);
		if(sTask == null){
			sTask = new TaskServerNodeModel();
		}
		int stat = taskResult.isSuccess() ? TaskState.FINISH.code() : TaskState.EXCEPTION.code();
		sTask.setResult(taskResult);
		sTask.setTaskStopTime(System.currentTimeMillis());
		sTask.setTaskState(stat);
		LOG.info("update serverNode task: {} - {} - {} - {}",taskType, taskname, serverId, TaskState.valueOf(stat).name());
		return release.updateServerTaskContentNode(serverId, taskname, taskType, sTask);
	}
	/**
	 * 概述：更新任务的状态
	 * @param taskType
	 * @param taskName
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static boolean updateTaskStateByComplete(String taskType, String taskName){
		ManagerContralFactory mcf = ManagerContralFactory.getInstance();
		MetaTaskManagerInterface release = mcf.getTm();
		// 更新TaskContent
		List<Pair<String, Integer>> cStatus = release.getServerStatus(taskType, taskName);
		if (cStatus == null || cStatus.isEmpty()) {
			return false;
		}
		LOG.info("complete c List {}", cStatus);
		int cstat = -1;
		boolean isException = false;
		int finishCount = 0;
		int size = cStatus.size();
		for (Pair<String, Integer> pair : cStatus) {
			cstat = pair.getValue();
			if (TaskState.EXCEPTION.code() == cstat) {
				isException = true;
				finishCount += 1;
			}
			else if (TaskState.FINISH.code() == cstat) {
				finishCount += 1;
			}
		}
		if (finishCount != size) {
			return false;
		}
		TaskModel task = release.getTaskContentNodeInfo(taskType, taskName);
		if (task == null) {
			task = new TaskModel();
		}
		if (isException) {
			task.setTaskState(TaskState.EXCEPTION.code());
		}
		else {
			task.setTaskState(TaskState.FINISH.code());
		}
		LOG.info("update task :{} - {} - {}", taskType, taskName, TaskState.valueOf(task.getTaskState()).name());
		release.updateTaskContentNode(task, taskType, taskName);
		return true;
	}
}
