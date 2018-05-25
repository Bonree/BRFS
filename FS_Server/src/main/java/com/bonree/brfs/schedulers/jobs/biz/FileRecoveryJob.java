package com.bonree.brfs.schedulers.jobs.biz;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.UnableToInterruptJobException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.task.TaskState;
import com.bonree.brfs.common.task.TaskType;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.common.utils.Pair;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.configuration.ServerConfig;
import com.bonree.brfs.disknode.client.DiskNodeClient;
import com.bonree.brfs.disknode.client.HttpDiskNodeClient;
import com.bonree.brfs.disknode.client.LocalDiskNodeClient;
import com.bonree.brfs.duplication.storagename.StorageNameManager;
import com.bonree.brfs.duplication.storagename.StorageNameNode;
import com.bonree.brfs.rebalance.route.SecondIDParser;
import com.bonree.brfs.schedulers.ManagerContralFactory;
import com.bonree.brfs.schedulers.jobs.JobDataMapConstract;
import com.bonree.brfs.schedulers.task.manager.MetaTaskManagerInterface;
import com.bonree.brfs.schedulers.task.model.AtomTaskModel;
import com.bonree.brfs.schedulers.task.model.AtomTaskResultModel;
import com.bonree.brfs.schedulers.task.model.BatchAtomModel;
import com.bonree.brfs.schedulers.task.model.TaskModel;
import com.bonree.brfs.schedulers.task.model.TaskResultModel;
import com.bonree.brfs.schedulers.task.model.TaskServerNodeModel;
import com.bonree.brfs.schedulers.task.operation.impl.QuartzOperationStateTask;
import com.bonree.brfs.schedulers.task.operation.impl.QuartzOperationStateWithZKTask;
import com.bonree.brfs.schedulers.task.operation.impl.TaskStateLifeContral;
import com.bonree.brfs.server.identification.ServerIDManager;
/******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年5月15日 下午9:47:56
 * @Author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @Description: 文件恢复任务，重复运行，自动从zk获取任务信息，若无信息则空泡
 *****************************************************************************
 */
public class FileRecoveryJob extends QuartzOperationStateTask {
	private static final Logger LOG = LoggerFactory.getLogger("FileRecoveryJob");
	@Override
	public void caughtException(JobExecutionContext context) {
		JobDataMap data = context.getJobDetail().getJobDataMap();
		int count = data.getInt(JobDataMapConstract.BATCH_SIZE);
		String zkHosts = data.getString(JobDataMapConstract.ZOOKEEPER_ADDRESS);
		String baseRoutPath = data.getString(JobDataMapConstract.BASE_ROUTE_PATH);
		String taskName = data.getString(JobDataMapConstract.TASK_NAME);
		if(!data.containsKey(JobDataMapConstract.CURRENT_INDEX)){
			data.put(JobDataMapConstract.CURRENT_INDEX, "-1");
		}
		int currenIndex = data.getInt(JobDataMapConstract.CURRENT_INDEX);
		ManagerContralFactory mcf = ManagerContralFactory.getInstance();
		if(currenIndex <= 0){
			//更新上次执行的任务状态
			String result = data.getString(JobDataMapConstract.TASK_RESULT);
			if(!BrStringUtils.isEmpty(result)){
				TaskResultModel results = JsonUtils.toObject(result, TaskResultModel.class);
				int stat = data.getInt(JobDataMapConstract.TASK_MAP_STAT);
				TaskStateLifeContral.updateTaskStatusByCompelete(mcf.getServerId(), taskName, TaskType.SYSTEM_COPY_CHECK.name(), result,stat);
				data.put(JobDataMapConstract.TASK_MAP_STAT, TaskState.INIT.code());
				data.put(JobDataMapConstract.TASK_RESULT, "");
			}
		}else{
			TaskResultModel resultTask = new TaskResultModel();
			resultTask.setSuccess(false);
			TaskStateLifeContral.updateMapTaskMessage(context, resultTask);
		}
		
	}

	@Override
	public void interrupt() throws UnableToInterruptJobException {

	}

	@Override
	public void operation(JobExecutionContext context) throws Exception {
		//TODO 测试中的代码
		LOG.info("----------->File Recover working");
		JobDataMap data = context.getJobDetail().getJobDataMap();
		int count = data.getInt(JobDataMapConstract.BATCH_SIZE);
		String zkHosts = data.getString(JobDataMapConstract.ZOOKEEPER_ADDRESS);
		String baseRoutPath = data.getString(JobDataMapConstract.BASE_ROUTE_PATH);
		String taskName = data.getString(JobDataMapConstract.TASK_NAME);
		if(!data.containsKey(JobDataMapConstract.CURRENT_INDEX)){
			data.put(JobDataMapConstract.CURRENT_INDEX, "-1");
		}
		int currenIndex = data.getInt(JobDataMapConstract.CURRENT_INDEX);
		LOG.info("task Name : {}", taskName);
		if(count == 0){
			count = 100;
		}
		// 判断任务是否处在副本恢复任务，若是，返回
		if (WatchSomeThingJob.getState(WatchSomeThingJob.RECOVERY_STATUSE)) {
			LOG.warn("rebalance task is running !! skip FileRecoverJob");
			return;
		}
		ManagerContralFactory mcf = ManagerContralFactory.getInstance();
		MetaTaskManagerInterface release = mcf.getTm();
		String serviceId = mcf.getServerId();
		// CurrentIdex值为0 或 -1 则获取新的任务，不为零则继续任务获取当前值 
		
		if(currenIndex <= 0){
			//更新上次执行的任务状态
			String result = data.getString(JobDataMapConstract.TASK_RESULT);
			if(!BrStringUtils.isEmpty(result)){
				TaskResultModel results = JsonUtils.toObject(result, TaskResultModel.class);
				int stat = data.getInt(JobDataMapConstract.TASK_MAP_STAT);
				TaskStateLifeContral.updateTaskStatusByCompelete(mcf.getServerId(), taskName, TaskType.SYSTEM_COPY_CHECK.name(), result,stat);
				data.put(JobDataMapConstract.TASK_MAP_STAT, TaskState.INIT.code());
				data.put(JobDataMapConstract.TASK_RESULT, "");
			}
			LOG.info(" get task from zk");
			// 从zk获取任务信息最后一次执行成功的  若任务为空则返回
			Pair<String,TaskModel> taskPair = getTaskModel(release, taskName, serviceId);
			if(taskPair == null){
				LOG.info("task queue is empty !!!");
				return;
			}
			// 将当前的任务分成批次执行
			TaskModel task = taskPair.getValue();
			String nextTaskName = taskPair.getKey();
			createBatch(release, context, task, nextTaskName, serviceId, count);
			TaskStateLifeContral.updateTaskRunState(mcf.getServerId(), taskName, TaskType.SYSTEM_COPY_CHECK.name());
			
		}else{
			TaskResultModel result = null;
			String content = data.getString(currenIndex +"");
			result = FileRecovery.recoveryDirs(content,zkHosts, baseRoutPath);
			data.put(JobDataMapConstract.CURRENT_INDEX, currenIndex -1 +"");
			TaskStateLifeContral.updateMapTaskMessage(context, result);
		}
	}
	

	/**
	 * 概述：创建批量任务
	 * @param release
	 * @param context
	 * @param task
	 * @param taskName
	 * @param serverId
	 * @param batchSize
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	private void createBatch(MetaTaskManagerInterface release, JobExecutionContext context,TaskModel task, String taskName, String serverId,int batchSize){
		JobDataMap data = context.getJobDetail().getJobDataMap();
		String taskType = TaskType.SYSTEM_COPY_CHECK.name();
		List<AtomTaskModel> tasks = convernTaskModel(task);
		if (tasks.isEmpty()) {
			TaskResultModel result = new TaskResultModel();
			result.setSuccess(true);
			data.put(JobDataMapConstract.CURRENT_INDEX, "1");
			data.put(JobDataMapConstract.TASK_NAME, taskName);
			TaskStateLifeContral.updateMapTaskMessage(context, result);
			return;
		}

		List<AtomTaskModel> atoms = task.getAtomList();
		int size = atoms == null ? 0 : atoms.size();
		int count = size / batchSize;
		BatchAtomModel batch = null;
		List<AtomTaskModel> tmp = null;
		data.put(JobDataMapConstract.CURRENT_INDEX, count + "");
		data.put(JobDataMapConstract.TASK_NAME, taskName);
		int index = 0;
		for (int i = 1; i <= count; i += count) {
			batch = new BatchAtomModel();
			if (index + count <= size) {
				tmp = atoms.subList(index, index + count);
			}
			else if (size > 0) {
				tmp = atoms.subList(index, size - 1);
			}
			else {
				tmp = new ArrayList<AtomTaskModel>();
			}
			batch.addAll(tmp);
			data.put(i + "", JsonUtils.toJsonString(batch));
			index = index + count;
		}

	}
	
	private List<AtomTaskModel> convernTaskModel(TaskModel task){
		List<AtomTaskModel> atoms = new ArrayList<AtomTaskModel>();
		boolean isException = TaskState.EXCEPTION.code() == task.getTaskState();
//		if(isException){
//			TaskResultModel result = task.getResult();
//			if(result == null){
//				atoms.addAll(task.getAtomList());
//				return atoms;
//			}
//			List<AtomTaskResultModel> atomRs = result.getAtoms();
//			if(atomRs == null || atomRs.isEmpty()){
//				atoms.addAll(task.getAtomList());
//				return atoms;
//			}
//			AtomTaskModel atomT = null;
//			for(AtomTaskResultModel atomR : atomRs){
//				if(atomR.getFiles() == null || atomR.getFiles().isEmpty()){
//					continue;
//				}
//				atomT = new AtomTaskModel();
//				atomT.setFiles(atomR.getFiles());
//				atomT.setStorageName(atomR.getSn());
//				atoms.add(atomT);
//			}
//			return atoms;
//		}
		List<AtomTaskModel> tasks = task.getAtomList();
		if(tasks == null || tasks.isEmpty()){
			return atoms;
		}
		atoms.addAll(tasks);
		return atoms;
	}
	/**
	 * 概述：获取任务信息
	 * @param release
	 * @param taskName
	 * @param serviceId
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	private Pair<String, TaskModel> getTaskModel(MetaTaskManagerInterface release,String taskName, String serviceId){
		if(BrStringUtils.isEmpty(taskName)){
			return getTaskModel(release, serviceId);
		}
		String taskType = TaskType.SYSTEM_COPY_CHECK.name();
		TaskModel task = release.getTaskContentNodeInfo(taskType, taskName);
		if(task != null && task.getTaskState() == TaskState.EXCEPTION.code()){
			return new Pair<String,TaskModel>(taskName, task);
		}
		String next = release.getNextTaskName(taskType, taskName);
		if(BrStringUtils.isEmpty(next)){
			return null;
		}
		task = release.getTaskContentNodeInfo(taskType, taskName);
		if(task != null){
			return new Pair<String,TaskModel>(next, task);
		}
		return null;
	}
	/**
	 * 概述：获取当前任务
	 * @param release
	 * @param serviceId
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	private Pair<String,TaskModel> getTaskModel(MetaTaskManagerInterface release, String serviceId){
		Pair<String,TaskModel> result = new Pair<>();
		TaskModel task = null;
		String taskType = TaskType.SYSTEM_COPY_CHECK.name();
		List<String> tasks = release.getTaskList(taskType);
		if(tasks == null || tasks.isEmpty()){
			return null;
		}
		String firstname = tasks.get(0);
		String successname = release.getLastSuccessTaskIndex(taskType, serviceId);
		if(BrStringUtils.isEmpty(successname)){
			task = release.getTaskContentNodeInfo(taskType, firstname);
			if(task == null){
				return null;
			}
			result.setValue(task);
			result.setKey(firstname);
			return result;
		}
		int index = tasks.indexOf(successname);
		if(index +1 >=tasks.size()){
			return null;
		}
		task = release.getTaskContentNodeInfo(taskType, tasks.get(index + 1));
		if(task == null){
			return null;
		}
		result.setValue(task);
		result.setKey(tasks.get(index + 1));
		return result;
	}
}
