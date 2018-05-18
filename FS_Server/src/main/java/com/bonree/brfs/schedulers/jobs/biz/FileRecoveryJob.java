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

	}

	@Override
	public void interrupt() throws UnableToInterruptJobException {

	}

	@Override
	public void operation(JobExecutionContext context) throws Exception {
		LOG.info("----------->File Recover working");
		JobDataMap data = context.getJobDetail().getJobDataMap();
		int count = data.getInt(JobDataMapConstract.BATCH_SIZE);
		String zkHosts = data.getString(JobDataMapConstract.ZOOKEEPER_ADDRESS);
		String baseRoutPath = data.getString(JobDataMapConstract.BASE_ROUTE_PATH);
		String taskName = data.getString(JobDataMapConstract.TASK_NAME);
		LOG.info("task Name : {}", taskName);
		if(count == 0){
			count = 3;
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
		
		if(!data.containsKey(JobDataMapConstract.CURRENT_INDEX)){
			data.put(JobDataMapConstract.CURRENT_INDEX, "-1");
		}
		int currenIndex = data.getInt(JobDataMapConstract.CURRENT_INDEX);
		if(currenIndex <= 0){
			//更新上次执行的任务状态
			String result = data.getString(JobDataMapConstract.TASK_RESULT);
			if(!BrStringUtils.isEmpty(result)){
				updateTaskStatusByCompelete(mcf.getServerId(), taskName, TaskType.SYSTEM_COPY_CHECK.name(), result);
				data.put(JobDataMapConstract.TASK_RESULT, "");
			}
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
			updateTaskRunState(mcf.getServerId(), taskName, TaskType.SYSTEM_COPY_CHECK.name());
		}else{
			TaskResultModel result = null;
			String content = data.getString(currenIndex +"");
			if(BrStringUtils.isEmpty(content)){
				LOG.warn("batch data is null");
				data.put(JobDataMapConstract.CURRENT_INDEX, currenIndex -1 +"");
				result = new TaskResultModel();
				result.setSuccess(false);
				updateMapTaskMessage(context, result);
				return;
			}
			result = recoveryDirs(content,zkHosts, baseRoutPath, taskName);
			data.put(JobDataMapConstract.CURRENT_INDEX, currenIndex -1 +"");
			updateMapTaskMessage(context, result);
		}
	}
	private TaskResultModel recoveryDirs(String content,String zkHosts, String baseRoutesPath, String taskName){
		TaskResultModel result = new TaskResultModel();
		if(BrStringUtils.isEmpty(content)){
			LOG.warn("content is empty");
			result.setSuccess(false);
			return result;
		}
		BatchAtomModel batch = JsonUtils.toObject(content, BatchAtomModel.class);
		if(batch == null){
			LOG.warn("batch content is empty");
			result.setSuccess(false);
			return result;
		}
		List<AtomTaskModel> atoms = batch.getAtoms();
		if(atoms == null || atoms.isEmpty()){
			LOG.info("batch atom task is empty");
			result.setSuccess(false);
			return result;
		}
		CuratorClient curatorClient = CuratorClient.getClientInstance(zkHosts);
		ManagerContralFactory mcf = ManagerContralFactory.getInstance();
		ServerIDManager sim = mcf.getSim();
		StorageNameManager snm = mcf.getSnm();
		StorageNameNode sn = null;
		SecondIDParser parser = null;
		String snName = null;
		int snId = 0;
		String snSId = null;
		AtomTaskResultModel atomR = null;
		for(AtomTaskModel atom :atoms){
			snName = atom.getStorageName();
			sn = snm.findStorageName(snName);
			if(sn == null){
				continue;
			}
			snId = sn.getId();
			snSId = sim.getSecondServerID(snId);
			parser = new SecondIDParser(curatorClient, snId, baseRoutesPath);
			parser.updateRoute();
			atomR = recoveryFiles(atom, parser);
			result.add(atomR);
			if(!atomR.isSuccess()){
				result.setSuccess(false);
			}
		}
		return result;
	}
	/**
	 * 概述：修复文件
	 * @param atom
	 * @param parser
	 * @param sim
	 * @param snm
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	private AtomTaskResultModel recoveryFiles(AtomTaskModel atom,SecondIDParser parser){
		AtomTaskResultModel atomR = new AtomTaskResultModel();
		String snName = atom.getStorageName();
		String dirName = atom.getDirName();
		atomR.setSn(snName);
		atomR.setDir(dirName);
		
		List<String> fileNames = atom.getFiles();
		if(fileNames == null || fileNames.isEmpty()){
			atomR.setSuccess(true);
			atomR.setMessage("nothing to do");
			return atomR;
		}

		ManagerContralFactory mcf = ManagerContralFactory.getInstance();
		ServerIDManager sim = mcf.getSim();
		StorageNameManager snm = mcf.getSnm();
		ServiceManager sm = mcf.getSm();
		
		StorageNameNode snNode = snm.findStorageName(snName);
		if(snNode == null){
			atomR.setSuccess(false);
			atomR.setFiles(atom.getFiles());
			atomR.setMessage("storage name is conver null");
			return atomR;
		}
		int snId = snNode.getId();
		String secondId = sim.getSecondServerID(snId);
		if(BrStringUtils.isEmpty(secondId)){
			atomR.setSuccess(false);
			atomR.setFiles(atom.getFiles());
			atomR.setMessage("storage second name is conver null");
			return atomR;
		}
		List<String> snIds = null;
		String[] sss = null;
		String rServer = null;
		Service rService = null;
		String path = null;
		for(String fileName : fileNames){
			sss = parser.getAliveSecondID(fileName);
			if(sss == null){
				atomR.add(fileName);
				atomR.setSuccess(false);
				continue;
			}
			int index = isContain(sss, secondId);
			if(-1 == index){
				continue;
			}
			path = snName+"/"+index+"/"+dirName+"/"+fileName;
			boolean isDo = false;
			for(String snsid : sss){
				//排除自己
				if(secondId.equals(snsid)){
					continue;
				}
				rServer = sim.getOtherFirstID(secondId, snId);
				rService = sm.getServiceById(ServerConfig.DEFAULT_DISK_NODE_SERVICE_GROUP, rServer);
				
				if(recoveryFile(rService, path)){
					isDo = true;
					break;
				}
			}
			if(!isDo){
				atomR.add(fileName);
				atomR.setSuccess(false);
			}
		}
		return atomR;
	}
	private boolean recoveryFile(Service service,String path){
		DiskNodeClient client = null;
		boolean isSuccess = true;
		try {
			client = new LocalDiskNodeClient();
			client.copyFrom(service.getHost(),service.getPort(), path, path);
		}
		catch (Exception e) {
			e.printStackTrace();
			isSuccess = false;
		}finally{
			if(client != null){
				try {
					client.close();
				}
				catch (IOException e) {
					e.printStackTrace();
				}
			}
			return isSuccess;
		}
	}
	private int isContain(String[] context, String second){
		if(context == null || context.length == 0|| BrStringUtils.isEmpty(second)){
			return -1;
		}
		int i = 0;
		for(String str : context){
			i++;
			if(BrStringUtils.isEmpty(str)){
				continue;
			}
			if(second.equals(str)){
				return i;
			}
		}
		return -1;
	}
	/**
	 * 概述：获取文件列表
	 * @param fileName
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	private List<String> getSNIds(String fileName){
		if(BrStringUtils.isEmpty(fileName)){
			return null;
		}
		
		String[] tmp = BrStringUtils.getSplit(fileName, "_");
		if(tmp == null || tmp.length == 0){
			return null;
		}
		List<String> snIds = new ArrayList<String>();
		for(int i = 1; i<tmp.length; i++){
			snIds.add(tmp[i]);
		}
		return snIds;
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
		boolean isException = TaskState.EXCEPTION.code() == task.getTaskState();
		List<AtomTaskModel> tasks = convernTaskModel(task);
		if (tasks.isEmpty()) {
			TaskResultModel result = new TaskResultModel();
			result.setSuccess(true);
			data.put(JobDataMapConstract.CURRENT_INDEX, "1");
			data.put(JobDataMapConstract.TASK_NAME, taskName);
//			updateMapTaskMessage(context, result);
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
		if(isException){
			TaskResultModel result = task.getResult();
			if(result == null){
				return atoms;
			}
			List<AtomTaskResultModel> atomRs = result.getAtoms();
			if(atomRs == null || atomRs.isEmpty()){
				return atoms;
			}
			AtomTaskModel atomT = null;
			for(AtomTaskResultModel atomR : atomRs){
				if(atomR.getFiles() == null || atomR.getFiles().isEmpty()){
					continue;
				}
				atomT = new AtomTaskModel();
				atomT.setFiles(atomR.getFiles());
				atomT.setStorageName(atomR.getSn());
				atoms.add(atomT);
			}
			return atoms;
		}
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
	/**
	 * 概述：更新任务状态
	 * @param serverId
	 * @param taskname
	 * @param taskType
	 * @param result
	 * @param stat
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	protected void updateTaskStatusByCompelete(String serverId, String taskname,String taskType,String result){
		TaskResultModel taskResult = null;
		if(!BrStringUtils.isEmpty(result)){
			taskResult = JsonUtils.toObject(result, TaskResultModel.class);
		}
		ManagerContralFactory mcf = ManagerContralFactory.getInstance();
		MetaTaskManagerInterface release = mcf.getTm();
		TaskServerNodeModel sTask = release.getTaskServerContentNodeInfo(taskType, taskname, serverId);
		if(sTask == null){
			sTask = new TaskServerNodeModel();
		}
		sTask.setResult(taskResult);
		sTask.setTaskStopTime(System.currentTimeMillis());
		sTask.setTaskState(taskResult.isSuccess() ? TaskState.FINISH.code() :TaskState.EXCEPTION.code());
		release.updateServerTaskContentNode(serverId, taskname, taskType, sTask);
		LOG.info("----> complete server task :{} - {} - {} - {}",taskType, taskname, serverId, TaskState.valueOf(sTask.getTaskState()).name());
		// 更新TaskContent
		List<Pair<String,Integer>> cStatus = release.getServerStatus(taskType, taskname);
		if(cStatus == null || cStatus.isEmpty()){
			return;
		}
		LOG.info("complete c List {}",cStatus);
		int cstat = -1;
		boolean isException = false;
		int finishCount = 0;
		int size = cStatus.size();
		for(Pair<String,Integer> pair : cStatus){
			cstat = pair.getValue();
			if(TaskState.EXCEPTION.code() == cstat){
				isException = true;
				finishCount +=1;
			}else if(TaskState.FINISH.code() == cstat){
				finishCount +=1;
			}
		}
		if(finishCount != size){
			return;
		}
		TaskModel task = release.getTaskContentNodeInfo(taskType, taskname);
		if(task == null){
			task = new TaskModel();
		}
		if(isException){
			task.setTaskState(TaskState.EXCEPTION.code());
		}else{
			task.setTaskState(TaskState.FINISH.code());
		}
		release.updateTaskContentNode(task, taskType, taskname);
		LOG.info("----> complete task :{} - {} - {}",taskType, taskname, TaskState.valueOf(task.getTaskState()).name());
	}
	/**
	 * 概述：更新任务map的任务状态
	 * @param context
	 * @param stat
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	protected  void updateMapTaskMessage(JobExecutionContext context, TaskResultModel result){
		JobDataMap data  = context.getJobDetail().getJobDataMap();
		if(data == null){
			return ;
		}
		// 更新任务结果
		if(result == null){
			return;
		}
		int taskStat = -1;
		if(data.containsKey(JobDataMapConstract.TASK_MAP_STAT)){
			taskStat = data.getInt(JobDataMapConstract.TASK_MAP_STAT);
		}
		
		if(!(TaskState.EXCEPTION.code() == taskStat || TaskState.FINISH.code() == taskStat)){
			data.put(JobDataMapConstract.TASK_MAP_STAT, result.isSuccess() ? TaskState.FINISH.code() : TaskState.EXCEPTION.code());
		}else{
		}
		TaskResultModel sumResult = null;
		String content = null;
		if(data.containsKey(JobDataMapConstract.TASK_RESULT)){
			content = data.getString(JobDataMapConstract.TASK_RESULT);
		}
		if(!BrStringUtils.isEmpty(content)){
			sumResult = JsonUtils.toObject(content, TaskResultModel.class);
		}else{
			sumResult = new TaskResultModel();
		}
		sumResult.addAll(result.getAtoms());
		String sumContent = JsonUtils.toJsonString(sumResult);
		data.put(JobDataMapConstract.TASK_RESULT, sumContent);
		
	}
	
	/**
	 * 概述：将服务状态修改为RUN
	 * @param serverId
	 * @param taskname
	 * @param taskType
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	protected void updateTaskRunState(String serverId, String taskname,String taskType){
		ManagerContralFactory mcf = ManagerContralFactory.getInstance();
		MetaTaskManagerInterface release = mcf.getTm();
		int taskStat = release.queryTaskState(taskname, taskType);
		//修改服务几点状态，若不为RUN则修改为RUN
		TaskServerNodeModel serverNode = release.getTaskServerContentNodeInfo(taskType, taskname, serverId);
		if(serverNode == null){
			serverNode =new TaskServerNodeModel();
		}
		serverNode.setTaskStartTime(System.currentTimeMillis());
		serverNode.setTaskState(TaskState.RUN.code());
		release.updateServerTaskContentNode(serverId, taskname, taskType, serverNode);
		LOG.info("----> run server task :{} - {} - {} - {}",taskType, taskname, serverId, TaskState.valueOf(serverNode.getTaskState()).name());
		//查询任务节点状态，若不为RUN则获取分布式锁，修改为RUN
		if(taskStat != TaskState.RUN.code() ){
			TaskModel task = release.getTaskContentNodeInfo(taskType, taskname);
			if(task == null){
				task = new TaskModel();
			}
			task.setTaskState(TaskState.RUN.code());
			release.updateTaskContentNode(task, taskType, taskname);
			LOG.info("----> run task :{} - {} - {}",taskType, taskname,  TaskState.valueOf(task.getTaskState()).name());
		}
		
	}
}
