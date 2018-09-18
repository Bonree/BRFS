package com.bonree.brfs.schedulers.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.task.TaskState;
import com.bonree.brfs.common.task.TaskType;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.common.utils.Pair;
import com.bonree.brfs.common.utils.TimeUtils;
import com.bonree.brfs.duplication.storageregion.StorageRegion;
import com.bonree.brfs.duplication.storageregion.StorageRegionManager;
import com.bonree.brfs.schedulers.ManagerContralFactory;
import com.bonree.brfs.schedulers.task.manager.MetaTaskManagerInterface;
import com.bonree.brfs.schedulers.task.model.AtomTaskModel;
import com.bonree.brfs.schedulers.task.model.TaskModel;
import com.bonree.brfs.schedulers.task.model.TaskResultModel;
import com.bonree.brfs.schedulers.task.model.TaskServerNodeModel;
import com.bonree.brfs.schedulers.task.model.TaskTypeModel;

public class TaskStateLifeContral {
	private static final Logger LOG = LoggerFactory.getLogger("TaskLife");
	
	/**
	 * 概述：更新任务状态
	 * @param serverId
	 * @param taskname
	 * @param taskType
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static void updateTaskStatusByCompelete(String serverId, String taskname,String taskType,TaskResultModel taskResult){
		if(BrStringUtils.isEmpty(taskname)){
			LOG.info("task name is empty !!! {} {} {}", taskType,taskname, serverId);
			return;
		}
		ManagerContralFactory mcf = ManagerContralFactory.getInstance();
		MetaTaskManagerInterface release = mcf.getTm();
		TaskServerNodeModel sTask = release.getTaskServerContentNodeInfo(taskType, taskname, serverId);
		if(sTask == null){
			LOG.info("server task is null !!! {} {} {}", taskType,taskname, serverId);
			sTask = new TaskServerNodeModel();
		}
		LOG.info("TaskMessage complete  sTask :{}", JsonUtils.toJsonStringQuietly(sTask));
		sTask.setResult(taskResult);
		if(BrStringUtils.isEmpty(sTask.getTaskStartTime())) {
			sTask.setTaskStartTime(TimeUtils.formatTimeStamp(System.currentTimeMillis(), TimeUtils.TIME_MILES_FORMATE));
		}
		sTask.setTaskStopTime(TimeUtils.formatTimeStamp(System.currentTimeMillis(), TimeUtils.TIME_MILES_FORMATE));
		TaskState status = taskResult == null ? TaskState.EXCEPTION : taskResult.isSuccess() ? TaskState.FINISH :TaskState.EXCEPTION;
		sTask.setTaskState(status.code());
		release.updateServerTaskContentNode(serverId, taskname, taskType, sTask);
		LOG.info("Complete server task :{} - {} - {} - {}",taskType, taskname, serverId, TaskState.valueOf(sTask.getTaskState()).name());
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
			cstat = pair.getSecond();
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
			LOG.info("task is null !!! {} {} {}", taskType,taskname);
			task = new TaskModel();
			task.setCreateTime(TimeUtils.formatTimeStamp(System.currentTimeMillis(), TimeUtils.TIME_MILES_FORMATE));
		}
		if(isException){
			task.setTaskState(TaskState.EXCEPTION.code());
		}else{
			task.setTaskState(TaskState.FINISH.code());
		}
		release.updateTaskContentNode(task, taskType, taskname);
		LOG.info("complete task :{} - {} - {}",taskType, taskname, TaskState.valueOf(task.getTaskState()).name());
		if(TaskType.SYSTEM_CHECK.name().equals(taskType)&&isException) {
			TaskModel cTask = TasksUtils.converyCopyTaskModel(release, taskname);
			if(cTask == null) {
				LOG.error("[{}]:[{}] task can't recovery !!!", taskType, taskname);
				return;
			}
			String str = TasksUtils.createCopyTask(release, taskname,cTask);
			if(BrStringUtils.isEmpty(str)) {
				boolean flag = release.setTransferTask(taskType, taskname);
				LOG.info("[{}] task [{}] find error ,transfer task create {}  ", taskType, taskname, flag ? "succefull !!!" : " fail !!!");
			}else {
				LOG.info("[{}]:[{}] find error create copy task [{}] to recovery ",taskType, taskname, str);
			}
		}
	}
	
	/**
	 * 概述：更新任务map的任务状态
	 * @param context
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static  void updateMapTaskMessage(JobExecutionContext context, TaskResultModel result){
		JobDataMap data  = context.getJobDetail().getJobDataMap();
		if(data == null){
			return ;
		}
		// 结果为空不更新批次任务结果
		if(result == null){
			return;
		}
		int taskStat = -1;
		boolean isSuccess = result.isSuccess();
		TaskResultModel sumResult = null;
		String content = null;
		if(data.containsKey(JobDataMapConstract.TASK_RESULT)){
			content = data.getString(JobDataMapConstract.TASK_RESULT);
		}
		if(!BrStringUtils.isEmpty(content)){
			sumResult = JsonUtils.toObjectQuietly(content, TaskResultModel.class);
		}else{
			sumResult = new TaskResultModel();
		}
		sumResult.addAll(result.getAtoms());
		sumResult.setSuccess(isSuccess && sumResult.isSuccess());
		String sumContent = JsonUtils.toJsonStringQuietly(sumResult);
		data.put(JobDataMapConstract.TASK_RESULT, sumContent);
	}
	
	/**
	 * 概述：将服务状态修改为RUN
	 * @param serverId
	 * @param taskname
	 * @param taskType
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static void updateTaskRunState(String serverId, String taskname,String taskType){
		ManagerContralFactory mcf = ManagerContralFactory.getInstance();
		MetaTaskManagerInterface release = mcf.getTm();
		int taskStat = release.queryTaskState(taskname, taskType);
		//修改服务几点状态，若不为RUN则修改为RUN
		TaskServerNodeModel serverNode = release.getTaskServerContentNodeInfo(taskType, taskname, serverId);
		if(serverNode == null){
			serverNode =new TaskServerNodeModel();
		}
		LOG.debug("TaskMessage Run  sTask :{}", JsonUtils.toJsonStringQuietly(serverNode));
		serverNode.setTaskStartTime(TimeUtils.formatTimeStamp(System.currentTimeMillis(), TimeUtils.TIME_MILES_FORMATE));
		serverNode.setTaskState(TaskState.RUN.code());
		release.updateServerTaskContentNode(serverId, taskname, taskType, serverNode);
		LOG.debug("> run server task :{} - {} - {} - {}",taskType, taskname, serverId, TaskState.valueOf(serverNode.getTaskState()).name());
		//查询任务节点状态，若不为RUN则获取分布式锁，修改为RUN
		if(taskStat != TaskState.RUN.code() ){
			TaskModel task = release.getTaskContentNodeInfo(taskType, taskname);
			if(task == null){
				task = new TaskModel();
			}
			task.setTaskState(TaskState.RUN.code());
			release.updateTaskContentNode(task, taskType, taskname);
			LOG.debug("run task :{} - {} - {}",taskType, taskname,  TaskState.valueOf(task.getTaskState()).name());
		}
	}
	/**
	 * 概述：获取当前任务信息
	 * @param release
	 * @param typeName
	 * @param serverId
	 * @param limitCount
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static Pair<String,TaskModel> getCurrentOperationTask(MetaTaskManagerInterface release, String typeName,String serverId, int limitCount){
		List<Pair<String,Pair<Integer,Integer>>> needTasks = getServerState(release, typeName, serverId);
		Pair<String,Pair<Integer,Integer>> task = getOperationTask(needTasks, limitCount);
		if(task == null){
			return null;
		}
		if(BrStringUtils.isEmpty(task.getFirst())){
			return null;
		}
		TaskModel cTask = release.getTaskContentNodeInfo(typeName, task.getFirst());
		if(cTask == null){
			return null;
		}
		//更新异常的次数
		if(task.getSecond().getFirst() == TaskState.EXCEPTION.code()){
			TaskServerNodeModel server = release.getTaskServerContentNodeInfo(typeName, task.getFirst(), serverId);
			LOG.info("TaskMessage get  sTask :{}", JsonUtils.toJsonStringQuietly(server));
			server.setRetryCount(server.getRetryCount() + 1);
			release.updateServerTaskContentNode(serverId, task.getFirst(), typeName, server);
		}
		
		return new Pair<String,TaskModel>(task.getFirst(), cTask);
	}
	public static TaskModel changeRunTaskModel(final TaskModel message, String dataPath){
		if(message == null){
			return null;
		}
		// 文件恢复单独不需要处理
		TaskModel changeTask = new TaskModel();
		changeTask.setCreateTime(message.getCreateTime());
		changeTask.setTaskState(changeTask.getTaskState());
		changeTask.setTaskType(message.getTaskType());
		if(TaskType.SYSTEM_COPY_CHECK.code() == changeTask.getTaskType()) {
			changeTask.setAtomList(message.getAtomList());
			return changeTask;
		}
		// 删除任务，校验任务，需要扫目录确定
		List<AtomTaskModel> mAtoms = message.getAtomList();
		if(mAtoms == null || mAtoms.isEmpty()){
			LOG.warn("task message atom list is empty!!!");
			return null;
		}
		// 循环atom，封装atom
		AtomTaskModel rAtom = null;
		long startTime = 0L;
		long endTime = 0L;
		String snName = null;
		int partNum = 0;
		for(AtomTaskModel atom : mAtoms){
			startTime = TimeUtils.getMiles(atom.getDataStartTime(), TimeUtils.TIME_MILES_FORMATE);
			endTime = TimeUtils.getMiles(atom.getDataStopTime(), TimeUtils.TIME_MILES_FORMATE);
			snName = atom.getStorageName();
			partNum = atom.getPatitionNum();
			List<String> dirs = LocalFileUtils.collectDucationTimeDirNames(dataPath, snName, startTime, endTime);
			dirs = filterRepeadDirs(dirs);
			List<Pair<Long,Long>> pDirs = LocalFileUtils.converPairByUniqueness(dirs);
			List<Pair<Long,Long>> batchTimes = LocalFileUtils.sortTime(pDirs);
			for(Pair<Long,Long> pair : batchTimes) {
				rAtom = AtomTaskModel.getInstance(null, snName, atom.getTaskOperation(), partNum, pair.getFirst(), pair.getSecond(), 0);
				changeTask.addAtom(rAtom);
			}
		}
		return changeTask;
	}
	public static List<String> filterRepeadDirs(List<String> files){
		if(files == null || files.isEmpty()) {
			return new ArrayList<String>();
		}
		List<String> nFiles = new ArrayList<String>();
		for(String file : files) {
			if(!nFiles.contains(file)) {
				nFiles.add(file);
			}
		}
		return nFiles;
	}
	
	/**
	 * 概述：将任务分批
	 * @param message
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	@Deprecated
	public static TaskModel changeRunTaskModel(final TaskModel message) {
		if(message == null) {
			return null;
		}
		
		TaskModel changeTask = new TaskModel();
		changeTask.setCreateTime(message.getCreateTime());
		changeTask.setTaskState(changeTask.getTaskState());
		changeTask.setTaskType(message.getTaskType());
		if(TaskType.SYSTEM_COPY_CHECK.code() == changeTask.getTaskType()) {
			changeTask.setAtomList(message.getAtomList());
			return changeTask;
		}
		List<AtomTaskModel> atoms = message.getAtomList();
		AtomTaskModel atom = null;
		if(atoms == null || atoms.isEmpty()) {
			return changeTask;
		}
		long startTime = 0;
		long endTime = 0;
		String snName = null;
		String operation = null;
		long granule = 0;
		for(AtomTaskModel aTask : atoms) {
			startTime = TimeUtils.getMiles(aTask.getDataStartTime(), TimeUtils.TIME_MILES_FORMATE);
			endTime = TimeUtils.getMiles(aTask.getDataStopTime(), TimeUtils.TIME_MILES_FORMATE);
			snName = aTask.getStorageName();
			operation = aTask.getTaskOperation();
			granule = aTask.getGranule();
			for(long start = startTime; start < endTime; start += granule) {
				if(start +granule > endTime) {
					continue;
				}
				atom = AtomTaskModel.getInstance(null, snName, operation, aTask.getPatitionNum(), start, start+granule, granule);
				changeTask.addAtom(atom);
			}
		}
		return changeTask;
	}
	
	/**
	 * 概述：获取指定任务的队列
	 * @param release
	 * @param typeName
	 * @param serverId
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static List<Pair<String,Pair<Integer,Integer>>> getServerState(MetaTaskManagerInterface release, String typeName, String serverId){
		List<String> taskNames = release.getTaskList(typeName);
		List<Pair<String,Pair<Integer,Integer>>> sTaskStatuss = new ArrayList<Pair<String,Pair<Integer,Integer>>>();
		if(taskNames == null || taskNames.isEmpty()){
			return sTaskStatuss;
		}
		Pair<String,Pair<Integer,Integer>> sTaskStatus = null;
		Pair<Integer,Integer> codeAndCount = null;
		TaskServerNodeModel server = null;
		for(String taskName : taskNames){
			server = release.getTaskServerContentNodeInfo(typeName, taskName, serverId);
			if(server == null){
				continue;
			}
			if(server.getTaskState() == TaskState.FINISH.code()){
				continue;
			}
			codeAndCount = new Pair<Integer, Integer>(server.getTaskState(), server.getRetryCount());
			sTaskStatus = new Pair<String,Pair<Integer,Integer>>(taskName,codeAndCount);
			sTaskStatuss.add(sTaskStatus);
		}
		return sTaskStatuss;
	}
	/**
	 * 概述：获取当前执行的任务
	 * @param tasks
	 * @param limtCount
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static Pair<String,Pair<Integer,Integer>> getOperationTask(List<Pair<String,Pair<Integer,Integer>>> tasks, int limtCount){
		if(tasks == null || tasks.isEmpty()){
			return null;
		}
		List<Pair<String,Pair<Integer,Integer>>> eTasks = new ArrayList<Pair<String,Pair<Integer,Integer>>>();
		Pair<Integer,Integer> codeAndCount = null;
		for(Pair<String,Pair<Integer,Integer>> task : tasks){
			codeAndCount = task.getSecond();
			if(codeAndCount == null){
				continue;
			}
			if(codeAndCount.getFirst() == TaskState.FINISH.code()|| codeAndCount.getFirst() == TaskState.RUN.code()||codeAndCount.getFirst() == TaskState.RERUN.code()){
				continue;
			}else if(codeAndCount.getFirst() == TaskState.INIT.code()){
				return task;
			}else if(codeAndCount.getFirst() == TaskState.EXCEPTION.code() && limtCount > 0){
				if(codeAndCount.getSecond() >limtCount){
					eTasks.add(task);
				}else{
					return task;
				}
			}
		}
		if(eTasks == null || eTasks.isEmpty()|| limtCount <= 0){
			return null;
		}
		Collections.sort(eTasks, new Comparator<Pair<String,Pair<Integer,Integer>>>() {
			public int compare(Pair<String,Pair<Integer,Integer>> o1, Pair<String,Pair<Integer,Integer>> o2) {
				if(o1 == null|| o1.getSecond() == null ){
					return -1;
				}
				if(o2 == null || o2.getSecond() == null){
					return 1;
				}
				if(o1.getSecond().getSecond() > o2.getSecond().getSecond()){
					return -1;
				}else if(o1.getSecond().getSecond() == o2.getSecond().getSecond()){
					return 0;
				}else{
					return 1;
				}
				
			}
		});
		return eTasks.get(eTasks.size() -1);
	}
	/**
	 * 概述：去掉已删除的sn
	 * @param release
	 * @param srs
	 * @param taskType
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static void watchSR(MetaTaskManagerInterface release,List<String> srs, String taskType) {
		TaskTypeModel  typeModel = release.getTaskTypeInfo(taskType);
		Map<String,Long> snMap = typeModel.getSnTimes();
		if(snMap == null || snMap.isEmpty()) {
			return ;
		}
		List<String> deleteSRs = new ArrayList<String>();
		for(String srName : snMap.keySet()) {
			if(srs.contains(srName)) {
				continue;
			}
			deleteSRs.add(srName);
		}
		if(deleteSRs == null || deleteSRs.isEmpty()) {
			return;
		}
		for(String str : deleteSRs) {
			typeModel.removesnTime(str);
		}
		release.setTaskTypeModel(taskType, typeModel);
	}
	/***
	 * 概述：转换
	 * @param srm
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static List<String> getSRs(StorageRegionManager srm){
		List<StorageRegion> srList = srm.getStorageRegionList();
		List<String> srs = new ArrayList<String>();
		if(srList == null || srList.isEmpty()) {
			return srs;
		}
		String srName = null;
		for(StorageRegion sr : srList) {
			srName = sr.getName();
			if(BrStringUtils.isEmpty(srName)) {
				continue;
			}
			srs.add(srName);
		}
		return srs;
	}
}
