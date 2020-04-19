package com.bonree.brfs.schedulers.utils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.ReturnCode;
import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.task.TaskState;
import com.bonree.brfs.common.task.TaskType;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.utils.TimeUtils;
import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.CommonConfigs;
import com.bonree.brfs.duplication.storageregion.StorageRegion;
import com.bonree.brfs.duplication.storageregion.StorageRegionManager;
import com.bonree.brfs.schedulers.ManagerContralFactory;
import com.bonree.brfs.schedulers.jobs.biz.UserDeleteJob;
import com.bonree.brfs.schedulers.jobs.system.CopyCheckJob;
import com.bonree.brfs.schedulers.task.manager.MetaTaskManagerInterface;
import com.bonree.brfs.schedulers.task.manager.impl.DefaultReleaseTask;
import com.bonree.brfs.schedulers.task.model.AtomTaskModel;
import com.bonree.brfs.schedulers.task.model.AtomTaskResultModel;
import com.bonree.brfs.schedulers.task.model.TaskModel;
import com.bonree.brfs.schedulers.task.model.TaskResultModel;
import com.bonree.brfs.schedulers.task.model.TaskServerNodeModel;
import com.bonree.brfs.schedulers.task.model.TaskTypeModel;

public class TasksUtils {
	private static final Logger LOG = LoggerFactory.getLogger(TasksUtils.class);
	/**
	 * 概述：
	 * @param services
	 * @param zkPaths
	 * @param sn
	 * @param startTime
	 * @param endTime
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	 public static ReturnCode createUserDeleteTask(List<Service> services, ZookeeperPaths zkPaths, StorageRegion sn, long startTime, long endTime, boolean isAll){
		 	if(services == null || services.isEmpty()){
		 		return ReturnCode.DELETE_DATA_ERROR;
		 	}
		 	if(sn == null){
		 		return ReturnCode.STORAGE_NONEXIST_ERROR;
		 	}
		 	
		 	String zkAddresses = Configs.getConfiguration().GetConfig(CommonConfigs.CONFIG_ZOOKEEPER_ADDRESSES);
		 	MetaTaskManagerInterface release = new DefaultReleaseTask(zkAddresses, zkPaths.getBaseTaskPath(), zkPaths.getBaseLocksPath());
	    	TaskTypeModel tmodel = release.getTaskTypeInfo(TaskType.USER_DELETE.name());
	    	if(!tmodel.isSwitchFlag()) {
	    		return ReturnCode.FORBID_DELETE_DATA_ERROR;
	    	}
	    	TaskModel task = TasksUtils.createUserDelete(sn, TaskType.USER_DELETE, isAll? UserDeleteJob.DELETE_SN_ALL :UserDeleteJob.DELETE_PART, startTime, endTime);
	    	if(task == null){
	    		return ReturnCode.DELETE_DATA_ERROR;
	    	}
	    	List<String> serverIds = CreateSystemTask.getServerIds(services);
	        String taskName = CreateSystemTask.updateTask(release, task, serverIds, TaskType.USER_DELETE);
	        if(!BrStringUtils.isEmpty(taskName)) {
	        	return ReturnCode.SUCCESS;
	        }else {
	        	return ReturnCode.DELETE_DATA_ERROR;
	        }
	    }
	/**
	 * 概述：创建可执行任务信息
	 * @param sn storageName信息
	 * @param taskType 任务类型
	 * @param opertationContent 执行的内容，暂时无用
	 * @param startTime 要操作数据的开始时间 -1标识为sn的创建时间
	 * @param endTime 要操作数据的结束时间。
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static TaskModel createUserDelete(final StorageRegion sn,final TaskType taskType, final String opertationContent, final long startTime, final long endTime){
		if(sn == null ||taskType == null){
			return null;
		}
		TaskModel task = new TaskModel();
		List<AtomTaskModel> storageAtoms = new ArrayList<>();
		if(endTime == 0 || startTime >= endTime){
			return null;
		}
		long granule = Duration.parse(sn.getFilePartitionDuration()).toMillis();
		String snName = sn.getName();
		long startHour;
		long endHour =  endTime - endTime%granule;
		// 删除sn
		boolean isALL = UserDeleteJob.DELETE_SN_ALL.equals(opertationContent);
		if(isALL){
			startHour = sn.getCreateTime() - sn.getCreateTime()%granule;
		}else{
			startHour = startTime - startTime%granule;
		}
		// 若是删除sn时，创建时间与删除时间间隔较短时，结束时间向后退
		if(startHour == endHour) {
			endHour = endHour + granule;
		}
		if(startHour >= endHour) {
			return null;
		}

		AtomTaskModel atom = AtomTaskModel.getInstance(null, snName, opertationContent, sn.getReplicateNum(), startHour, endHour, granule);
		storageAtoms.add(atom);
		task.setAtomList(storageAtoms);
		task.setTaskState(TaskState.INIT.code());
		task.setCreateTime(TimeUtils.formatTimeStamp(System.currentTimeMillis(), TimeUtils.TIME_MILES_FORMATE));
		task.setTaskType(taskType.code());
		return task;
	}
	/**
	 * 概述：根据CRC校验任务，创建副本数校验任务
	 * @param taskName
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static String createCopyTask(String taskName) {
		ManagerContralFactory mcf = ManagerContralFactory.getInstance();
		MetaTaskManagerInterface release = mcf.getTm();
		List<String> sNames = release.getTaskServerList(TaskType.SYSTEM_CHECK.name(), taskName);
		if(sNames == null|| sNames.isEmpty()) {
			LOG.error("CRC task [{}] get serviceList is empty!!!",taskName);
			return null;
		}
		
		TaskModel task =converyCopyTaskModel(release, taskName);
		return createTask(release, sNames, TaskType.SYSTEM_CHECK.name(), task);
	}
	public static String createCopyTask(MetaTaskManagerInterface release, String taskName,TaskModel task) {
		List<String> sNames = release.getTaskServerList(TaskType.SYSTEM_CHECK.name(), taskName);
		if(sNames == null|| sNames.isEmpty()) {
			LOG.error("CRC task [{}] get serviceList is empty!!!",taskName);
			return null;
		}
		return createTask(release, sNames, TaskType.SYSTEM_CHECK.name(), task);
	}
	public static TaskModel converyCopyTaskModel(MetaTaskManagerInterface release, String taskName) {
		List<String> sNames = release.getTaskServerList(TaskType.SYSTEM_CHECK.name(), taskName);
		if(sNames == null|| sNames.isEmpty()) {
			LOG.error("CRC task [{}] get serviceList is empty!!!",taskName);
			return null;
		}
		List<TaskServerNodeModel> sTasks = getServicesInfo(release, TaskType.SYSTEM_CHECK, taskName);
		if(sTasks == null || sTasks.isEmpty()) {
			LOG.error("CRC task [{}] get task service List is empty!!!",taskName);
			return null;
		}
		return getErrorFile(sTasks);
	}
	/**
	 * 概述： 
	 * @param release
	 * @param services
	 * @param taskType
	 * @param task
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static String createTask(MetaTaskManagerInterface release, List<String> services, String taskType, TaskModel task) {
		String tName;
		tName = release.updateTaskContentNode(task, TaskType.SYSTEM_COPY_CHECK.name(), null);
		if(BrStringUtils.isEmpty(tName)) {
			return null;
		}
		for(String sname : services) {
			release.updateServerTaskContentNode(sname, tName, TaskType.SYSTEM_COPY_CHECK.name(), TaskServerNodeModel.getInitInstance());
		}
		return tName;
	}
	/**

	/**
	 * 概述：
	 * @param release
	 * @param taskType
	 * @param name
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static List<TaskServerNodeModel> getServicesInfo(MetaTaskManagerInterface release, TaskType taskType, String name){
		List<String> sNames = release.getTaskServerList(taskType.name(), name);
		if(sNames == null|| sNames.isEmpty()) {
			LOG.error("CRC task [{}] get serviceList is empty!!!",name);
			return null;
		}
		List<TaskServerNodeModel> sTasks = new ArrayList<>();
		TaskServerNodeModel sTask;
		for(String sName : sNames) {
			sTask = release.getTaskServerContentNodeInfo(taskType.name(), name, sName);
			if(sTask == null) {
				continue;
			}
			sTasks.add(sTask);
		}
		return sTasks;
	}

	/**
	 * 概述：生成任务信息
	 * @param taskContents
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static TaskModel getErrorFile(List<TaskServerNodeModel> taskContents){
		if(taskContents == null || taskContents.isEmpty()) {
			return null;
		}
		TaskResultModel tmpR;
		List<AtomTaskResultModel> tmpRs;
		Map<String,Map<String,AtomTaskModel>> rmap = new HashMap<>();
		Map<String,AtomTaskModel> emap;
		String snName;
		String key;
		AtomTaskModel atom;
		Map<String,Integer> snMap = new HashMap<>();
		for(TaskServerNodeModel serverModel : taskContents) {
			tmpR = serverModel.getResult();
			if(tmpR == null) {
				continue;
			}
			tmpRs = tmpR.getAtoms();
			if(tmpRs == null || tmpRs.isEmpty()) {
				continue;
			}
			for(AtomTaskResultModel r : tmpRs) {
				if(r == null) {
					continue;
				}
				snName = r.getSn();
				if(!rmap.containsKey(snName)) {
					rmap.put(snName, new HashMap<>());
				}
				if(!snMap.containsKey(snName)) {
					snMap.put(snName, r.getPartNum());
				}
				emap = rmap.get(snName);
				key = r.getDataStartTime() + "_"+r.getDataStopTime();
				long granule = TimeUtils.getMiles(r.getDataStopTime(),TimeUtils.TIME_MILES_FORMATE) - TimeUtils.getMiles(r.getDataStartTime(),TimeUtils.TIME_MILES_FORMATE);
				if(!emap.containsKey(key)) {
					atom = AtomTaskModel.getInstance(null,snName,CopyCheckJob.RECOVERY_CRC,r.getPartNum(),r.getDataStartTime(),r.getDataStopTime(),granule);
					emap.put(key, atom);
				}
				atom = emap.get(key);
				atom.addAllFiles(r.getFiles());
			}
		}
		List<AtomTaskModel> tList = filterError(rmap, snMap);
		if(tList == null|| tList.isEmpty()) {
			return null;
		}
		TaskModel tTask = new TaskModel();
		tTask.setCreateTime(TimeUtils.formatTimeStamp(System.currentTimeMillis(), TimeUtils.TIME_MILES_FORMATE));
		tTask.setAtomList(tList);
		tTask.setTaskState(TaskState.INIT.code());
		tTask.setTaskType(TaskType.SYSTEM_COPY_CHECK.code());
		return  tTask;
	}
	/**
	 * 概述：收集可执行子任务
	 * @param rmap
	 * @param snCountMap
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static List<AtomTaskModel> filterError(Map<String,Map<String,AtomTaskModel>> rmap , Map<String,Integer>snCountMap){
		if(rmap == null || rmap.isEmpty() || snCountMap == null || snCountMap.isEmpty()) {
			return null;
		}
		String snName;
		Map<String,AtomTaskModel> sMap;
		int count;
		List<AtomTaskModel> atoms = new ArrayList<>();
		List<AtomTaskModel> tmp;
		for(Map.Entry<String, Integer> entry : snCountMap.entrySet()) {
			snName = entry.getKey();
			count = entry.getValue();
			if(count == 1) {
				continue;
			}
			if(!rmap.containsKey(snName)) {
				continue;
			}
			sMap = rmap.get(snName);
			if(sMap == null || sMap.isEmpty()) {
				continue;
			}
			tmp = collectAtoms(sMap, count);
			if(tmp == null || tmp.isEmpty()) {
				continue;
			}
			atoms.addAll(tmp);
		}
		return atoms;
	}
	/**
	 * 概述：收集子任务
	 * @param sMap
	 * @param count
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static List<AtomTaskModel> collectAtoms(Map<String,AtomTaskModel> sMap, int count){
		if(sMap == null || sMap.isEmpty() || count <=1) {
			return null;
		}
		List<String> eFiles;
		List<AtomTaskModel> atoms = new ArrayList<>();
		for(AtomTaskModel atom : sMap.values()) {
			eFiles = atom.getFiles();
			if(eFiles == null || eFiles.isEmpty()) {
				continue;
			}
			eFiles = collectFiles(eFiles, count);
			if(eFiles == null|| eFiles.isEmpty()) {
				continue;
			}
			atom.getFiles().clear();
			atom.setFiles(eFiles);
			atoms.add(atom);
		}
		return atoms;
	}
	/**
	 * 概述：收集可恢复的文件
	 * @param files
	 * @param count
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static List<String> collectFiles(List<String> files, int count){
		if(files == null || files.isEmpty()) {
			return null;
		}
		Map<String,Integer> cMap = new HashMap<>();
		for(String file :files) {
			if(cMap.containsKey(file)) {
				cMap.put(file, cMap.get(file) +1);
			} else {
				cMap.put(file, 1);
			}
		}
		if(cMap.isEmpty()) {
			return null;
		}
		return collectionFiles(cMap, count);
	}
	/**
	 * 概述：收集可恢复的文件
	 * @param cmap
	 * @param replicationCount
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static List<String> collectionFiles(Map<String,Integer> cmap, int replicationCount){
		if(cmap == null || cmap.isEmpty()) {
			return null;
		}
		String fileName;
		int count;
		List<String> files = new ArrayList<>();
		for(Map.Entry<String, Integer> entry : cmap.entrySet()) {
			fileName = entry.getKey();
			count = entry.getValue();
			if(count >= replicationCount) {
				continue;
			}
			files.add(fileName);
		}
		return files;
	}
}
