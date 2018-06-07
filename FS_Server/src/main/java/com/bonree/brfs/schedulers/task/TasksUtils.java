package com.bonree.brfs.schedulers.task;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.quartz.JobDataMap;

import com.bonree.brfs.common.ReturnCode;
import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.service.impl.DefaultServiceManager;
import com.bonree.brfs.common.task.TaskState;
import com.bonree.brfs.common.task.TaskType;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.common.utils.TimeUtils;
import com.bonree.brfs.configuration.ServerConfig;
import com.bonree.brfs.disknode.server.handler.data.FileInfo;
import com.bonree.brfs.duplication.coordinator.FileNode;
import com.bonree.brfs.duplication.storagename.StorageNameNode;
import com.bonree.brfs.schedulers.jobs.system.CreateSystemTask;
import com.bonree.brfs.schedulers.task.manager.MetaTaskManagerInterface;
import com.bonree.brfs.schedulers.task.manager.impl.DefaultReleaseTask;
import com.bonree.brfs.schedulers.task.meta.impl.QuartzSimpleInfo;
import com.bonree.brfs.schedulers.task.model.AtomTaskModel;
import com.bonree.brfs.schedulers.task.model.TaskModel;
import com.bonree.brfs.schedulers.task.model.TaskServerNodeModel;
import com.bonree.brfs.schedulers.task.model.TaskTypeModel;

public class TasksUtils {
	/**
	 * 概述：
	 * @param services
	 * @param serverConfig
	 * @param zkPaths
	 * @param sn
	 * @param startTime
	 * @param endTime
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	 public static ReturnCode createUserDeleteTask(List<Service> services,ServerConfig serverConfig, ZookeeperPaths zkPaths, StorageNameNode sn, long startTime, long endTime){
		 	if(services == null || services.isEmpty()){
		 		return ReturnCode.DELETE_DATA_ERROR;
		 	}
		 	if(sn == null){
		 		return ReturnCode.STORAGE_NONEXIST_ERROR;
		 	}
		 	ReturnCode code = checkTime(startTime, endTime, sn.getCreateTime(), 3600000);
		 	MetaTaskManagerInterface release = DefaultReleaseTask.getInstance();
		 	release.setPropreties(serverConfig.getZkHosts(), zkPaths.getBaseTaskPath(), zkPaths.getBaseLocksPath());
	    	TaskTypeModel tmodel = release.getTaskTypeInfo(TaskType.USER_DELETE.name());
	    	if(!tmodel.isSwitchFlag()) {
	    		return ReturnCode.FORBID_DELETE_DATA_ERROR;
	    	}
	    	TaskModel task = TasksUtils.createUserDelete(sn, TaskType.USER_DELETE, "", startTime, endTime);
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
	public static TaskModel createUserDelete(final StorageNameNode sn,final TaskType taskType, final String opertationContent, final long startTime, final long endTime){
		if(sn == null ||taskType == null){
			return null;
		}
		TaskModel task = new TaskModel();
		List<AtomTaskModel> storageAtoms = new ArrayList<AtomTaskModel>();
		if(endTime == 0 || startTime >= endTime){
			return null;
		}	
		AtomTaskModel atom = null;
		String dirName = null;
		String snName = sn.getName();
		int count = sn.getReplicateCount();
		long startHour = 0;
		long endHour =  endTime/1000/60/60*60*60*1000;
		if(startTime <=0 || startTime < sn.getCreateTime()){
			startHour = sn.getCreateTime()/1000/60/60*60*60*1000;
		}else{
			startHour = startTime/1000/60/60*60*60*1000;
		}
		// 若是删除sn时，创建时间与删除时间间隔较短时，结束时间向后退
		if(startHour == endHour && startTime <=0) {
			endHour = endHour + 3600000;
		}
		if(startHour >= endHour) {
			return null;
		}
		
		for(int i = 1; i<=count; i++){
			atom = new AtomTaskModel();
			atom.setStorageName(snName);
			atom.setTaskOperation(opertationContent);
			atom.setDirName(i+"");
			atom.setDataStartTime(TimeUtils.formatTimeStamp(startHour, TimeUtils.TIME_MILES_FORMATE));
			atom.setDataStopTime(TimeUtils.formatTimeStamp(endHour, TimeUtils.TIME_MILES_FORMATE));
			storageAtoms.add(atom);
		}
		task.setAtomList(storageAtoms);
		task.setTaskState(TaskState.INIT.code());
		task.setCreateTime(TimeUtils.formatTimeStamp(System.currentTimeMillis(), TimeUtils.TIME_MILES_FORMATE));
		task.setTaskType(taskType.code());
		return task;
	}
	
	/***
	 * 概述：检测时间
	 * @param startTime
	 * @param endTime
	 * @param cTime
	 * @param granule
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static ReturnCode checkTime(long startTime, long endTime, long cTime, long granule) {
		// 1，时间格式不对
		if(startTime != (startTime - startTime%granule)
				|| endTime !=(endTime - endTime%granule)) {
			return ReturnCode.TIME_FORMATE_ERROR;
		}
		long currentTime = System.currentTimeMillis();
		long cuGra = currentTime - currentTime%granule;
		long sGra = startTime - startTime%granule;
		long eGra = endTime - endTime%granule;
		long cGra = cTime - cTime%granule;
		// 2.开始时间等于结束世界
		if(sGra >= eGra) {
			return ReturnCode.PARAMETER_ERROR;
		}
		// 3.开始时间，结束时间小于创建时间
		if(cGra >sGra ||cGra >eGra) {
			return ReturnCode.TIME_EARLIER_THAN_CREATE_ERROR;
		}
		// 4.当前时间
		if(cuGra <= sGra || cuGra<eGra) {
			return ReturnCode.FORBID_DELETE_CURRENT_ERROR;
		}
		// 若成功则返回null
		return ReturnCode.SUCCESS;
	}
}
