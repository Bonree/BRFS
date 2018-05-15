package com.bonree.brfs.schedulers.task;

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

import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.service.impl.DefaultServiceManager;
import com.bonree.brfs.common.task.TaskState;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.utils.TimeUtils;
import com.bonree.brfs.schedulers.task.meta.impl.QuartzSimpleInfo;
import com.bonree.brfs.schedulers.task.model.AtomTaskModel;
import com.bonree.brfs.schedulers.task.model.TaskMessageModel;
import com.bonree.brfs.schedulers.task.model.TaskModel;
import com.bonree.brfs.schedulers.task.model.TaskSNMessageModel;

public class TasksUtils {
	
	/**
	 * 概述：将taskmessage转换为可执行的任务模型
	 * @param message
	 * @param storageNames
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static TaskModel converTask(TaskMessageModel message, Collection<String> storageNames){
		if(message == null || storageNames == null|| storageNames.isEmpty()){
			return null;
		}
		TaskModel  task = new TaskModel();
		task.setTaskType(message.getTaskType());
		task.setCreateTime(System.currentTimeMillis());
		task.setTaskState(TaskState.INIT.code());
		List<AtomTaskModel> storageAtoms = new ArrayList<AtomTaskModel>();
		
		List<TaskSNMessageModel> snList = message.getStorageMessages();
		if(snList == null || snList.isEmpty()){
			return null;
		}
		
		if(message.getIncidence() == 0){
			for(TaskSNMessageModel model : snList){
				long startTime = model.getStartTime();
				long endTime = model.getEndTime();
				String sn = model.getStorageName();
				if(!storageNames.contains(sn)){
					continue;
				}
				String operationContent = model.getOperationContent();
				storageAtoms.addAll(createAtomTaskModel(sn, operationContent, startTime, endTime));
			}
		} else {
			TaskSNMessageModel tmp = snList.get(0);
			long startTime = tmp.getStartTime();
			long endTime = tmp.getEndTime();
			String operationContent = tmp.getOperationContent();
			for(String sn : storageNames){
				storageAtoms.addAll(createAtomTaskModel(sn, operationContent, startTime, endTime));
			}
		}
		if(storageAtoms == null || storageAtoms.isEmpty()){
			return null;
		}
		task.setAtomList(storageAtoms);
		return task;
	}
	/**
	 * 概述：转化为可执行的sn任务
	 * @param sn
	 * @param opertationContent
	 * @param startTime
	 * @param endTime
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	//TODO:要与俞朋的保持一致
	public static List<AtomTaskModel> createAtomTaskModel(final String sn, final String opertationContent, final long startTime, final long endTime){
		List<AtomTaskModel> storageAtoms = new ArrayList<AtomTaskModel>();
		if(startTime == 0 || endTime == 0 || startTime > endTime){
			return storageAtoms;
		}
		if(BrStringUtils.isEmpty(sn)){
			return storageAtoms;
		}
		long startHour = startTime/1000/60/60*60*60*1000;
		long endHour = endTime/1000/60/60*60*60*1000;
		AtomTaskModel atom = null;
		String dirName = null;;
		for(long tmpTime = startHour; tmpTime < endHour; tmpTime+= 60*60*1000 ){
			atom = new AtomTaskModel();
			atom.setStorageName(sn);
			atom.setTaskOperation(opertationContent);
			dirName = TimeUtils.timeInterval(tmpTime, 60*60*1000);
			atom.setDirName(dirName);
			storageAtoms.add(atom);
		}
		return storageAtoms;
	}
}
