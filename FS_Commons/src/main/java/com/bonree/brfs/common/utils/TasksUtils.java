package com.bonree.brfs.common.utils;

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

import com.bonree.brfs.common.schedulers.model.AtomTaskModel;
import com.bonree.brfs.common.schedulers.model.TaskMessageModel;
import com.bonree.brfs.common.schedulers.model.TaskModel;
import com.bonree.brfs.common.schedulers.model.TaskSNMessageModel;
import com.bonree.brfs.common.schedulers.model.impl.QuartzSimpleInfo;
import com.bonree.brfs.common.schedulers.task.TaskStat;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.service.impl.DefaultServiceManager;

public class TasksUtils {
	/**
	 * 概述：解析开关工具
	 * @param data
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static List<String> getSwitchTasks(JobDataMap data){
		List<String> tasks = new ArrayList<String>();
		String taskSwitches = null;
		if(!data.containsKey("TASK_SWITCH_ON")){
			return tasks;
		}
		taskSwitches = data.getString("TASK_SWITCH_ON");
		if(!BrStringUtils.isEmpty(taskSwitches)){
			String[] taskTypes = BrStringUtils.getSplit(taskSwitches, ",");
			for(String ele : taskTypes){
				if(BrStringUtils.isEmpty(ele)){
					continue;
				}
				tasks.add(ele);
			}
		}
		return tasks;		
	}
	/**
	 * 概述：解析任务的执行类引用
	 * @param data
	 * @param tasks
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static Map<String, String> getSwitchTaskClass(JobDataMap data, List<String> tasks){
		Map<String,String> taskClassMap = new HashMap<String,String>();
		if(data == null){
			return taskClassMap;
		}
		if(tasks == null || tasks.isEmpty()){
			return taskClassMap;
		}
		String clazz = null;
		for(String task : tasks){
			clazz = data.getString(task);
			if(BrStringUtils.isEmpty(clazz)){
				continue;
			}
			taskClassMap.put(task, clazz);
		}
		return taskClassMap;
	}
	/**
	 * 概述：解析任务的执行类引用
	 * @param data
	 * @param tasks
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static Map<String, String> getSwitchTaskData(JobDataMap data, List<String> tasks){
		Map<String,String> taskDataMap = new HashMap<String,String>();
		if(data == null){
			return taskDataMap;
		}
		if(tasks == null || tasks.isEmpty()){
			return taskDataMap;
		}
		String taskContent = null;
		for(String task : tasks){
			if(BrStringUtils.isEmpty(task)){
				continue;
			}
			taskContent = data.getString(task+"_data");
			if(BrStringUtils.isEmpty(taskContent)){
				continue;
			}
			taskDataMap.put(task, taskContent);
		}
		return taskDataMap;
	}
	/**
	 * 概述：设置任务执行类引用
	 * @param simple
	 * @param task
	 * @param taskClass
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static void setSwitchTaskClass(QuartzSimpleInfo simple, String task, String taskClass){
		if(simple == null){
			throw new NullPointerException("taskInfo is null");
		}
		if(BrStringUtils.isEmpty(task)){
			throw new NullPointerException("task name is null");
		}
		if(BrStringUtils.isEmpty(taskClass)){
			throw new NullPointerException("task Class is null");
		}
		simple.putContent(task, taskClass);
	}
	/**
	 * 概述：设置task引用class
	 * @param simple
	 * @param taskClassMap
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static void setSwitcheTaskClass(QuartzSimpleInfo simple, Map<String,String> taskClassMap){
		if(simple == null){
			throw new NullPointerException("taskInfo is null");
		}
		if(taskClassMap == null || taskClassMap.isEmpty()){
			throw new NullPointerException("task name is null");
		}
		simple.setTaskContent(taskClassMap);
	}
	/**
	 * 概述：设置任务启动参数
	 * @param simple
	 * @param task
	 * @param taskContent
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static void setSTaskData(QuartzSimpleInfo simple, String task, String taskContent){
		if(simple == null){
			throw new NullPointerException("taskInfo is null");
		}
		if(BrStringUtils.isEmpty(task)){
			throw new NullPointerException("task name is null");
		}
		if(BrStringUtils.isEmpty(taskContent)){
			throw new NullPointerException("task Content is null");
		}
		String taskDataKey = task+"_data";
		simple.putContent(taskDataKey, taskContent);
	}
	/**
	 * 概述：获取可用server
	 * @param zkUrl
	 * @param groupName
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static List<String> getServerIds(String zkUrl, String groupName){
		List<String> serverIds = new ArrayList<String>();
		CuratorFramework client = null;
		try {
			if(BrStringUtils.isEmpty(zkUrl)){
				return serverIds;
			}
			if(BrStringUtils.isEmpty(groupName)){
				return serverIds;
			}
			RetryPolicy retryPolicy = new RetryNTimes(3, 1000);
			client = CuratorFrameworkFactory.newClient(zkUrl, retryPolicy);
			ServiceManager sManager = new DefaultServiceManager(client);
			List<Service> serverList = sManager.getServiceListByGroup(groupName);
			for(Service server : serverList){
				serverIds.add(server.getServiceId());
			}
		}
		catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			if(client != null){
				client.close();
			}
		}
		return serverIds;
	}
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
		task.setTaskState(TaskStat.INIT.code());
		List<AtomTaskModel> storageAtoms = new ArrayList<AtomTaskModel>();
		AtomTaskModel atom = null;
		
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
	public static List<AtomTaskModel> createAtomTaskModel(final String sn, final String opertationContent, final long startTime, final long endTime){
		List<AtomTaskModel> storageAtoms = new ArrayList<AtomTaskModel>();
		if(startTime == 0 || endTime == 0 || startTime > endTime){
			return storageAtoms;
		}
		if(BrStringUtils.isEmpty(sn)){
			return storageAtoms;
		}
		long startHour = DateFormatUtils.interceptHourTime(startTime);
		long endHour = DateFormatUtils.interceptHourTime(endTime);
		AtomTaskModel atom = null;
		String dirName = null;;
		for(long tmpTime = startHour; tmpTime < endHour; tmpTime = DateFormatUtils.addMins(tmpTime, 60l) ){
			atom = new AtomTaskModel();
			atom.setStorageName(sn);
			atom.setTaskOperation(opertationContent);
			dirName = DateFormatUtils.format(tmpTime, DateFormatUtils.FORMAT_YEAR_TO_HOUR);
			atom.setDirName(dirName);
			storageAtoms.add(atom);
		}
		return storageAtoms;
	}
}
