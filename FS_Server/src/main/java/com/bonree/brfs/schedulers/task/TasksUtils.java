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

import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.service.impl.DefaultServiceManager;
import com.bonree.brfs.common.task.TaskState;
import com.bonree.brfs.common.task.TaskType;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.common.utils.TimeUtils;
import com.bonree.brfs.duplication.storagename.StorageNameNode;
import com.bonree.brfs.schedulers.task.manager.MetaTaskManagerInterface;
import com.bonree.brfs.schedulers.task.manager.impl.DefaultReleaseTask;
import com.bonree.brfs.schedulers.task.meta.impl.QuartzSimpleInfo;
import com.bonree.brfs.schedulers.task.model.AtomTaskModel;
import com.bonree.brfs.schedulers.task.model.TaskMessageModel;
import com.bonree.brfs.schedulers.task.model.TaskModel;
import com.bonree.brfs.schedulers.task.model.TaskSNMessageModel;
import com.bonree.brfs.schedulers.task.model.TaskServerNodeModel;

public class TasksUtils {
	
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
	public static TaskModel createTaskModel(final StorageNameNode sn,final TaskType taskType, final String opertationContent, final long startTime, final long endTime){
		TaskModel task = new TaskModel();
		List<AtomTaskModel> storageAtoms = new ArrayList<AtomTaskModel>();
		if(endTime == 0 || startTime > endTime){
			return null;
		}
		long startHour = 0;
		if(startTime <=0 || startTime < sn.getCreateTime()){
			startHour = sn.getCreateTime()/1000/60/60*60*60*1000;
		}else{
			startHour = startTime/1000/60/60*60*60*1000;
		}
		long endHour = endTime/1000/60/60*60*60*1000;
		AtomTaskModel atom = null;
		String dirName = null;
		String snName = sn.getName();
		int count = sn.getReplicateCount();
		for(long tmpTime = startHour; tmpTime < endHour; tmpTime+= 60*60*1000 ){
			for(int i = 0; i<count;i++){
				atom = new AtomTaskModel();
				atom.setStorageName(snName);
				atom.setTaskOperation(opertationContent);
				dirName = TimeUtils.timeInterval(tmpTime, 60*60*1000);
				atom.setDirName(snName+File.separator+i+File.separator+dirName);
				storageAtoms.add(atom);
			}
		}
		task.setAtomList(storageAtoms);
		task.setTaskState(TaskState.INIT.code());
		task.setCreateTime(System.currentTimeMillis());
		task.setTaskType(taskType.code());
		task.setStartDataTime(startHour);
		task.setEndDataTime(endHour);
		return task;
	}
	public static TaskServerNodeModel createServerTaskNode(){
		TaskServerNodeModel server = new TaskServerNodeModel();
		server.setTaskState(TaskState.INIT.code());
		return server;
	}
	public static void main(String[] args) {
		StorageNameNode sn = new StorageNameNode();
		sn.setCreateTime(System.currentTimeMillis() - 7*24*60*60*1000*52);
		sn.setEnable(true);
		sn.setId(1);
		sn.setName("Test");
		sn.setReplicateCount(2);
		sn.setTtl(3600000);
		TaskModel task = createTaskModel(sn, TaskType.USER_DELETE, "", -1, System.currentTimeMillis());
		
		MetaTaskManagerInterface release = DefaultReleaseTask.getInstance();
		release.setPropreties("192.168.101.86:2181", "/Test/tasks", "/Test/lock");
		//创建任务节点
		String taskName = release.updateTaskContentNode(task, TaskType.USER_DELETE.name(), null);
		TaskServerNodeModel serverModel = createServerTaskNode();
		//创建服务节点
		String[] services = new String[]{
			"1","2","3","4"	
		};
		for(String serverId : services){
			release.updateServerTaskContentNode(serverId, taskName, TaskType.USER_DELETE.name(), serverModel);
		}
		
	}
}
