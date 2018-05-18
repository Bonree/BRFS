package com.bonree.brfs.schedulers.jobs.system;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.curator.RetryPolicy;
import org.apache.curator.RetrySleeper;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.eclipse.jetty.io.ssl.ALPNProcessor.Server;
import org.joda.time.DateTime;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.UnableToInterruptJobException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.service.impl.DefaultServiceManager;
import com.bonree.brfs.common.task.TaskState;
import com.bonree.brfs.common.task.TaskType;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.common.utils.StorageNameFileUtils;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.duplication.storagename.StorageNameManager;
import com.bonree.brfs.duplication.storagename.StorageNameNode;
import com.bonree.brfs.schedulers.ManagerContralFactory;
import com.bonree.brfs.schedulers.jobs.JobDataMapConstract;
import com.bonree.brfs.schedulers.task.TasksUtils;
import com.bonree.brfs.schedulers.task.manager.MetaTaskManagerInterface;
import com.bonree.brfs.schedulers.task.meta.impl.QuartzSimpleInfo;
import com.bonree.brfs.schedulers.task.model.AtomTaskModel;
import com.bonree.brfs.schedulers.task.model.TaskModel;
import com.bonree.brfs.schedulers.task.model.TaskServerNodeModel;
import com.bonree.brfs.schedulers.task.operation.impl.QuartzOperationStateTask;

public class CreateSystemTaskJob extends QuartzOperationStateTask {
	private static final Logger LOG = LoggerFactory.getLogger("CreateSysTask");
	@Override
	public void caughtException(JobExecutionContext context) {
		LOG.info("------------create sys task happened Exception !!!-----------------");
	}

	@Override
	public void interrupt() throws UnableToInterruptJobException {
		LOG.info(" happened Interrupt !!!");
		
	}

	@Override
	public void operation(JobExecutionContext context) throws Exception {
		ManagerContralFactory mcf = ManagerContralFactory.getInstance();
		JobDataMap data = context.getJobDetail().getJobDataMap();
		String path = data.getString(JobDataMapConstract.DATA_PATH);
		if(BrStringUtils.isEmpty(path)){
			throw new NullPointerException("data path is empty !!!");
		}
		int lastIndex = path.lastIndexOf("/");
		if(lastIndex >0){
			path = path.substring(0, lastIndex);
		}
		lastIndex = path.lastIndexOf("\\");
		if(lastIndex >0){
			path = path.substring(0, lastIndex);
		}
		MetaTaskManagerInterface release = mcf.getTm();
		// 获取开启的任务名称
		List<TaskType> switchList = mcf.getTaskOn();
		if(switchList==null || switchList.isEmpty()){
			throw new NullPointerException("switch on task is empty !!!");
		}
		// 获取可用服务
		String groupName = mcf.getGroupName();
		ServiceManager sm = mcf.getSm();
		// 2.设置可用服务
		List<String> serverIds = getServerIds(sm, groupName);
		if(serverIds == null || serverIds.isEmpty()){
			throw new NullPointerException(groupName + " available server list is null");
		}
		// 3.获取storageName
		StorageNameManager snm = mcf.getSnm();
		List<StorageNameNode> snList = snm.getStorageNameNodeList();
		List<AtomTaskModel> snAtomTaskList = new ArrayList<AtomTaskModel>();
		long currentTime = System.currentTimeMillis();
		TaskModel task = null;
		byte[] byteData = null;
		String taskName = null;
		String prexTaskName = null;
		TaskModel prexTask = null;
		List<String> taskList = null;
		long preCreateTime = 0l;
		for(TaskType taskType : switchList){
			//检查创建的时间间隔是否达到一小时
			taskList = release.getTaskList(taskType.name());
			if(taskList != null && !taskList.isEmpty()){
				prexTaskName = taskList.get(taskList.size() - 1);
			}
			if(!BrStringUtils.isEmpty(prexTaskName)){
				prexTask = release.getTaskContentNodeInfo(taskType.name(), prexTaskName);
			}
			if(prexTask != null){
				preCreateTime = prexTask.getCreateTime();
			}
			if(currentTime - preCreateTime< 60*60*1000 ){
				LOG.info("skip create {} task", taskType.name());
				continue;
			}
			if(TaskType.SYSTEM_DELETE.equals(taskType)){
				//创建删除任务
				task = createTaskModel(snList, taskType, currentTime, path, "");
			}else if(TaskType.SYSTEM_CHECK.equals(taskType)){
				task = createTaskModel(snList, taskType, currentTime, path, "");
			}else{
				continue;
			}
			// 任务为空，跳过
			if(task == null){
				continue;
			}
			taskName = release.updateTaskContentNode(task, taskType.name(), null);
			if(taskName == null){
				continue;
			}
			TaskServerNodeModel sTask = null;
			for(String serviceId : serverIds){
				sTask = createServerNodeModel();
				release.updateServerTaskContentNode(serviceId, taskName, taskType.name(), sTask);
				LOG.info("=======>create s task {} - {} - {} - {} ",taskType, taskName,serviceId, TaskState.valueOf(sTask.getTaskState()).name());
			}
			LOG.info("=======>create task {} - {} - {} ",taskType, taskName, TaskState.valueOf(task.getTaskState()).name());
		}
	}
	public TaskServerNodeModel createServerNodeModel(){
		TaskServerNodeModel task = new TaskServerNodeModel();
		task.setTaskState(TaskState.INIT.code());
		return task;
	}
	/**
	 * 概述：生成任务信息
	 * @param snList
	 * @param taskType
	 * @param currentTime
	 * @param dataPath
	 * @param taskOperation
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public TaskModel createTaskModel(List<StorageNameNode> snList,TaskType taskType, long currentTime,String dataPath,String taskOperation){
		TaskModel task = new TaskModel();
		task.setCreateTime(System.currentTimeMillis());
		task.setTaskState(TaskState.INIT.code());
		task.setTaskType(taskType.code());
		long operationDirTime = 0;
		long creatTime = 0;
		long ttl = 0;
		List<AtomTaskModel> atoms = new ArrayList<AtomTaskModel>();
		List<AtomTaskModel> cAtoms = null;
		for(StorageNameNode snn : snList){
			creatTime = snn.getCreateTime();
			ttl = snn.getTtl();
			//系统删除任务判断
			if(TaskType.SYSTEM_DELETE.equals(taskType) && (currentTime - creatTime) < ttl){
				continue;
			}
			operationDirTime =currentTime - ttl;
			cAtoms = createAtomTaskModel(snn, dataPath, operationDirTime, taskOperation);
			if(cAtoms == null || cAtoms.isEmpty()){
				continue;
			}
			atoms.addAll(cAtoms);
		}
		if(atoms == null || atoms.isEmpty()){
			return null;
		}
		task.setAtomList(atoms);
		return task;
	}
	
	/**
	 * 概述：生成基本任务信息
	 * @param sn
	 * @param dataPath
	 * @param time
	 * @param taskOperation
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	private List<AtomTaskModel> createAtomTaskModel(StorageNameNode sn, String dataPath, final long time, String taskOperation){
		List<AtomTaskModel> atomList = new ArrayList<AtomTaskModel>();
		AtomTaskModel atom = null;
		int copyCount = sn.getReplicateCount();
		String path = null;
		String snName = sn.getName();
		for(int i = 1; i <= copyCount; i++){
			atom = new AtomTaskModel();
			atom.setStorageName(snName);
			atom.setTaskOperation(taskOperation);
			path = StorageNameFileUtils.createSNDir(snName, dataPath, i, time);
			if(BrStringUtils.isEmpty(path)){
				LOG.warn("sn {} create dir error !! path is empty !!!", snName);
				continue;
			}
			atom.setDirName(path);
			atomList.add(atom);
		}
		return atomList;
	}
	/***
	 * 概述：获取存活的serverid
	 * @param sm
	 * @param groupName
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	private List<String> getServerIds(ServiceManager sm, String groupName){
		List<String> sids = new ArrayList<>();
		List<Service> sList = sm.getServiceListByGroup(groupName);
		if(sList == null || sList.isEmpty()){
			return sids;
		}
		String sid = null;
		for(Service server : sList){
			sid = server.getServiceId();
			if(BrStringUtils.isEmpty(sid)){
				continue;
			}
			sids.add(sid);
		}
		return sids;
	}
	
	

}
