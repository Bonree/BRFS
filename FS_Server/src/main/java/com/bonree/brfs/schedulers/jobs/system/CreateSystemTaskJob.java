package com.bonree.brfs.schedulers.jobs.system;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import com.bonree.brfs.common.utils.Pair;
import com.bonree.brfs.common.utils.StorageNameFileUtils;
import com.bonree.brfs.common.utils.TimeUtils;
import com.bonree.brfs.duplication.storagename.StorageNameManager;
import com.bonree.brfs.duplication.storagename.StorageNameNode;
import com.bonree.brfs.schedulers.ManagerContralFactory;
import com.bonree.brfs.schedulers.jobs.JobDataMapConstract;
import com.bonree.brfs.schedulers.jobs.biz.WatchSomeThingJob;
import com.bonree.brfs.schedulers.task.manager.MetaTaskManagerInterface;
import com.bonree.brfs.schedulers.task.model.AtomTaskModel;
import com.bonree.brfs.schedulers.task.model.TaskModel;
import com.bonree.brfs.schedulers.task.model.TaskServerNodeModel;
import com.bonree.brfs.schedulers.task.model.TaskTypeModel;
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
		LOG.info("-------> create system task working");
		//判断是否有恢复任务，有恢复任务则不进行创建
		JobDataMap data = context.getJobDetail().getJobDataMap();
		long checkTtl = data.getLong(JobDataMapConstract.CHECK_TTL);
		long gsnTtl = data.getLong(JobDataMapConstract.GLOBAL_SN_DATA_TTL);
		ManagerContralFactory mcf = ManagerContralFactory.getInstance();
		MetaTaskManagerInterface release = mcf.getTm();
		// 获取开启的任务名称
		List<TaskType> switchList = mcf.getTaskOn();
		if(switchList==null || switchList.isEmpty()){
			LOG.warn("switch on task is empty !!!");
			return;
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
		if(snList == null || snList.isEmpty()) {
			LOG.info("SKIP create system task !!! because storageName is null !!!");
			return;
		}
		TaskModel task = null;
		String taskName = null;
		TaskTypeModel tmodel = null;
		long ttl = 0;
		Pair<TaskModel,TaskTypeModel> result = null;
		for(TaskType taskType : switchList){
			if(TaskType.SYSTEM_COPY_CHECK.equals(taskType)||TaskType.USER_DELETE.equals(taskType)) {
				continue;
			}
			if(TaskType.SYSTEM_DELETE.equals(taskType)) {
				ttl = 0;
			}else if(TaskType.SYSTEM_CHECK.equals(taskType)) {
				ttl = checkTtl;
			}
			tmodel = release.getTaskTypeInfo(taskType.name());
			result = CreateSystemTask.createSystemTask(tmodel,taskType, snList, 3600000, ttl);
			if(result == null) {
				continue;
			}
			task = result.getKey();
			taskName = updateTask(release, task, serverIds, taskType);
			if(!BrStringUtils.isEmpty(taskName)) {
				LOG.info("create {} {} task successfull !!!", taskType.name(), taskName);
				release.setTaskTypeModel(taskType.name(), result.getValue());
			}
		}
	}
	/**
	 * 概述：将任务信息创建到zk
	 * @param release
	 * @param task
	 * @param serverIds
	 * @param taskType
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	private String updateTask( MetaTaskManagerInterface release, TaskModel task, List<String> serverIds, TaskType taskType) {
		if (task == null) {
			LOG.warn(" task create is null skip ");
			return null;
		}
		String taskName = release.updateTaskContentNode(task, taskType.name(), null);
		if (taskName == null) {
			LOG.warn("create task error : taskName is empty");
			return null;
		}
		TaskServerNodeModel sTask = TaskServerNodeModel.getInitInstance();
		for (String serviceId : serverIds) {
			release.updateServerTaskContentNode(serviceId, taskName, taskType.name(), sTask);
		}
		return taskName;
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
