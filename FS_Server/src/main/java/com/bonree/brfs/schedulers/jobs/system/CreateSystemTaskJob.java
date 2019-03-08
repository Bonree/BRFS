package com.bonree.brfs.schedulers.jobs.system;

import java.util.List;

import com.bonree.brfs.configuration.units.ResourceConfigs;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.task.TaskType;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.utils.Pair;
import com.bonree.brfs.duplication.storageregion.StorageRegion;
import com.bonree.brfs.duplication.storageregion.StorageRegionManager;
import com.bonree.brfs.schedulers.ManagerContralFactory;
import com.bonree.brfs.schedulers.task.manager.MetaTaskManagerInterface;
import com.bonree.brfs.schedulers.task.model.TaskModel;
import com.bonree.brfs.schedulers.task.model.TaskTypeModel;
import com.bonree.brfs.schedulers.task.operation.impl.QuartzOperationStateTask;
import com.bonree.brfs.schedulers.utils.CreateSystemTask;
import com.bonree.brfs.schedulers.utils.JobDataMapConstract;
import com.bonree.brfs.schedulers.utils.TaskStateLifeContral;
import com.bonree.brfs.schedulers.utils.TasksUtils;

public class CreateSystemTaskJob extends QuartzOperationStateTask {
	private static final Logger LOG = LoggerFactory.getLogger(CreateSystemTaskJob.class);
	@Override
	public void caughtException(JobExecutionContext context) {
	}

	@Override
	public void interrupt(){
	}

	@Override
	public void operation(JobExecutionContext context){
		LOG.info("create system task working");
		//判断是否有恢复任务，有恢复任务则不进行创建
		JobDataMap data = context.getJobDetail().getJobDataMap();
		long checkTtl = data.getLong(JobDataMapConstract.CHECK_TTL);
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
		List<String> serverIds = CreateSystemTask.getServerIds(sm, groupName);
		if(serverIds == null || serverIds.isEmpty()){
			LOG.warn("{} available server list is null", groupName);
			return;
		}
		// 3.获取storageName
		StorageRegionManager snm = mcf.getSnm();
		List<StorageRegion> snList = snm.getStorageRegionList();
		if(snList == null || snList.isEmpty()) {
			LOG.info("skip create system task !!! because storageName is null !!!");
			return;
		}
		TaskModel task;
		String taskName;
		TaskTypeModel tmodel;
		long ttl = 0;
		Pair<TaskModel,TaskTypeModel> result;
		List<String> srs = TaskStateLifeContral.getSRs(snm);
		for(TaskType taskType : switchList){
			if(TaskType.SYSTEM_COPY_CHECK.equals(taskType)||TaskType.USER_DELETE.equals(taskType)) {
				continue;
			}
			TaskStateLifeContral.watchSR(release, srs, taskType.name());
			if(TaskType.SYSTEM_DELETE.equals(taskType)) {
				ttl = 0;
			}else if(TaskType.SYSTEM_CHECK.equals(taskType)) {
				ttl = checkTtl;
			}
			tmodel = release.getTaskTypeInfo(taskType.name());
			if(tmodel == null){
				tmodel = new TaskTypeModel();
				tmodel.setSwitchFlag(true);
				LOG.warn("taskType{} is switch but metadata is null");
			}
			result = CreateSystemTask.createSystemTask(tmodel,taskType, snList, ttl);
			if(result == null) {
				LOG.warn("create sys task is empty {}",taskType.name());
				continue;
			}
			task = result.getFirst();
			taskName = CreateSystemTask.updateTask(release, task, serverIds, taskType);
			if(!BrStringUtils.isEmpty(taskName)) {
				LOG.info("create {} {} task successfull !!!", taskType.name(), taskName);
				release.setTaskTypeModel(taskType.name(), tmodel);
			}
		}
	}
	
}
