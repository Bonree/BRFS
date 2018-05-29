package com.bonree.brfs.schedulers.jobs.system;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.net.ssl.ManagerFactoryParameters;

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
import com.bonree.brfs.common.utils.TimeUtils;
import com.bonree.brfs.configuration.ServerConfig;
import com.bonree.brfs.disknode.client.DiskNodeClient;
import com.bonree.brfs.disknode.client.HttpDiskNodeClient;
import com.bonree.brfs.disknode.server.handler.data.FileInfo;
import com.bonree.brfs.duplication.storagename.StorageNameManager;
import com.bonree.brfs.duplication.storagename.StorageNameNode;
import com.bonree.brfs.schedulers.ManagerContralFactory;
import com.bonree.brfs.schedulers.jobs.JobDataMapConstract;
import com.bonree.brfs.schedulers.jobs.biz.WatchSomeThingJob;
import com.bonree.brfs.schedulers.task.manager.MetaTaskManagerInterface;
import com.bonree.brfs.schedulers.task.model.AtomTaskModel;
import com.bonree.brfs.schedulers.task.model.TaskModel;
import com.bonree.brfs.schedulers.task.model.TaskServerNodeModel;
import com.bonree.brfs.schedulers.task.operation.impl.QuartzOperationStateTask;

public class CopyCheckJob extends QuartzOperationStateTask{
	private static final Logger LOG = LoggerFactory.getLogger("CopyCheckJob");
	@Override
	public void caughtException(JobExecutionContext context) {
		LOG.error("Create Task error !! {}",TaskType.SYSTEM_COPY_CHECK.name());
	}

	@Override
	public void interrupt() throws UnableToInterruptJobException {
		
	}

	@Override
	public void operation(JobExecutionContext context) throws Exception {
		LOG.info("----------- > createCheck Copy Job working");	
		JobDataMap data = context.getJobDetail().getJobDataMap();
		String timestr = data.getString(JobDataMapConstract.CHECK_TTL);
		long time = 3600000;
		if(!BrStringUtils.isEmpty(timestr)){
			time = data.getLong(JobDataMapConstract.CHECK_TTL);
		}
		ManagerContralFactory mcf = ManagerContralFactory.getInstance();
		MetaTaskManagerInterface release = mcf.getTm();
		StorageNameManager snm = mcf.getSnm();
		ServiceManager sm = mcf.getSm();
		//判断是否有恢复任务，有恢复任务则不进行创建
		if(WatchSomeThingJob.getState(WatchSomeThingJob.RECOVERY_STATUSE)){
			LOG.warn("rebalance task is running !! skip check copy task");
			return;
		}
		String taskType = TaskType.SYSTEM_COPY_CHECK.name();
		//TODO：判断任务创建的时间若无则创建当前时间的前天的第一小时的
		long startTime = getStartTime(release,time);
		if(startTime < 0){
			LOG.warn("create inveral time less {} hour", time/1000/60/60);
			return;
		}

		TaskModel newTask = new TaskModel();
		long currentTime = System.currentTimeMillis();
		newTask.setCreateTime(currentTime);
		newTask.setEndDataTime(startTime + 60*60*1000);
		newTask.setStartDataTime(startTime);
		newTask.setTaskState(TaskState.INIT.code());
		newTask.setTaskType(TaskType.SYSTEM_COPY_CHECK.code());
		List<Service> services = sm.getServiceListByGroup(ServerConfig.DEFAULT_DISK_NODE_SERVICE_GROUP);
		String dirName = TimeUtils.timeInterval(startTime, 60*60*1000);
		Map<String,List<String>> losers = CopyCountCheck.collectLossFile(dirName);
		if(losers != null && !losers.isEmpty()){
			List<AtomTaskModel> atoms = createAtoms(losers, dirName);
			if(atoms != null && !atoms.isEmpty()){
				newTask.setAtomList(atoms);
			}
		}
		String taskName = release.updateTaskContentNode(newTask, taskType, null);
		//补充任务节点
		createServiceNodes(services, release, taskName);
		LOG.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>create {} task {} success !!",taskType, taskName);
	}
	/**
	 * 概述：创建子任务信息
	 * @param losers
	 * @param dirName
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	private List<AtomTaskModel> createAtoms(Map<String,List<String>> losers, String dirName){
		if(losers == null|| losers.isEmpty()){
			return null;
		}
		// 统计副本个数
		StorageNameNode sn = null;
		List<String> files = null;
		Map<String, Integer> snFilesCounts = null;
		Pair<List<String>, List<String>> result = null;
		int filterCount = 0;
		AtomTaskModel atom = null;
		List<AtomTaskModel> atoms = new ArrayList<AtomTaskModel>();
		for (Map.Entry<String, List<String>> entry : losers.entrySet()) {
			atom = new AtomTaskModel();
			atom.setStorageName(entry.getKey());
			atom.setDirName(dirName);
			atom.setFiles(entry.getValue());
			atoms.add(atom);
		}
		return atoms;
	}
	/**
	 * 概述：创建服务节点
	 * @param services
	 * @param release
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public void createServiceNodes(List<Service> services, MetaTaskManagerInterface release,String taskName){
		if(services == null || services.isEmpty()){
			LOG.warn("service list is null");
			return;
		}
		String serverId = null;
		boolean isSuccess = false;
		for(Service service : services){
			serverId = service.getServiceId();
			isSuccess = release.updateServerTaskContentNode(serverId, taskName, TaskType.SYSTEM_COPY_CHECK.name(), createServerNodeModel());
			if(!isSuccess){
				LOG.warn("create server node error {}", serverId);
			}
		}
	}
	/**
	 * 概述：创建任务
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public TaskServerNodeModel createServerNodeModel(){
		TaskServerNodeModel task = new TaskServerNodeModel();
		task.setTaskState(TaskState.INIT.code());
		return task;
	}
	
	/**
	 * 概述：获取任务开始时间
	 * @param release
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	private long getStartTime(MetaTaskManagerInterface release,long time){
		String taskType = TaskType.SYSTEM_COPY_CHECK.name();
		//判断任务创建的时间若无则创建当前时间的前天的第一小时的
		List<String> tasks = release.getTaskList(taskType);
		long startTime = new Date().getTime()/(1000*60*60) *(1000*60*60);
		startTime = startTime - time - 3600000;
		long currentTime = 0l;
		long createTime = 0l;
		if(tasks != null && !tasks.isEmpty()){
			String lasTask = tasks.get(tasks.size() - 1);			
			TaskModel task = release.getTaskContentNodeInfo(taskType, lasTask);
			if(task != null){
				currentTime = task.getEndDataTime();
				createTime = task.getCreateTime();
			}
		}else{
			LOG.info("{} task queue is empty !!", taskType);
		}
		//创建时间间隔小于一小时的不进行创建
		if(System.currentTimeMillis() - createTime <60*60*1000){
			return  -1;
		}
		if(currentTime == 0){
			return startTime;
		}else{
			return currentTime;
		}
	}
	

}
