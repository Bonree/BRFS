package com.bonree.brfs.schedulers.jobs.system;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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
import com.bonree.brfs.schedulers.task.model.TaskTypeModel;
import com.bonree.brfs.schedulers.task.operation.impl.QuartzOperationStateTask;

import ch.qos.logback.classic.net.SyslogAppender;

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
		JobDataMap data = context.getJobDetail().getJobDataMap();
		String timestr = data.getString(JobDataMapConstract.CHECK_TTL);
		long time = 0;
		long currentTime = System.currentTimeMillis();
		long min = (currentTime%3600000)/60000;
		if(min < 10) {
			return;
		}
		LOG.info("createCheck Copy Job working");	
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
		List<Service> services = sm.getServiceListByGroup(ServerConfig.DEFAULT_DISK_NODE_SERVICE_GROUP);
		if(services == null || services.isEmpty()) {
			LOG.info("SKIP create {} task, because service is empty",taskType);
			return ;
		}
		List<StorageNameNode> snList = snm.getStorageNameNodeList();
		if( snList== null || snList.isEmpty()) {
			LOG.info("SKIP storagename list is null");
			return;
		}
		// 1.获取sn创建任务的实际那
		TaskTypeModel tmodel = release.getTaskTypeInfo(taskType);
		if(tmodel == null) {
			tmodel = new TaskTypeModel();
			tmodel.setSwitchFlag(true);
			release.setTaskTypeModel(taskType, tmodel);
		}
		Map<String,Long> sourceTimes = tmodel.getSnTimes();
		// 2.过滤不符合副本校验的sn信息
		List<StorageNameNode> needSns = CopyCountCheck.filterSn(snList, services.size());
		// 3.针对第一次出现的sn补充时间
		sourceTimes = CopyCountCheck.repairTime(sourceTimes, needSns, 3600000,time);
		Map<String,List<String>> losers = CopyCountCheck.collectLossFile(needSns, services, sourceTimes, 3600000);
		
		Pair<TaskModel,Map<String,Long>> pair = CreateSystemTask.creatTaskWithFiles(sourceTimes, losers, needSns, TaskType.SYSTEM_COPY_CHECK, "RECOVERY", 3600000, time);
		if(pair == null) {
			LOG.warn("create pair is empty !!!!");
			return;
		}
		TaskModel task = pair.getKey();
		String taskName = null;
		if(task != null) {
			List<String> servers = CreateSystemTask.getServerIds(services);
			taskName = CreateSystemTask.updateTask(release, task, servers, TaskType.SYSTEM_COPY_CHECK);
		}
		if(!BrStringUtils.isEmpty(taskName)) {
			LOG.info("create {} {} task successfull !!!", taskType, taskName);
		}
		sourceTimes = pair.getValue();
		// 更新sn临界信息
		tmodel = release.getTaskTypeInfo(taskType);
		tmodel.putAllSnTimes(sourceTimes);
		release.setTaskTypeModel(taskType, tmodel);
		LOG.info("update sn time");
		
	}
	/**
	 * 概述：创建副本任务
	 * @param release
	 * @param snm
	 * @param sm
	 * @param initMap
	 * @param taskType
	 * @param time
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static Map<String,Long> createCopyTask(MetaTaskManagerInterface release,	StorageNameManager snm,	ServiceManager sm ,final Map<String,Long> initMap,String taskType, long time) {
		List<Service> services = sm.getServiceListByGroup(ServerConfig.DEFAULT_DISK_NODE_SERVICE_GROUP);
		if(services == null || services.isEmpty()) {
			LOG.info("SKIP create {} task, because service is empty",taskType);
			return null;
		}
		// 1.过滤不符合副本校验的sn信息
		List<StorageNameNode> needSns = getNeedSns(snm, services);
		if(needSns == null|| needSns.isEmpty()) {
			LOG.info("SKIP storagename list is null");
			return null;
		}
		
		// 2.获取sn创建任务的实际那
		Map<String,Long> sourceTimes = new HashMap<String,Long>();
		if(initMap != null) {
			sourceTimes.putAll(initMap);
		}
		// 3.针对第一次出现的sn补充时间
		sourceTimes = CopyCountCheck.repairTime(sourceTimes, needSns, 3600000,time);
		Map<String,List<String>> losers = CopyCountCheck.collectLossFile(needSns, services, sourceTimes, 3600000);
		
		Pair<TaskModel,Map<String,Long>> pair = CreateSystemTask.creatTaskWithFiles(sourceTimes, losers, needSns, TaskType.SYSTEM_COPY_CHECK, "RECOVERY", 3600000, time);
		if(pair == null) {
			LOG.warn("create pair is empty !!!!");
			return null;
		}
		TaskModel task = pair.getKey();
		String taskName = null;
		if(task != null) {
			List<String> servers = CreateSystemTask.getServerIds(services);
			taskName = CreateSystemTask.updateTask(release, task, servers, TaskType.SYSTEM_COPY_CHECK);
		}
		if(!BrStringUtils.isEmpty(taskName)) {
			LOG.info("create {} {} task successfull !!!", taskType, taskName);
		}
		return pair.getValue();
	}
	/***
	 * 概述：获取需要副本恢复的任务
	 * @param snm
	 * @param services
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static List<StorageNameNode> getNeedSns(StorageNameManager snm, List<Service> services){
		if(services == null || services.isEmpty()) {
			return null;
		}
		List<StorageNameNode> snList = snm.getStorageNameNodeList();
		if( snList== null || snList.isEmpty()) {
			LOG.info("SKIP storagename list is null");
			return null;
		}
		return CopyCountCheck.filterSn(snList, services.size());
	}
	
}
