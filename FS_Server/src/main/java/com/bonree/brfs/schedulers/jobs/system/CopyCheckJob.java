package com.bonree.brfs.schedulers.jobs.system;

import java.util.List;
import java.util.Map;

import org.quartz.JobExecutionContext;
import org.quartz.UnableToInterruptJobException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.task.TaskType;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.utils.Pair;
import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.CommonConfigs;
import com.bonree.brfs.duplication.storageregion.StorageRegion;
import com.bonree.brfs.duplication.storageregion.StorageRegionManager;
import com.bonree.brfs.schedulers.ManagerContralFactory;
import com.bonree.brfs.schedulers.jobs.biz.WatchSomeThingJob;
import com.bonree.brfs.schedulers.task.manager.MetaTaskManagerInterface;
import com.bonree.brfs.schedulers.task.model.TaskModel;
import com.bonree.brfs.schedulers.task.model.TaskTypeModel;
import com.bonree.brfs.schedulers.task.operation.impl.QuartzOperationStateTask;
import com.bonree.brfs.schedulers.utils.TaskStateLifeContral;

public class CopyCheckJob extends QuartzOperationStateTask{
	private static final Logger LOG = LoggerFactory.getLogger("CopyCheckJob");
	public static final String RECOVERY_NUM = "1";
	public static final String RECOVERY_CRC = "0";
	@Override
	public void caughtException(JobExecutionContext context) {
		LOG.error("Create Task error !! {}",TaskType.SYSTEM_COPY_CHECK.name());
	}

	@Override
	public void interrupt() throws UnableToInterruptJobException {
		
	}

	@Override
	public void operation(JobExecutionContext context) throws Exception {
		
		long currentTime = System.currentTimeMillis();

		LOG.info("createCheck Copy Job working");
		ManagerContralFactory mcf = ManagerContralFactory.getInstance();
		MetaTaskManagerInterface release = mcf.getTm();
		StorageRegionManager snm = mcf.getSnm();
		ServiceManager sm = mcf.getSm();
		List<String> srs = TaskStateLifeContral.getSRs(snm);
		TaskStateLifeContral.watchSR(release, srs, TaskType.SYSTEM_COPY_CHECK.name());
		//判断是否有恢复任务，有恢复任务则不进行创建
		if(WatchSomeThingJob.getState(WatchSomeThingJob.RECOVERY_STATUSE)){
			LOG.warn("rebalance task is running !! skip check copy task");
			return;
		}
		String taskType = TaskType.SYSTEM_COPY_CHECK.name();
		List<Service> services = sm.getServiceListByGroup(Configs.getConfiguration().GetConfig(CommonConfigs.CONFIG_DATA_SERVICE_GROUP_NAME));
		if(services == null || services.isEmpty()) {
			LOG.info("SKIP create {} task, because service is empty",taskType);
			return ;
		}
		List<StorageRegion> snList = snm.getStorageRegionList();
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
		LOG.info("update init sn time :{}", sourceTimes);
		// 2.过滤不符合副本校验的sn信息
		List<StorageRegion> needSns = CopyCountCheck.filterSn(snList, services.size());
		// 3.针对第一次出现的sn补充时间
		sourceTimes = CopyCountCheck.repairTime(sourceTimes, needSns);
		Map<String,List<String>> losers = CopyCountCheck.collectLossFile(needSns, services, sourceTimes);
		LOG.info("update before sn time :{}", sourceTimes);
		Pair<TaskModel,Map<String,Long>> pair = CreateSystemTask.creatTaskWithFiles(sourceTimes, losers, needSns, TaskType.SYSTEM_COPY_CHECK, RECOVERY_NUM,0);
		if(pair == null) {
			LOG.warn("create pair is empty !!!!");
			return;
		}
		TaskModel task = pair.getFirst();
		String taskName = null;
		if(task != null) {
			List<String> servers = CreateSystemTask.getServerIds(services);
			taskName = CreateSystemTask.updateTask(release, task, servers, TaskType.SYSTEM_COPY_CHECK);
		}
		if(!BrStringUtils.isEmpty(taskName)) {
			LOG.info("create {} {} task successfull !!!", taskType, taskName);
		}
		sourceTimes = pair.getSecond();
		// 更新sn临界信息
		tmodel = release.getTaskTypeInfo(taskType);
		tmodel.putAllSnTimes(sourceTimes);
		release.setTaskTypeModel(taskType, tmodel);
		LOG.info("update sn time {}", sourceTimes);
		
	}
}
