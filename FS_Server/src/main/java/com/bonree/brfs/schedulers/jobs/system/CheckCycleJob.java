
package com.bonree.brfs.schedulers.jobs.system;

import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.task.TaskType;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.utils.Pair;
import com.bonree.brfs.common.utils.TimeUtils;
import com.bonree.brfs.duplication.storagename.StorageNameManager;
import com.bonree.brfs.duplication.storagename.StorageNameNode;
import com.bonree.brfs.schedulers.ManagerContralFactory;
import com.bonree.brfs.schedulers.jobs.biz.WatchSomeThingJob;
import com.bonree.brfs.schedulers.task.manager.MetaTaskManagerInterface;
import com.bonree.brfs.schedulers.task.model.TaskModel;
import com.bonree.brfs.schedulers.task.operation.impl.QuartzOperationStateTask;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.quartz.JobExecutionContext;
import org.quartz.UnableToInterruptJobException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CheckCycleJob extends QuartzOperationStateTask {
	private static final Logger LOG = LoggerFactory.getLogger("CopyCheckJob");
	@Override
	public void caughtException(JobExecutionContext context) {
		LOG.error("Create Task error !! {}", TaskType.SYSTEM_COPY_CHECK.name());
	}
	@Override
	public void interrupt() throws UnableToInterruptJobException {
	}
	@Override
	public void operation(JobExecutionContext context) throws Exception {
		LOG.info("cycle check job work !!!");
		ManagerContralFactory mcf = ManagerContralFactory.getInstance();
		MetaTaskManagerInterface release = mcf.getTm();
		StorageNameManager snm = mcf.getSnm();
		ServiceManager sm = mcf.getSm();

		if (WatchSomeThingJob.getState(WatchSomeThingJob.RECOVERY_STATUSE)) {
			LOG.warn("rebalance task is running !! skip check copy task");
			return;
		}
		List services = sm.getServiceListByGroup("disk_group");
		if ((services == null) || (services.isEmpty())) {
			LOG.info("SKIP create {} task, because service is empty", TaskType.SYSTEM_COPY_CHECK);
			return;
		}
		List snList = snm.getStorageNameNodeList();
		if ((snList == null) || (snList.isEmpty())) {
			LOG.info("SKIP storagename list is null");
			return;
		}
		long granule = 3600000L;
		long currentTime = System.currentTimeMillis();
		long lGraDay = currentTime - currentTime % 86400000L;
		long sGraDay = lGraDay - 604800000L;

		List needSns = CopyCountCheck.filterSn(snList, services.size());

		Map sourceTimes = null;
		for (long startTime = sGraDay; startTime <= lGraDay; startTime += granule) {
			sourceTimes = fixTimes(snList, startTime);
			if ((sourceTimes != null) && (!sourceTimes.isEmpty())) {
				LOG.info("collection {} data to check copy count",
					TimeUtils.formatTimeStamp(startTime, "yyyy-MM-dd HH:mm:ss.SSS"));
				createSingleTask(release, needSns, services, TaskType.SYSTEM_COPY_CHECK, sourceTimes, granule);
			}
		}
	}

	public Map<String, Long> fixTimes(List<StorageNameNode> snList, long startTime) {
		if ((snList == null) || (startTime <= 0L)) {
			return null;
		}
		Map fixMap = new HashMap();
		long createTime = 0L;
		String snName = null;
		for (StorageNameNode sn : snList) {
			createTime = sn.getCreateTime();
			snName = sn.getName();
			if (createTime <= startTime) {
				fixMap.put(snName, Long.valueOf(startTime));
			}
		}
		return fixMap;
	}

	public void createSingleTask(MetaTaskManagerInterface release, List<StorageNameNode> needSns,
			List<Service> services, TaskType taskType, Map<String, Long> sourceTimes, long granule) {
		Map losers = CopyCountCheck.collectLossFile(needSns, services, sourceTimes, granule);
		Pair pair = CreateSystemTask.creatTaskWithFiles(sourceTimes, losers, needSns, taskType, "0", granule, 0L);
		if (pair == null) {
			LOG.warn("create pair is empty !!!!");
			return;
		}
		TaskModel task = (TaskModel) pair.getKey();
		String taskName = null;
		if (task != null) {
			List servers = CreateSystemTask.getServerIds(services);
			taskName = CreateSystemTask.updateTask(release, task, servers, TaskType.SYSTEM_COPY_CHECK);
		}
		if (!BrStringUtils.isEmpty(taskName).booleanValue())
			LOG.info("create {} {} task successfull !!!", taskType, taskName);
	}
}
