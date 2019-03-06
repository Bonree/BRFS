
package com.bonree.brfs.schedulers.jobs.system;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.task.TaskType;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.utils.Pair;
import com.bonree.brfs.common.utils.TimeUtils;
import com.bonree.brfs.duplication.storageregion.StorageRegion;
import com.bonree.brfs.duplication.storageregion.StorageRegionManager;
import com.bonree.brfs.schedulers.ManagerContralFactory;
import com.bonree.brfs.schedulers.jobs.biz.WatchSomeThingJob;
import com.bonree.brfs.schedulers.task.manager.MetaTaskManagerInterface;
import com.bonree.brfs.schedulers.task.model.TaskModel;
import com.bonree.brfs.schedulers.task.operation.impl.QuartzOperationStateTask;
import com.bonree.brfs.schedulers.utils.CopyCountCheck;
import com.bonree.brfs.schedulers.utils.CreateSystemTask;
import com.bonree.brfs.schedulers.utils.JobDataMapConstract;

public class CheckCycleJob extends QuartzOperationStateTask {
	private static final Logger LOG = LoggerFactory.getLogger(CheckCycleJob.class);
	@Override
	public void caughtException(JobExecutionContext context) {
		LOG.error("Create Task error !! {}", TaskType.SYSTEM_COPY_CHECK.name());
	}
	@Override
	public void interrupt(){
	}
	@Override
	public void operation(JobExecutionContext context) throws Exception {
		LOG.info("cycle check job work !!!");
		JobDataMap data = context.getJobDetail().getJobDataMap();
		int day = data.getInt(JobDataMapConstract.CHECK_TIME_RANGE);
		if(day <=0) {
			LOG.warn("skip cycle job!! because check time range is 0");
			return;
		}
		ManagerContralFactory mcf = ManagerContralFactory.getInstance();
		MetaTaskManagerInterface release = mcf.getTm();
		StorageRegionManager snm = mcf.getSnm();
		ServiceManager sm = mcf.getSm();

		if (WatchSomeThingJob.getState(WatchSomeThingJob.RECOVERY_STATUSE)) {
			LOG.warn("rebalance task is running !! skip check copy task ,wait next time to check");
			return;
		}
		List services = sm.getServiceListByGroup(mcf.getGroupName());
		if ((services == null) || (services.isEmpty())) {
			LOG.warn("SKIP create {} task, because service is empty", TaskType.SYSTEM_COPY_CHECK);
			return;
		}
		List<StorageRegion> snList = snm.getStorageRegionList();
		if ((snList == null) || (snList.isEmpty())) {
			LOG.warn("SKIP storagename list is null");
			return;
		}
		long currentTime = System.currentTimeMillis();
		long lGraDay = currentTime - currentTime % 86400000L;
		long sGraDay = lGraDay - day * 86400000L;
		List<StorageRegion> needSns = CopyCountCheck.filterSn(snList, services.size());
		if(needSns == null|| needSns.isEmpty()) {
			LOG.warn("no storagename need check copy count ! ");
			return ;
		}
		Map<String,List<Long>> snTimes = collectionTimes(needSns, sGraDay, lGraDay);
		if(snTimes == null || snTimes.isEmpty()) {
			LOG.warn("{} - {} time, no data to check copy count", TimeUtils.formatTimeStamp(sGraDay), TimeUtils.formatTimeStamp(lGraDay));
			return;
		}
		List<Map<String,Long>> tTimes = converTimes(snTimes);
		for(Map<String,Long> sourceTimes : tTimes) {
			if(sourceTimes == null|| sourceTimes.isEmpty()) {
				continue;
			}
			createSingleTask(release, needSns, services, TaskType.SYSTEM_COPY_CHECK, sourceTimes);
		}
	}
	/**
	 * 概述：生成sn时间
	 * @param needSns
	 * @param startTime
	 * @param endTime
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public Map<String,List<Long>> collectionTimes(List<StorageRegion> needSns, final long startTime, final long endTime){
		if(needSns == null || needSns.isEmpty()) {
			return null;
		}
		Map<String,List<Long>> snTimes = new HashMap<>();
		List<Long> times;
		long cTime;
		long granule;
		String snName;
		for(StorageRegion sn : needSns) {
			snName = sn.getName();
			cTime = sn.getCreateTime();
			granule = Duration.parse(sn.getFilePartitionDuration()).toMillis();
			times = new ArrayList<>();
			for(long start =startTime; start <endTime; start += granule) {
				if(start < cTime) {
					continue;
				}
				times.add(start);
			}
			if(times == null || times.isEmpty()) {
				continue;
			}
			if(!snTimes.containsKey(snName)) {
				snTimes.put(snName, times);
			}
			
		}
		return snTimes;
	}
	public List<Map<String,Long>> converTimes(Map<String,List<Long>> snTime){
		int maxSize = 0;
		List<Map<String,Long>> snTimes = new ArrayList<Map<String,Long>>();
		String snName;
		List<Long> times;
		Map<String,Long> atoms;
		for(Map.Entry<String, List<Long>> entry : snTime.entrySet()) {
			snName = entry.getKey();
			times = entry.getValue();
			if(maxSize < times.size()) {
				maxSize = times.size();
			}
			for(int i = 0; i< times.size(); i++) {
				if(i >= snTimes.size()) {
					atoms = new HashMap<>();
					snTimes.add(atoms);
				}
				atoms = snTimes.get(i);
				atoms.put(snName,times.get(i));
			}
		}
		return snTimes;
	}
//	/**
//	 * 概述：填补时间
//	 * @param snList
//	 * @param startTime
//	 * @return
//	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
//	 */
//	public Map<String, Long> fixTimes(List<StorageRegion> snList, long startTime) {
//		if ((snList == null) || (startTime <= 0L)) {
//			return null;
//		}
//		Map<String,Long> fixMap = new HashMap<String,Long>();
//		long crGra;
//		String snName;
//		long granule;
//		for (StorageRegion sn : snList) {
//			granule = Duration.parse(sn.getFilePartitionDuration()).toMillis();
//			crGra = sn.getCreateTime() - sn.getCreateTime()%granule;
//			snName = sn.getName();
//			LOG.info("<fixTimes> sn {}, cTime:{}, time:{}", snName,crGra,startTime);
//			if (crGra <= startTime) {
//				fixMap.put(snName, Long.valueOf(startTime));
//			}
//		}
//		return fixMap;
//	}
	/**
	 * 概述：创建单个任务
	 * @param release
	 * @param needSns
	 * @param services
	 * @param taskType
	 * @param sourceTimes
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public void createSingleTask(MetaTaskManagerInterface release, List<StorageRegion> needSns, List<Service> services, TaskType taskType, Map<String, Long> sourceTimes) throws Exception {
		Map losers = CopyCountCheck.collectLossFile(needSns, services, sourceTimes);
		Pair pair = CreateSystemTask.creatTaskWithFiles(sourceTimes, losers, needSns, taskType, CopyCheckJob.RECOVERY_NUM, 0L);
		if (pair == null) {
			LOG.warn("create pair is empty !!!!");
			return;
		}
		TaskModel task = (TaskModel) pair.getFirst();
		String taskName = null;
		if (task != null) {
			List<String> servers = CreateSystemTask.getServerIds(services);
			taskName = CreateSystemTask.updateTask(release, task, servers, TaskType.SYSTEM_COPY_CHECK);
		}
		if (!BrStringUtils.isEmpty(taskName)) {
			LOG.info("create {} {} task successfull !!!", taskType, taskName);
		}
	}
}
