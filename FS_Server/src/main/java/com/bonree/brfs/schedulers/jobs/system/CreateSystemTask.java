package com.bonree.brfs.schedulers.jobs.system;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.task.TaskType;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.utils.Pair;
import com.bonree.brfs.common.utils.TimeUtils;
import com.bonree.brfs.duplication.storageregion.StorageRegion;
import com.bonree.brfs.schedulers.task.manager.MetaTaskManagerInterface;
import com.bonree.brfs.schedulers.task.model.AtomTaskModel;
import com.bonree.brfs.schedulers.task.model.TaskModel;
import com.bonree.brfs.schedulers.task.model.TaskServerNodeModel;
import com.bonree.brfs.schedulers.task.model.TaskTypeModel;
/***
 * *****************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年6月4日 下午9:01:11
 * @Author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @Description: 
 *****************************************************************************
 */
public class CreateSystemTask {
	private static final Logger LOG = LoggerFactory.getLogger("CreateSystemTask");
	/***
	 * 概述：创建系统任务model
	 * @param taskType
	 * @param prex
	 * @param snList
	 * @param granule
	 * @param globalttl
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static Pair<TaskModel,TaskTypeModel> createSystemTask(TaskTypeModel tmodel, final TaskType taskType, final List<StorageRegion> snList,final long globalttl){
		if(snList == null || snList.isEmpty()) {
			return null;
		}
		Map<String,Long> snTimes = null;
		if(tmodel == null) {
			return null;
		}
		if(!tmodel.isSwitchFlag()) {
			return null;
		}
		snTimes = tmodel.getSnTimes();
		Pair<TaskModel, Map<String,Long>> pair =  creatSingleTask(snTimes, snList, taskType, globalttl);
		if(pair == null) {
			return null;
		}
		
		snTimes = pair.getSecond();
		if(snTimes != null && !snTimes.isEmpty()) {
			tmodel.putAllSnTimes(snTimes);
		}
		return new Pair<TaskModel,TaskTypeModel>(pair.getFirst(), tmodel);
	}
	/***
	 * 概述：获取任务
	 * @param release
	 * @param taskType
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static TaskModel getLastTask(MetaTaskManagerInterface release,TaskType taskType) {
		String prexTaskName = null;
		TaskModel prexTask = null;
		List<String> taskList = null;
		taskList = release.getTaskList(taskType.name());
		if(taskList != null && !taskList.isEmpty()){
			prexTaskName = taskList.get(taskList.size() - 1);
		}
		if(!BrStringUtils.isEmpty(prexTaskName)){
			prexTask = release.getTaskContentNodeInfo(taskType.name(), prexTaskName);
		}
		return prexTask;
	}
	/**
	 * 概述：创建单个类型任务
	 * @param snTimes
	 * @param needSn
	 * @param taskType
	 * @param granule
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static Pair<TaskModel,Map<String,Long>> creatTaskWithFiles(final Map<String,Long> snTimes, final Map<String,List<String>> snFiles,List<StorageRegion> needSn, TaskType taskType, String taskOperation, long globalTTL) {
		if(needSn == null || snTimes == null) {
			return null;
		}
		String snName = null;
		long cTime = 0;
		long startTime = 0;
		long endTime = 0;
		int copyCount = 1;
		long currentTime = System.currentTimeMillis();
		List<AtomTaskModel> sumAtoms = new ArrayList<AtomTaskModel>();
		long ttl = 0;
		Map<String,Long> lastSnTimes = new HashMap<String,Long>();
		if(snTimes != null) {
			lastSnTimes.putAll(snTimes);
		}
		List<String> files = null;
		AtomTaskModel atom = null;
		String dir = null;
		long cGraTime = 0;
		long granule = 0;
		for(StorageRegion sn : needSn) {
			granule = Duration.parse(sn.getFilePartitionDuration()).toMillis();
			cGraTime = currentTime - ( currentTime % granule );
			snName = sn.getName();
			cTime = sn.getCreateTime();
			// 获取开始时间
			if(snTimes.containsKey(snName)) {
				startTime = snTimes.get(snName);
			}else{
				continue;
			}
			// 获取有效的过期时间
			ttl = getTTL(sn, taskType, globalTTL);
			endTime = startTime + granule;
			// 当ttl小于等于0 的sn 跳过
			if(ttl < 0) {
				LOG.debug("sn {} don't to create task !!!",snName);
				continue;
			}
			// 当未达到过期的跳过
			if(cGraTime - startTime < ttl || cGraTime - endTime < ttl ) {
				LOG.debug("it's not time to check {}",snName);
				continue;
			}
			// 当前粒度不允许操作
			if(cGraTime == startTime) {
				LOG.debug("current time is forbid to check !!!");
				continue;
			}
			// 当无文件不操作
			if(snFiles != null) {
				files = snFiles.get(snName);
			}
			if(files != null && !files.isEmpty()) {
				dir = TimeUtils.timeInterval(startTime, granule);
				atom = AtomTaskModel.getInstance(files, snName, taskOperation, sn.getReplicateNum(), startTime, endTime, granule);
				sumAtoms.add(atom);
			}
			lastSnTimes.put(snName, endTime);
		}
		if(sumAtoms == null || sumAtoms.isEmpty()) {
			return new Pair<TaskModel,Map<String,Long>>(null, lastSnTimes);
		}
		TaskModel task = TaskModel.getInitInstance(taskType, "1", granule);
		task.putAtom(sumAtoms);
		return new Pair<TaskModel,Map<String,Long>>(task,lastSnTimes);
	}
	/**
	 * 概述：创建单个类型任务
	 * @param snTimes
	 * @param needSn
	 * @param taskType
	 * @param granule
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static Pair<TaskModel,Map<String,Long>> creatSingleTask(final Map<String,Long> snTimes, List<StorageRegion> needSn, TaskType taskType, long globalTTL) {
		String snName = null;
		long cTime = 0;
		long startTime = 0;
		long endTime = 0;
		int copyCount = 1;
		long currentTime = System.currentTimeMillis();
		List<AtomTaskModel> sumAtoms = new ArrayList<AtomTaskModel>();
		long ttl = 0;
		Map<String,Long> lastSnTimes = new HashMap<String,Long>(snTimes);
		long cGraTime = 0;
		long granule = 0;
		AtomTaskModel atom = null;
		for(StorageRegion sn : needSn) {
			granule =Duration.parse(sn.getFilePartitionDuration()).toMillis();
			cGraTime = currentTime - currentTime%granule;
			snName = sn.getName();
			cTime = sn.getCreateTime();
			// 获取开始时间
			if(snTimes.containsKey(snName)) {
				startTime = snTimes.get(snName);
			}else{
				startTime = cTime - cTime%granule;
			}
			// 获取有效的过期时间
			ttl = getTTL(sn, taskType, globalTTL);
			endTime = startTime + granule;
			LOG.info("sn : {} ,ttl:{}, taskType,", sn.getName(),ttl,taskType.name());
			// 当ttl小于等于0 的sn 跳过
			if(ttl <= 0) {
				LOG.info("sn {} don't to create task !!!",snName);
				continue;
			}
			// 当未达到过期的跳过
			if(cGraTime - startTime < ttl || cGraTime - endTime < ttl ) {
				continue;
			}
			// 当前粒度不允许操作
			if(cGraTime == startTime) {
				continue;
			}
			copyCount = sn.getReplicateNum();
			atom  =  AtomTaskModel.getInstance(null, snName, "", sn.getReplicateNum(), startTime, endTime, granule);
			if(atom == null) {
				continue;
			}
			sumAtoms.add(atom);
			lastSnTimes.put(snName, endTime);
		}
		if(sumAtoms == null || sumAtoms.isEmpty()) {
			return null;
		}
		TaskModel task = TaskModel.getInitInstance(taskType, "", granule);
		task.putAtom(sumAtoms);
		
		return new Pair<TaskModel,Map<String,Long>>(task,lastSnTimes);
	}
	/**
	 * 概述：首次执行指定任务sn任务创建
	 * @param sn
	 * @param taskType
	 * @param ttl
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static long getTTL(StorageRegion sn,TaskType taskType, long ttl) {
		if(sn == null) {
			return ttl;
		}
		if(taskType == null) {
			return ttl;
		}
		if(TaskType.SYSTEM_DELETE.equals(taskType)) {
			return Duration.parse(sn.getDataTtl()).toMillis();
		}
		return ttl;
	}
	/**
	 * 概述：获取上次执行时间
	 * @param prex
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static Map<String,Long> getLastTime(TaskModel prex){
		Map<String,Long> snCurrentTime = new HashMap<String,Long>();
		if(prex == null) {
			return snCurrentTime;
		}
		List<AtomTaskModel> atoms = prex.getAtomList();
		if(atoms == null || atoms.isEmpty()) {
			return snCurrentTime;
		}
		String snName = null;
		long startTime = 0;
		for(AtomTaskModel atom : atoms) {
			snName = atom.getStorageName();
			startTime = TimeUtils.getMiles(atom.getDataStopTime(),TimeUtils.TIME_MILES_FORMATE);
			if(BrStringUtils.isEmpty(snName)) {
				continue;
			}
			if(startTime < 0 ) {
				continue;
			}
			if(!snCurrentTime.containsKey(snName)) {
				snCurrentTime.put(snName, startTime);
			}
			if(startTime > snCurrentTime.get(snName)) {
				snCurrentTime.put(snName, startTime);
			}
		}
		return snCurrentTime;
	}
	/**
	 * 概述：过滤不过期的SN
	 * @param snList
	 * @param currentTime
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static List<StorageRegion> fileterNeverTTL(final List<StorageRegion> snList){
		if(snList == null || snList.isEmpty()) {
			return null;
		}
		List<StorageRegion> filters = new ArrayList<>();
		long ttl = 0;
		long cTime = 0;
		long currentTime = System.currentTimeMillis();
		for(StorageRegion sn : snList) {
			//TODO 测试阶段单位为s，正式阶段单位为d
			ttl = Duration.parse(sn.getDataTtl()).toMillis();
			cTime = sn.getCreateTime();
			// 过滤永久保存的sn
			if(ttl <0) {
				continue;
			}
			// 过滤还不到过期的sn
			if(ttl > currentTime - cTime) {
				continue;
			}
			filters.add(sn);
		}
		return filters;
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
	public static String updateTask( MetaTaskManagerInterface release, TaskModel task, List<String> serverIds, TaskType taskType) {
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
	public static List<String> getServerIds(ServiceManager sm, String groupName){
		List<Service> sList = sm.getServiceListByGroup(groupName);
		
		return getServerIds(sList);
	}
	
	public static  List<String> getServerIds(List<Service> sList){
		List<String> sids = new ArrayList<>();
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
