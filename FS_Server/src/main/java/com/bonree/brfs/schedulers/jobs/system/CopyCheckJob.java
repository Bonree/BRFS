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
		
	}

	@Override
	public void interrupt() throws UnableToInterruptJobException {
		
	}

	@Override
	public void operation(JobExecutionContext context) throws Exception {
		LOG.info("----------- > createCheck Copy Job working");		
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
		long startTime = getStartTime(release);
		if(startTime < 0){
			LOG.warn("create inveral time less 1 hour");
			return;
		}
		
		List<Service> services = sm.getServiceListByGroup(ServerConfig.DEFAULT_DISK_NODE_SERVICE_GROUP);
		int size = services.size();
		//判断过滤sn，若sn为单副本的过滤掉，只针对多副本的
		List<StorageNameNode> snList = filterSn(snm.getStorageNameNodeList(), size);
		if(snList == null || snList.isEmpty()){
			LOG.warn("storageName is null");
			return;
		}
		
		Map<StorageNameNode, List<String>> snFiles = collectionSnFiles(services, snList, startTime);
		
		// 统计副本个数
		StorageNameNode sn = null;
		List<String> files = null;
		Map<String,Integer> snFilesCounts = null;
		Pair<List<String>,List<String>> result = null;
		int filterCount = 0;
		TaskModel newTask = new TaskModel();
		long currentTime = System.currentTimeMillis();
		newTask.setCreateTime(currentTime);
		newTask.setEndDataTime(startTime + 60*60*1000);
		newTask.setStartDataTime(startTime);
		newTask.setTaskState(TaskState.INIT.code());
		newTask.setTaskType(TaskType.SYSTEM_COPY_CHECK.code());
		AtomTaskModel atom = null;
		if(snFiles == null || snFiles.isEmpty()){
			release.updateTaskContentNode(newTask, taskType, null);
			LOG.info("{} time cluster's is no data ", startTime);
			return;
		}
		for(Map.Entry<StorageNameNode, List<String>> entry : snFiles.entrySet()){
			sn = entry.getKey();
			files = entry.getValue();
			if(files == null || files.isEmpty()){
				continue;
			}
			snFilesCounts = calcFileCount(files);
			if(snFilesCounts == null || snFilesCounts.isEmpty()){
				continue;
			}
			result = filterLoser(snFilesCounts, sn.getReplicateCount());
			atom = new AtomTaskModel();
			atom.setStorageName(sn.getName());
			atom.setFiles(result.getKey());
			newTask.addAtom(atom);
		}
		release.updateTaskContentNode(newTask, taskType, null);
		//补充任务节点
		String serverId = null;
		for(Service service : services){
			serverId = service.getServiceId();
			release.updateServerTaskContentNode(serverId, null, TaskType.SYSTEM_COPY_CHECK.name(), createServerNodeModel());
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
	 * 概述：获取集群对应目录的文件
	 * @param services
	 * @param snList
	 * @param dataPath
	 * @param startTime
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	private Map<StorageNameNode, List<String>> collectionSnFiles(List<Service> services, List<StorageNameNode> snList, long startTime){
		Map<StorageNameNode,List<String>> snMap = new HashMap<>();
		String dirName = TimeUtils.timeInterval(startTime, 60*60*1000);
		DiskNodeClient client = null;
		int reCount = 0;
		String snName = null;
		String path = null;
		List<FileInfo> files = null;
		List<String> strs = null;
		for(Service service : services){
			try {
				client = new HttpDiskNodeClient(service.getHost(), service.getPort());
				for(StorageNameNode sn : snList){
					reCount = sn.getReplicateCount();
					snName = sn.getName();
					for(int i = 0; i <reCount; i++){
						path = snName+File.separator+i+File.separator+dirName;
						files =client.listFiles(path, 1);
						if(files == null){
							LOG.info("the list file of {} is null ", service.getServiceId());
							continue;
						}
						if(!snMap.containsKey(sn)){
							snMap.put(sn, new ArrayList<String>());
						}
						strs = converToStringList(files);
						snMap.get(snName).addAll(strs);
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}finally{
				if(client != null){
					try {
						client.close();
					}
					catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
			
		}
		return snMap;
	}
	/**
	 * 概述：转换集合为str集合
	 * @param files
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	private List<String> converToStringList(List<FileInfo> files){
		List<String> strs = new ArrayList<>();
		String path = null;
		String fileName = null;
		int lastIndex = 0;
		for(FileInfo file : files){
			path = file.getPath();
			lastIndex = path.lastIndexOf("/");
			if(lastIndex <0){
				lastIndex = path.lastIndexOf("\\");
				continue;
			}
			if(lastIndex <0){
				continue;
			}
			fileName = path.substring(lastIndex+1);
			strs.add(fileName);
		}
		return strs;
	}
	private List<StorageNameNode> filterSn(List<StorageNameNode> sns, int size){
		List<StorageNameNode> filters = new ArrayList<StorageNameNode>();
		if(sns == null || sns.isEmpty()){
			return filters;
		}
		int count = 0;
		for(StorageNameNode sn : sns){
			count = sn.getReplicateCount();
			if(count == 1){
				continue;
			}
			if(count >size){
				continue;
			}
			filters.add(sn);
		}
		return sns;
		
	}
	/**
	 * 概述：获取任务开始时间
	 * @param release
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	private long getStartTime(MetaTaskManagerInterface release){
		String taskType = TaskType.SYSTEM_COPY_CHECK.name();
		//判断任务创建的时间若无则创建当前时间的前天的第一小时的
		List<String> tasks = release.getTaskList(taskType);
		long startTime = (new Date().getTime() - 24*60*60*1000)/(1000*60*60) *(1000*60*60);
		long currentTime = 0l;
		long createTime = 0l;
		if(tasks != null && !tasks.isEmpty()){
			String lasTask = tasks.get(tasks.size() - 1);			
			TaskModel task = release.getTaskContentNodeInfo(taskType, lasTask);
			LOG.info("TEST task {} {} ",taskType, lasTask);
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
	/**
	 * 概述：统计副本的个数
	 * @param files
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public Map<String, Integer> calcFileCount(final Collection<String> files){
		Map<String, Integer> filesMap = new HashMap<>();
		for(String file : files){
			if(filesMap.containsKey(file)){
				filesMap.put(file, filesMap.get(file) + 1);
			}else{
				filesMap.put(file,1);
			}
		}
		return filesMap;
	}
	/**
	 * 概述：收集副本数异常的副本
	 * @param resultMap
	 * @param filterValue
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public Pair<List<String>,List<String>>filterLoser(Map<String,Integer> resultMap, int filterValue){
		List<String> filterBiggestResult = new ArrayList<String>();
		List<String> filterLitterResult = new ArrayList<String>();
		String key = null;
		int count = 0;
		for(Map.Entry<String, Integer> entry : resultMap.entrySet()){
			count = entry.getValue();
			key = entry.getKey();
			if(filterValue == count){
				continue;
			} else	if(filterValue > count){
				filterLitterResult.add(key);
			}else if(filterValue < count){
				filterBiggestResult.add(key);
			}
		}
		Pair<List<String>,List<String>> result = new Pair<List<String>,List<String>>();
		result.setKey(filterLitterResult);
		result.setValue(filterBiggestResult);
		return result;
	}

}
