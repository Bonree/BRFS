
package com.bonree.brfs.schedulers.task.manager.impl;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.task.TaskState;
import com.bonree.brfs.common.task.TaskType;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.common.utils.Pair;
import com.bonree.brfs.common.utils.TimeUtils;
import com.bonree.brfs.common.zookeeper.ZookeeperClient;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.schedulers.task.manager.MetaTaskManagerInterface;
import com.bonree.brfs.schedulers.task.model.TaskModel;
import com.bonree.brfs.schedulers.task.model.TaskServerNodeModel;
import com.bonree.brfs.schedulers.task.model.TaskTypeModel;

public class DefaultReleaseTask implements MetaTaskManagerInterface {
	private static final Logger LOG = LoggerFactory.getLogger(DefaultReleaseTask.class);
	private static final String QUEUE = "queue";
	private static final String TRANSFER = "transfer";
	private String zkUrl = null;
	private String taskRootPath = null;
	private String taskLockPath = null;
	private ZookeeperClient client = null;
	private String taskQueue = null;
	private String taskTransfer = null;
	private static class releaseInstance {
		public static DefaultReleaseTask instance = new DefaultReleaseTask();
	}

	private DefaultReleaseTask() {

	}

	public static DefaultReleaseTask getInstance() {
		return releaseInstance.instance;
	}

	@Override
	public String updateTaskContentNode(TaskModel data, String taskType, String taskName){
		String pathNode = null;
		try {
			if (data == null) {
				LOG.warn("task content is empty");
				return null;
			}
			byte[] datas = JsonUtils.toJsonBytes(data);
			if(datas == null || datas.length == 0){
				LOG.warn("task content convert is empty");
				return null;
			}
			if (BrStringUtils.isEmpty(taskType)) {
				LOG.warn("task type is empty");
				return null;
			}
			TaskType current = TaskType.valueOf(taskType);
			int taskTypeIndex = current == null ? 0 : current.code();

			StringBuilder pathBuilder = new StringBuilder();
			pathBuilder.append(this.taskQueue).append("/").append(taskType).append("/");
			if(BrStringUtils.isEmpty(taskName)){
				pathBuilder.append(taskTypeIndex);
			}else{
				pathBuilder.append(taskName);
			}
			String taskPath = pathBuilder.toString();
			if (!BrStringUtils.isEmpty(taskName) && client.checkExists(taskPath)) {
				client.setData(taskPath, datas);
				return taskName;
			}
			pathNode = client.createPersistentSequential(taskPath, true, datas);
			String[] nodes = BrStringUtils.getSplit(pathNode, "/");
			if (nodes != null && nodes.length != 0) {
				return nodes[nodes.length - 1];
			}
		}catch (Exception e) {
			LOG.error("{}",e);
		}
		return pathNode;
	}

	@Override
	public String getCurrentTaskIndex(String taskType){
		try {
			List<String> taskInfos = getTaskList(taskType);
			if(taskInfos == null || taskInfos.isEmpty()){
				return null;
			}
			return taskInfos.get(taskInfos.size() - 1);
		}
		catch (Exception e) {
			LOG.error("{}",e);
		}
		return null;
	}

	@Override
	public int queryTaskState(String taskName, String taskType){
		// TODO Auto-generated method stub
		try {
			if (BrStringUtils.isEmpty(taskName)) {
				return -1;
			}
			if (BrStringUtils.isEmpty(taskType)) {
				return -2;
			}
			StringBuilder taskPath = new StringBuilder();
			taskPath.append(this.taskQueue).append("/").append(taskType).append("/").append(taskName);
			String path = taskPath.toString();
			if (!client.checkExists(path)) {
				return -3;
			}
			byte[] data = client.getData(path);
			if (data == null || data.length == 0) {
				return -4;
			}
			TaskModel tmp = JsonUtils.toObject(data, TaskModel.class);
			return tmp.getTaskState();
		}catch (Exception e) {
			LOG.error("{}",e);
		}
		return -5;
	}
	@Override
	public boolean updateServerTaskContentNode(String serverId, String taskName, String taskType, TaskServerNodeModel data){
		
		try {
			if (BrStringUtils.isEmpty(serverId)) {
				return false;
			}
			if (BrStringUtils.isEmpty(taskName)) {
				return false;
			}
			if (BrStringUtils.isEmpty(taskType)) {
				return false;
			}
			if (data == null) {
				LOG.warn("task content is empty");
				return false;
			}
			byte[] datas = JsonUtils.toJsonBytes(data);
			if(datas == null || datas.length == 0){
				LOG.warn("task content convert is empty");
				return false;
			}
			StringBuilder taskPath = new StringBuilder();
			taskPath.append(this.taskQueue).append("/").append(taskType).append("/").append(taskName).append("/").append(serverId);
			String path = taskPath.toString();
			if (client.checkExists(path)) {
				client.setData(path, datas);
			} else {
				client.createPersistent(path, true, datas);
			}
			return true;
		}
		catch (Exception e) {
			LOG.error("{}",e);
		}
		return false;
	}

	@Override
	public boolean changeTaskContentNodeState(String taskName, String taskType, int taskState){
		try {
			if (BrStringUtils.isEmpty(taskName)) {
				return false;
			}
			if (BrStringUtils.isEmpty(taskType)) {
				return false;
			}			
			TaskModel tmp = getTaskContentNodeInfo(taskType, taskName);
			if(tmp == null){
				return false;
			}
			tmp.setTaskState(taskState);
			updateTaskContentNode(tmp, taskType, taskName);
			return true;
		}
		catch (Exception e) {
			LOG.error("{}",e);
		}
		return false;
	}
	@Override
	public boolean changeTaskServerNodeContentState(String taskName, String taskType, String serverId, int taskState){
		try {
			if (BrStringUtils.isEmpty(taskName)) {
				return false;
			}
			if (BrStringUtils.isEmpty(taskType)) {
				return false;
			}
			if (BrStringUtils.isEmpty(serverId)){
				return false;
			}
			TaskServerNodeModel tmp = getTaskServerContentNodeInfo(taskType, taskName, serverId);
			if(tmp == null){
				return false;
			}
			tmp.setTaskState(taskState);
			updateServerTaskContentNode(serverId, taskName, taskType, tmp);
			return true;
		}
		catch (Exception e) {
			LOG.error("{}",e);
		}
		return false;
	}

	@Override
	public String getLastSuccessTaskIndex(String taskType, String serverId){
		try {
			List<String> taskInfos = getTaskList(taskType);
			if(taskInfos == null || taskInfos.isEmpty()){
				return null;
			}
			int maxIndex = taskInfos.size() - 1;
			StringBuilder path;
			String taskName;
			String taskPath;
			TaskServerNodeModel tmpR;
			for (int i = maxIndex; i >= 0; i--) {
				taskName = taskInfos.get(i);
				path = new StringBuilder();
				path.append(this.taskQueue).append("/").append(taskType).append("/").append(taskName).append("/").append(
					serverId);
				taskPath = path.toString();
				if (!client.checkExists(taskPath)) {
					continue;
				}
				tmpR = getTaskServerContentNodeInfo(taskType, taskName, serverId);
				if(tmpR == null){
					continue;
				}
				if(tmpR.getTaskState() != TaskState.FINISH.code()){
					continue;
				}
				return taskInfos.get(i);
			}
		}
		catch (Exception e) {
			LOG.error("{}",e);
		}
		return null;
	}
	@Override
	public String getFirstServerTask(String taskType,String serverId){
		List<String> taskInfos = getTaskList(taskType);
		if(taskInfos == null || taskInfos.isEmpty()){
			return null;
		}
		StringBuilder path;
		String taskPath;
		for(String taskInfo : taskInfos) {
			path = new StringBuilder();
			path.append(this.taskQueue).append("/").append(taskType).append("/").append(taskInfo).append("/").append(serverId);
			taskPath = path.toString();
			if(!client.checkExists(taskPath)) {
				continue;
			}
			return taskInfo;
		}
		return null;
	}
	@Override
	public boolean deleteTask(String taskName, String taskType) {
		try {
			if (BrStringUtils.isEmpty(taskName)) {
				return false;
			}
			if (BrStringUtils.isEmpty(taskType)) {
				return false;
			}
			String path = this.taskQueue + "/" + taskType + "/" + taskName;
			if (!client.checkExists(path)) {
				return false;
			}
			client.delete(path, true);
			return true;
		}
		catch (Exception e) {
			LOG.error("{}",e);
		}
		return false;
	}

	@Override
	public int deleteTasks(long deleteTime, String taskType) {
		try {
			if (BrStringUtils.isEmpty(taskType)) {
				return -1;
			}
			// TODO Auto-generated method stub
			List<String> nodes = getTaskList(taskType);
			if(nodes == null || nodes.isEmpty()){
				return 0;
			}
			int size = nodes.size();
			if (size == 0) {
				return 0;
			}
			long firstTime = getTaskCreateTime(nodes.get(0), taskType);
			if (deleteTime < firstTime) {
				return 0;
			}
			//循环删除数据
			int count = 0;
			long cTime;
			for (String taskName : nodes) {
				if (BrStringUtils.isEmpty(taskName)) {
					continue;
				}
				cTime = getTaskCreateTime(taskName, taskType);
				if (cTime > deleteTime) {
					continue;
				}
				if (deleteTask(taskName, taskType)) {
					count++;
				}
			}
			return count;
		}
		catch (Exception e) {
			LOG.error("{}",e);
		}
		return -1;
	}

	private long getTaskCreateTime(String taskName, String taskType) {
		try {
			if (BrStringUtils.isEmpty(taskName)) {
				return -1;
			}
			if (BrStringUtils.isEmpty(taskType)) {
				return -2;
			}
			String path = this.taskQueue + "/" + taskType + "/" + taskName;
			if (!client.checkExists(path)) {
				return -3;
			}
			byte[] data;
			data = client.getData(path);
			if (data == null || data.length == 0) {
				return -4;
			}
			TaskModel taskInfo = JsonUtils.toObject(data, TaskModel.class);
			String createTime = taskInfo.getCreateTime();
			if(BrStringUtils.isEmpty(createTime)) {
				return 0;
			}else {
				return TimeUtils.getMiles(createTime, TimeUtils.TIME_MILES_FORMATE);
			}
		}
		catch (Exception e) {
			LOG.error("{}",e);
		}
		return -5;
	}

	@Override
	public boolean isInit() {
		// TODO Auto-generated method stub
		if (this.client == null) {
			return false;
		}
		if (BrStringUtils.isEmpty(this.zkUrl)) {
			return false;
		}
		return true;
	}

	@Override
	public void setPropreties(String zkUrl, String taskPath,String lockPath, String... args) {
		try {
			this.zkUrl = zkUrl;
			this.taskRootPath = taskPath;
			if (BrStringUtils.isEmpty(this.zkUrl)) {
				throw new NullPointerException("zookeeper address is empty");
			}
			if (BrStringUtils.isEmpty(this.taskRootPath)) {
				throw new NullPointerException("task root path is empty");
			}
			if(BrStringUtils.isEmpty(lockPath)){
				throw new NullPointerException("task lock path is empty");
			}
			this.taskLockPath = lockPath;
			this.taskQueue = this.taskRootPath + "/" + QUEUE;
			this.taskTransfer = this.taskRootPath + "/" + TRANSFER;
			client = CuratorClient.getClientInstance(this.zkUrl);
		}
		catch (Exception e) {
			LOG.error("{}",e);
		}
	}

	@Override
	public Pair<Integer,Integer> reviseTaskStat(String taskType, long ttl, Collection<String> aliveServers){
		Pair<Integer, Integer> counts = new Pair<>(0, 0);
		try {
			if(BrStringUtils.isEmpty(taskType)){
				throw new NullPointerException("taskType is empty");
			}
			if(aliveServers == null || aliveServers.isEmpty()){
				throw new NullPointerException("alive servers is empty");
			}
			// 获取子任务名称队列
			List<String> taskQueues = getTaskList(taskType);
			if(taskQueues == null || taskQueues.isEmpty()){
				return counts;
			}
			// 删除任务
			int deleteCount = deleteTasks(taskQueues, taskType, ttl);
			// 维护任务状态
			int reviseCount = reviseTaskState(taskQueues, aliveServers, taskType, deleteCount);
			counts.setFirst(deleteCount);
			counts.setSecond(reviseCount);
		}
		catch (Exception e) {
			LOG.error("{}",e);
		}
		return counts;
	}
	/**
	 * 概述：维护任务的状态
	 * @param taskQueue
	 * @param aliveServers
	 * @param taskType
	 * @param deleteIndex
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	private int reviseTaskState(List<String> taskQueue, Collection<String> aliveServers, String taskType, int deleteIndex){
		int count = 0;
		try {
			int size = taskQueue.size();
			StringBuilder taskPath;
			String taskName;
			String tmpPath;
			TaskModel taskContent;
			TaskServerNodeModel taskServer;
			List<String> cServers;
			for(int i = (size - 1); i >= deleteIndex; i--){
				taskName = taskQueue.get(i);
				taskContent = getTaskContentNodeInfo(taskType, taskName);
				if(taskContent == null){
					LOG.warn("{} {} is null", taskType, taskName);
					continue;
				}
				// 不为RUN与Exception的任务不进行检查
				int stat = taskContent.getTaskState();
				boolean exceptionFlag = TaskState.EXCEPTION.code() == stat;
				if(!(TaskState.RUN.code() == stat || exceptionFlag)){
					continue;
				}
				// 获取任务下的子节点
				taskPath = new StringBuilder();
				taskPath.append(this.taskQueue).append("/").append(taskType).append("/").append(taskName);
				tmpPath = taskPath.toString();
				cServers = client.getChildren(tmpPath);
				// 服务为空，任务标记为异常
				if(cServers == null || cServers.isEmpty()){
					count ++;
					taskContent.setTaskState(TaskState.EXCEPTION.code());
					updateTaskContentNode(taskContent, taskType, taskName);
					continue;
				}
				boolean isException = false;
				for(String cServer : cServers){
					// 存活的server不进行操作
					if(aliveServers.contains(cServer)){
						continue;
					}
					//不存活的server，节点标记为Exception
					taskServer = getTaskServerContentNodeInfo(taskType, taskName, cServer);
					if(taskServer == null){
						LOG.warn("taskType :{}, taskName :{}, serverId :{} is not exists", taskType, taskName, cServer);
						taskServer = new TaskServerNodeModel();
						taskServer.setTaskState(TaskState.UNKNOW.code());
					}

					if(TaskState.FINISH.code() == taskServer.getTaskState()){
						continue;
					}
					isException =true;
					taskServer.setTaskState(TaskState.EXCEPTION.code());
					updateServerTaskContentNode(cServer, taskName, taskType, taskServer);
				}
				if(isException && !exceptionFlag){
					count ++;
					taskContent.setTaskState(TaskState.EXCEPTION.code());
					updateTaskContentNode(taskContent, taskType, taskName);
				}
				
			}
		}catch (Exception e) {
			LOG.error("{}",e);
		}
		return count;
	}
	/**
	 * 概述：删除过期的任务
	 * @param taskQueue
	 * @param taskType
	 * @param ttl
	 * @return 删除任务的个数
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	private int deleteTasks(List<String> taskQueue, String taskType, long ttl) {
		try {
			long currentTime = System.currentTimeMillis();
			// 1.判断任务是否永久保存，永久保存，则不进行删除
			if (ttl <= 0) {
				return 0;
			}
			long deleteTime = currentTime - ttl;
			// 2.当ttl配置不合理时，不进行删除操作
			if (deleteTime <= 0) {
				throw new NullPointerException("ttl is too large");
			}
			//循环删除数据
			int count = 0;
			long cTime;
			for(String aTaskQueue : taskQueue) {
				if(BrStringUtils.isEmpty(aTaskQueue)) {
					continue;
				}
				cTime = getTaskCreateTime(aTaskQueue, taskType);
				if(cTime > deleteTime) {
					break;
				}
				if(cTime == -3) {
					LOG.warn("taskType:{}, taskName :{} is not exists ! skip it", taskType, aTaskQueue);
					continue;
				}
				if(cTime == 0) {
					LOG.warn("taskType:{}, taskName :{} create time is 0 ! will delete", taskType, aTaskQueue);
				}
				if(cTime == -4) {
					LOG.warn("delete taskType: {}, taskName: {}, content: taskcontent is null", taskType, aTaskQueue);
				}
				if(deleteTask(aTaskQueue, taskType)) {
					count++;
				}
			}
			LOG.info("delete time out task complete from");
			return count;
		}
		catch (Exception e) {
			LOG.error("{}",e);
		}
		return 0;
	}
//	/**
//	 * 概述：获取队列有效的创建时间，若存在无效的任务节点将被删除
//	 * @param taskQueue
//	 * @param taskType
//	 * @return
//	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
//	 */
//	private Pair<Integer, Long> getAvailableFirstTime(List<String> taskQueue, String taskType){
//		int index = 0;
//		long createTime = 0l;
//		long tmp;
//		for(String taskName : taskQueue){
//			tmp = getTaskCreateTime(taskName, taskType);
//			LOG.info("select taskName :{} createTime :{}", taskName, tmp);
//			if(tmp > 0){
//				createTime = tmp;
//				break;
//			}
//			if(createTime == -4){
//				LOG.warn("Delete taskType : {}, taskName : {}, content : taskcontent is null", taskType, taskName);
//				deleteTask(taskName, taskType);
//			}
//			index ++;
//		}
//		return new Pair<Integer,Long>(index,createTime);
//	}

	@Override
	public TaskModel getTaskContentNodeInfo(String taskType, String taskName) {
		try {
			if (BrStringUtils.isEmpty(taskName)) {
				return null;
			}
			if (BrStringUtils.isEmpty(taskType)) {
				return null;
			}
			String path = this.taskQueue + "/" + taskType + "/" + taskName;
			if(!client.checkExists(path)){
				return null;
			}
			byte[] data = client.getData(path);
			if (data == null || data.length == 0) {
				return null;
			}
			return JsonUtils.toObject(data, TaskModel.class);
		}
		catch (Exception e) {
			LOG.error("{}",e);
		}
		return null;
	}

	@Override
	public TaskServerNodeModel getTaskServerContentNodeInfo(String taskType, String taskName, String serverId) {
		try {
			if (BrStringUtils.isEmpty(taskName)) {
				return null;
			}
			if (BrStringUtils.isEmpty(taskType)) {
				return null;
			}
			String path = this.taskQueue + "/" + taskType + "/" + taskName + "/" + serverId;
			if(!client.checkExists(path)){
				return null;
			}
			byte[] data = client.getData(path);
			if (data == null || data.length == 0) {
				return null;
			}
			return JsonUtils.toObject(data, TaskServerNodeModel.class);
		}
		catch (Exception e) {
			LOG.error("{}",e);
		}
		return null;
	}

	@Override
	public String getNextTaskName(String taskType, String taskName) {
		if(BrStringUtils.isEmpty(taskType) || BrStringUtils.isEmpty(taskName)){
			return null;
		}
		List<String> orderTaskName = getTaskList(taskType);
		if(orderTaskName == null || orderTaskName.isEmpty()){
			return null;
		}
		int index = orderTaskName.indexOf(taskName);
		if(index <0){
			return null;
		}
		if(index == orderTaskName.size()-1){
			return null;
		}
		return orderTaskName.get(index + 1);
	}

	@Override
	public String getFirstTaskName(String taskType) {
		if(BrStringUtils.isEmpty(taskType)){
			return null;
		}
		List<String> orderTaskName = getTaskList(taskType);
		if(orderTaskName == null || orderTaskName.isEmpty()){
			return null;
		}
		return orderTaskName.get(0);
	}

	@Override
	public boolean changeTaskContentNodeStateByLock(String serverId, String taskName, String taskType, int taskState) {
		try {
			if(BrStringUtils.isEmpty(serverId)){
				return false;
			}
			if (BrStringUtils.isEmpty(taskName)) {
				return false;
			}
			if (BrStringUtils.isEmpty(taskType)) {
				return false;
			}
			String locks = this.taskLockPath + "/" + taskType + "/" + taskName;
			byte[] data = serverId.getBytes(StandardCharsets.UTF_8);
			if(!client.checkExists(locks)){
				client.createEphemeral(locks, true, data);
			}
			byte[]tData = client.getData(locks);
			String zkStr = new String(tData, StandardCharsets.UTF_8);
			if(!serverId.equals(zkStr)){
				return false;
			}
			TaskModel tmp = getTaskContentNodeInfo(taskType, taskName);
			if(tmp == null){
				return false;
			}
			tmp.setTaskState(taskState);
			updateTaskContentNode(tmp, taskType, taskName);
			client.delete(locks, false);
			return true;
		}
		catch (Exception e) {
			LOG.error("{}",e);
		}
		return false;
	}

	@Override
	public List<Pair<String, Integer>> getServerStatus(String taskType, String taskName) {
		List<Pair<String, Integer>> serverStatus = new ArrayList<>();
		List<String> childeServers = getTaskServerList(taskType, taskName);
		if(childeServers == null || childeServers.isEmpty()){
			return serverStatus;
		}
		Pair<String, Integer> stat;
		int iStat;
		TaskServerNodeModel tmpServer;
		for(String child : childeServers){
			stat = new Pair<>();
			tmpServer = getTaskServerContentNodeInfo(taskType, taskName, child);
			stat.setFirst(child);
			iStat = tmpServer == null ? -3 :tmpServer.getTaskState();
			stat.setSecond(iStat);
			serverStatus.add(stat);
		}
		return serverStatus;
	}

	@Override
	public List<String> getTaskServerList(String taskType, String taskName) {
		List<String> childeServers = new ArrayList<>();
		if(BrStringUtils.isEmpty(taskType)){
			return childeServers;
		}
		if(BrStringUtils.isEmpty(taskName)){
			return childeServers;
		}
		String path = this.taskQueue + "/" + taskType + "/" + taskName;
		childeServers = client.getChildren(path);
		if (childeServers == null || childeServers.isEmpty()) {
			return childeServers;
		}
		//升序排列任务
		childeServers.sort(Comparator.naturalOrder());
		return childeServers;
	}
	@Override
	public List<String> getTaskList(String taskType) {
		if (StringUtils.isEmpty(taskType)) {
			throw new NullPointerException("taskType is empty");
		}
		String path = this.taskQueue + "/" + taskType;
		if (!client.checkExists(path)) {
			return null;
		}
		List<String> childNodes = client.getChildren(path);
		if (childNodes == null || childNodes.isEmpty()) {
			return childNodes;
		}
		//升序排列任务
		childNodes.sort(Comparator.naturalOrder());
		return childNodes;
	}

	@Override
	public List<String> getTaskTypeList() {
		List<String> taskTypeList = new ArrayList<>();
		if (!client.checkExists(this.taskQueue)) {
			return taskTypeList;
		}
		taskTypeList = client.getChildren(this.taskQueue);
		return taskTypeList;
	}

	@Override
	public TaskTypeModel getTaskTypeInfo(String taskType) {
		if(BrStringUtils.isEmpty(taskType)) {
			return null;
		}
		String path = this.taskQueue + "/" + taskType;
		if(!client.checkExists(path)) {
			System.out.println(path);
			return null;
		}
		byte[] data = client.getData(path);
		if(data == null || data.length == 0) {
			return null;
		}
		return JsonUtils.toObjectQuietly(data, TaskTypeModel.class);
	}

	@Override
	public boolean setTaskTypeModel(String taskType, TaskTypeModel type) {
		if(BrStringUtils.isEmpty(taskType)) {
			return false;
		}
		if(type == null) {
			return false;
		}
		byte[] data = JsonUtils.toJsonBytesQuietly(type);
		if(data == null || data.length == 0) {
			return false;
		}
		String path = this.taskQueue + "/" + taskType;
		if(client.checkExists(path)) {
			client.setData(path, data);
		}else {
			client.createPersistent(path, true, data);
		}
		return true;
	}

	@Override
	public List<String> getTransferTask(String taskType) {
		if(BrStringUtils.isEmpty(taskType)) {
			return null;
		}
		String path = this.taskTransfer+"/"+taskType;
		if(!client.checkExists(path)) {
			return null;
		}
		return client.getChildren(path);
	}

	@Override
	public boolean deleteTransferTask(String taskType, String taskName) {
		if(BrStringUtils.isEmpty(taskType)) {
			return false;
		}
		if(BrStringUtils.isEmpty(taskName)) {
			return false;
		}
		String path = this.taskTransfer + "/" + taskType + "/" + taskName;
		if(!client.checkExists(path)) {
			return true;
		}
		client.delete(path, true);
		return true;
	}

	@Override
	public boolean setTransferTask(String taskType, String taskName) {
		if(BrStringUtils.isEmpty(taskType)) {
			return false;
		}
		if(BrStringUtils.isEmpty(taskName)) {
			return false;
		}
		String path = this.taskTransfer + "/" + taskType + "/" + taskName;
		if(client.checkExists(path)) {
			return false;
		}
		String str = client.createPersistent(path, true);
		return !BrStringUtils.isEmpty(str);
	}

}
