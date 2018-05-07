
package com.bonree.brfs.schedulers.task.manager.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import javax.transaction.Synchronization;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.task.TaskState;
import com.bonree.brfs.common.task.TaskType;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.common.utils.Pair;
import com.bonree.brfs.common.zookeeper.ZookeeperClient;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.schedulers.task.manager.MetaTaskManagerInterface;
import com.bonree.brfs.schedulers.task.model.TaskModel;
import com.bonree.brfs.schedulers.task.model.TaskServerNodeModel;

public class DefaultReleaseTask implements MetaTaskManagerInterface {
	private static final Logger LOG = LoggerFactory.getLogger(DefaultReleaseTask.class);
	private String zkUrl = null;
	private String taskRootPath = null;
	private String taskLockPath = null;
	private ZookeeperClient client = null;
	private static class releaseInstance {
		public static DefaultReleaseTask instance = new DefaultReleaseTask();
	}

	private DefaultReleaseTask() {

	}

	public static DefaultReleaseTask getInstance() {
		return releaseInstance.instance;
	}

	private List<String> getOrderTaskInfos(String taskType) {
		if (StringUtils.isEmpty(taskType)) {
			throw new NullPointerException("taskType is empty");
		}
		StringBuilder pathBuilder = new StringBuilder();
		pathBuilder.append(this.taskRootPath).append("/").append(taskType);
		String path = pathBuilder.toString();
		if (!client.checkExists(path)) {
//			throw new NullPointerException(taskType + " is not exists");
			return null;
		}
		List<String> childNodes = client.getChildren(path);
		if (childNodes == null || childNodes.isEmpty()) {
			return childNodes;
		}
		//升序排列任务
		Collections.sort(childNodes, new Comparator<String>() {
			public int compare(String o1, String o2) {
				return o1.compareTo(o2);
			}
		});
		return childNodes;
	}
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
			pathBuilder.append(this.taskRootPath).append("/").append(taskType).append("/");
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
			e.printStackTrace();
		}
		return pathNode;
	}

	@Override
	public String getCurrentTaskIndex(String taskType){
		try {
			List<String> taskInfos = getOrderTaskInfos(taskType);
			if(taskInfos == null || taskInfos.isEmpty()){
				return null;
			}
			return taskInfos.get(taskInfos.size() - 1);
		}
		catch (Exception e) {
			e.printStackTrace();
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
			taskPath.append(taskRootPath).append("/").append(taskType).append("/").append(taskName);
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
			e.printStackTrace();
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
			taskPath.append(taskRootPath).append("/").append(taskType).append("/").append(taskName).append("/").append(serverId);
			String path = taskPath.toString();
			if (client.checkExists(path)) {
				client.setData(path, datas);
			} else {
				client.createPersistent(path, true, datas);
			}
			return true;
		}
		catch (Exception e) {
			e.printStackTrace();
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
			e.printStackTrace();
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
			e.printStackTrace();
		}
		return false;
	}

	@Override
	public String getLastSuccessTaskIndex(String taskType, String serverId){
		try {
			List<String> taskInfos = getOrderTaskInfos(taskType);
			if(taskInfos == null || taskInfos.isEmpty()){
				return null;
			}
			int maxIndex = taskInfos.size() - 1;
			StringBuilder path = null;
			StringBuilder pPath = null;
			String taskName = null;
			byte[] data = null;
			String taskPath = null;
			int lastLostIndex = -1;
			for (int i = maxIndex; i >= 0; i--) {
				taskName = taskInfos.get(i);
				path = new StringBuilder();
				path.append(this.taskRootPath).append("/").append(taskType).append("/").append(taskName).append("/").append(
					serverId);
				taskPath = path.toString();
				if (!client.checkExists(taskPath)) {
					continue;
				}
				int stat = queryTaskState(taskName, taskType);
				// 不成功的跳过
				if (stat != TaskState.FINISH.code()) {
					continue;
				}
				return taskInfos.get(i);
			}
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	@Override
	public String getFirstServerTask(String taskType,String serverId){
		List<String> taskInfos = getOrderTaskInfos(taskType);
		if(taskInfos == null || taskInfos.isEmpty()){
			return null;
		}
		int maxIndex = taskInfos.size() - 1;
		StringBuilder path = null;
		StringBuilder pPath = null;
		String taskName = null;
		byte[] data = null;
		String taskPath = null;
		int lastLostIndex = -1;
		for (int i = maxIndex; i >= 0; i--) {
			taskName = taskInfos.get(i);
			path = new StringBuilder();
			path.append(this.taskRootPath).append("/").append(taskType).append("/").append(taskName).append("/").append(
				serverId);
			taskPath = path.toString();
			if (!client.checkExists(taskPath)) {
				continue;
			}
			return taskInfos.get(i);
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
			StringBuilder pathBuilder = new StringBuilder();
			pathBuilder.append(this.taskRootPath).append("/").append(taskType).append("/").append(taskName);
			String path = pathBuilder.toString();
			if (!client.checkExists(path)) {
				return false;
			}
			client.delete(path, true);
			return true;
		}
		catch (Exception e) {
			e.printStackTrace();
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
			List<String> nodes = getOrderTaskInfos(taskType);
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
			long cTime = 0l;
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
			e.printStackTrace();
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
			StringBuilder pathBuilder = new StringBuilder();
			pathBuilder.append(this.taskRootPath).append("/").append(taskType).append("/").append(taskName);
			String path = pathBuilder.toString();
			if (!client.checkExists(path)) {
				return -3;
			}
			byte[] data = null;
			data = client.getData(path);
			if (data == null || data.length == 0) {
				return -4;
			}
			TaskModel taskInfo = JsonUtils.toObject(data, TaskModel.class);
			return taskInfo.getCreateTime();
		}
		catch (Exception e) {
			e.printStackTrace();
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
			client = CuratorClient.getClientInstance(this.zkUrl);
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public Pair<Integer,Integer> reviseTaskStat(String taskType, long ttl, Collection<String> aliveServers){
		Pair<Integer, Integer> counts = new Pair<Integer, Integer>(0, 0);
		try {
			if(BrStringUtils.isEmpty(taskType)){
				throw new NullPointerException("taskType is empty");
			}
			if(aliveServers == null || aliveServers.isEmpty()){
				throw new NullPointerException("alive servers is empty");
			}
			// 获取子任务名称队列
			List<String> taskQueues = getOrderTaskInfos(taskType);
			if(taskQueues == null || taskQueues.isEmpty()){
				return counts;
			}
			// 删除任务
			int deleteCount = deleteTasks(taskQueues, taskType, ttl);
			// 维护任务状态
			int reviseCount = reviseTaskState(taskQueues, aliveServers, taskType, deleteCount);
			counts.setKey(deleteCount);
			counts.setValue(reviseCount);
		}
		catch (Exception e) {
			e.printStackTrace();
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
			StringBuilder taskPath = null;
			String taskName;
			String tmpPath = null;
			TaskModel taskContent = null;
			TaskServerNodeModel taskServer = null;
			List<String> cServers = null;
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
				taskPath.append(taskRootPath).append("/").append(taskType).append("/").append(taskName);
				tmpPath = taskPath.toString();
				cServers = client.getChildren(tmpPath);
				if((cServers == null || cServers.isEmpty()) && !exceptionFlag){
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
			e.printStackTrace();
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
			int size = taskQueue.size();
			//循环删除数据
			int count = 0;
			long cTime = 0l;
			String taskName = null;
			for (int i = 0; i < size ; i++) {
				taskName = taskQueue.get(i);
				if (BrStringUtils.isEmpty(taskName)) {
					continue;
				}
				cTime = getTaskCreateTime(taskName, taskType);
				if (cTime > deleteTime) {
					break;
				}
				if(cTime == -3){
					LOG.warn("taskType:{}, taskName :{} is not exists ! skip it", taskType, taskName);
					continue;
				}
				if(cTime == 0){
					LOG.warn("taskType:{}, taskName :{} create time is 0 ! will delete", taskType, taskName);
				}
				if(cTime == -4){
					LOG.warn("delete taskType: {}, taskName: {}, content: taskcontent is null", taskType, taskName);
				}
				if (deleteTask(taskName, taskType)) {
					count++;
				}
			}
			LOG.info("delete time out task complete from");
			return count;
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		return 0;
	}
	/**
	 * 概述：获取队列有效的创建时间，若存在无效的任务节点将被删除
	 * @param taskQueue
	 * @param taskType
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	private Pair<Integer, Long> getAvailableFirstTime(List<String> taskQueue, String taskType){
		int index = 0;
		long createTime = 0l;
		long tmp;
		for(String taskName : taskQueue){
			tmp = getTaskCreateTime(taskName, taskType);
			LOG.info("select taskName :{} createTime :{}", taskName, tmp);
			if(tmp > 0){
				createTime = tmp;
				break;
			}
			if(createTime == -4){
				LOG.warn("Delete taskType : {}, taskName : {}, content : taskcontent is null", taskType, taskName);
				deleteTask(taskName, taskType);
			}
			index ++;
		}
		return new Pair<Integer,Long>(index,createTime);
	}

	@Override
	public TaskModel getTaskContentNodeInfo(String taskType, String taskName) {
		TaskModel taskContent = null;
		try {
			if (BrStringUtils.isEmpty(taskName)) {
				return taskContent;
			}
			if (BrStringUtils.isEmpty(taskType)) {
				return taskContent;
			}
			StringBuilder taskPath = new StringBuilder();
			taskPath.append(taskRootPath).append("/").append(taskType).append("/").append(taskName);
			String path = taskPath.toString();
			if(!client.checkExists(path)){
				return taskContent;
			}
			byte[] data = client.getData(path);
			if (data == null || data.length == 0) {
				return taskContent;
			}
			taskContent = JsonUtils.toObject(data, TaskModel.class);
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		return taskContent;
	}

	@Override
	public TaskServerNodeModel getTaskServerContentNodeInfo(String taskType, String taskName, String serverId) {
		TaskServerNodeModel taskServerNode = null;
		try {
			if (BrStringUtils.isEmpty(taskName)) {
				return taskServerNode;
			}
			if (BrStringUtils.isEmpty(taskType)) {
				return taskServerNode;
			}
			StringBuilder taskPath = new StringBuilder();
			taskPath.append(taskRootPath).append("/").append(taskType).append("/").append(taskName).append("/").append(serverId);
			String path = taskPath.toString();
			byte[] data = client.getData(path);
			if (data == null || data.length == 0) {
				return taskServerNode;
			}
			taskServerNode = JsonUtils.toObject(data, TaskServerNodeModel.class);
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		return taskServerNode;
	}

	@Override
	public String getNextTaskName(String taskType, String taskName) {
		if(BrStringUtils.isEmpty(taskType) || BrStringUtils.isEmpty(taskName)){
			return null;
		}
		List<String> orderTaskName = getOrderTaskInfos(taskType);
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
		List<String> orderTaskName = getOrderTaskInfos(taskType);
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
			StringBuilder lockPath = new StringBuilder();
			lockPath.append(this.taskLockPath).append("/").append(taskType).append("/").append(taskName);
			String locks = lockPath.toString();
			byte[] data = serverId.getBytes("UTF-8");
			if(!client.checkExists(locks)){
				client.createEphemeral(locks, true, data);
			}
			byte[]tData = client.getData(locks);
			String zkStr = new String(tData,"UTF-8");
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
			e.printStackTrace();
		}
		return false;
	}

	@Override
	public List<Pair<String, Integer>> getServerStatus(String taskType, String taskName) {
		List<Pair<String, Integer>> serverStatus = new ArrayList<Pair<String,Integer>>();
		if(BrStringUtils.isEmpty(taskType)){
			return serverStatus;
		}
		if(BrStringUtils.isEmpty(taskName)){
			return serverStatus;
		}
		StringBuilder pathStr = new StringBuilder();
		pathStr.append(this.taskRootPath).append("/").append(taskType).append("/").append(taskName);
		String path = pathStr.toString();
		List<String> childeServers = client.getChildren(path);
		if(childeServers == null || childeServers.isEmpty()){
			return serverStatus;
		}
		Pair<String, Integer> stat = null;
		int iStat = -1;
		TaskServerNodeModel tmpServer = null;
		for(String child : childeServers){
			stat = new Pair<String, Integer>();
			tmpServer = getTaskServerContentNodeInfo(taskType, taskName, child);
			stat.setKey(child);
			iStat = tmpServer == null ? -3 :tmpServer.getTaskState();
			stat.setValue(iStat);
		}
		return serverStatus;
	}

}
