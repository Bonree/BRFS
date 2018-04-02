package com.bonree.brfs.common.schedulers.task.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.map.util.Comparators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.schedulers.model.TaskContent;
import com.bonree.brfs.common.schedulers.task.MetaTaskManagerInterface;
import com.bonree.brfs.common.schedulers.task.TaskStat;
import com.bonree.brfs.common.schedulers.task.TaskType;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.common.zookeeper.ZookeeperClient;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;

public class ReleaseTaskOperation implements MetaTaskManagerInterface{
	private static final Logger LOG = LoggerFactory.getLogger(ReleaseTaskOperation.class);
	private String zkUrl = null;
	private String taskRootPath = null;
	private String availbleServerPath = null;
	private ZookeeperClient  client = null;
	private static class releaseInstance{
		public static ReleaseTaskOperation instance = new ReleaseTaskOperation();
	}
	private ReleaseTaskOperation(){
		
	}
	public static ReleaseTaskOperation getInstance(){
		return releaseInstance.instance;
	}
	public ReleaseTaskOperation setPropreties(String zkUrl, String taskRootPath, String avalibleServerPath){
		this.zkUrl = zkUrl;
		this.taskRootPath = taskRootPath;
		this.availbleServerPath = avalibleServerPath;
		if(BrStringUtils.isEmpty(this.zkUrl)){
			throw new NullPointerException("zookeeper address is empty");
		}
		if(BrStringUtils.isEmpty(this.taskRootPath)){
			throw new NullPointerException("task root path is empty");
		}
		if(BrStringUtils.isEmpty(this.availbleServerPath)){
			throw new NullPointerException("availbleServerPath is empty");
		}
		client = CuratorClient.getClientInstance(this.zkUrl);
		return this;
	}
	
	private List<String> getOrderTaskInfos(String taskType){
		if(StringUtils.isEmpty(taskType)){
			throw new NullPointerException("taskType is empty");
		}
		StringBuilder  pathBuilder = new StringBuilder();
		pathBuilder.append(this.taskRootPath)
		.append("/")
		.append(taskType);
		String path = pathBuilder.toString();
		if(!client.checkExists(path)){
			throw new NullPointerException(taskType + " is not exists");
		}
		List<String> childNodes = client.getChildren(path);
		if(childNodes == null || childNodes.isEmpty()){
			throw new NullPointerException(taskType + " tasklist is empty");
		}
		//升序排列任务
		Collections.sort(childNodes, new Comparator<String>() {				
			public int compare(String o1, String o2) {
				return o1.compareTo(o2);
			}
		}); 
		return childNodes;
	}
	@Override
	public String releaseTaskContentNode(byte[] data, String taskType) throws Exception{
		
		if(data == null || data.length == 0){
			throw new NullPointerException("task content is empty");
		}
		if(BrStringUtils.isEmpty(taskType)){
			throw new NullPointerException("task type is empty");
		}
		TaskType current = TaskType.valueOf(taskType);
		int taskTypeIndex = current == null ? 0 : current.code();
		
		StringBuilder  pathBuilder = new StringBuilder();
		pathBuilder.append(this.taskRootPath)
		.append("/")
		.append(taskType)
		.append("/")
		.append(taskTypeIndex);
		String taskPath = pathBuilder.toString();
		if(client.checkExists(taskPath)){
			return null;
		}
		String pathNode = client.createPersistentSequential(taskPath , true, data);
		// 获取最新的节点
		List<String> availServers = getAvaliServer();
		StringBuilder serverPath = null;
		for(String server : availServers){
			serverPath = new StringBuilder();
			serverPath.append(pathNode).append("/").append(server);
			client.createPersistent(serverPath.toString(), false);
		}
		String[] nodes = BrStringUtils.getSplit(pathNode, "/");
		if(nodes != null && nodes.length != 0){
			return nodes[nodes.length -1];
		}
		return pathNode;
	}

	@Override
	public String getCurrentTaskIndex(String taskType) throws Exception{
		List<String> taskInfos = getOrderTaskInfos(taskType);
		return taskInfos.get(taskInfos.size() -1);
	}

	@Override
	public int queryTaskState(String taskName, String taskType) throws Exception {
		// TODO Auto-generated method stub
		if (BrStringUtils.isEmpty(taskName)) {
			return -1;
		}
		if (BrStringUtils.isEmpty(taskType)) {
			return -1;
		}
		StringBuilder taskPath = new StringBuilder();
		taskPath.append(taskRootPath).append("/").append(taskType).append("/").append(taskName);
		String path = taskPath.toString();
		if(!client.checkExists(path)){
			return TaskStat.UNKONW.code();
		}
		byte[] data = client.getData(path);
		if (data == null || data.length == 0) {
			return TaskStat.UNKONW.code();
		}
		TaskContent tmp = JsonUtils.toObject(data, TaskContent.class);
		return tmp.getTaskState();
	}
	
	private List<String> getAvaliServer(){
		List<String> serverList = new ArrayList<String>();
		List<String> servers = client.getChildren(this.availbleServerPath);
		StringBuilder pathBuilder = null;
		String path = null;
		Service single = null;
		byte[] content = null;
		for(String server : servers){
			pathBuilder = new StringBuilder();
			pathBuilder.append(this.availbleServerPath).append("/")
			.append(server);
			path = pathBuilder.toString();
			if(!client.checkExists(path)){
				continue;
			}
			content = client.getData(path);
			if(content == null || content.length == 0){
				continue;
			}
			single = JsonUtils.toObject(content, Service.class);
			serverList.add(single.getServiceId());
		}
		return serverList;
	}
	@Override
	public boolean releaseServerTaskContentNode(String serverId, String taskName, String taskType, byte[] data) throws Exception {
		// TODO Auto-generated method stub
		if(BrStringUtils.isEmpty(serverId)){
			return false;
		}
		if(BrStringUtils.isEmpty(taskName)){
			return false;
		}
		if(BrStringUtils.isEmpty(taskType)){
			return false;
		}
		StringBuilder taskPath = new StringBuilder();
		taskPath.append(taskRootPath).append("/")
		.append(taskType).append("/")
		.append(taskName).append("/")
		.append(serverId);
		String path = taskPath.toString();
		if(client.checkExists(path)){
			return false;
		}
		System.out.println(path);
		if(data == null || data.length == 0){
			client.createPersistent(path, true);
		} else{
			client.createPersistent(path, true, data);
		}
		return true;
	}
	@Override
	public boolean changeTaskContentNodeStat(String taskName, String taskType, int taskState) throws Exception {
		// TODO Auto-generated method stub
		if(BrStringUtils.isEmpty(taskName)){
			return false;
		}
		if(BrStringUtils.isEmpty(taskType)){
			return false;
		}
		StringBuilder taskPath = new StringBuilder();
		taskPath.append(taskRootPath).append("/")
		.append(taskType).append("/")
		.append(taskName);
		String path = taskPath.toString();
		byte[] data = client.getData(path);
		if(data == null || data.length ==0){
			return false;
		}
		TaskContent tmp = JsonUtils.toObject(data, TaskContent.class); 
		tmp.setTaskState(taskState);
		data = JsonUtils.toJsonBytes(tmp);
		client.setData(path, data);
		return true;
	}
	@Override
	public String getLastSuccessTaskIndex(String taskType, String serverId) throws Exception {
		List<String> taskInfos = getOrderTaskInfos(taskType);
		int maxIndex = taskInfos.size() -1;
		StringBuilder path = null;
		StringBuilder pPath = null;
		String taskName = null;
		byte[] data = null;
		String taskPath = null;
		int lastLostIndex = -1;
		for(int i = maxIndex; i >= 0 ; i--){
			taskName = taskInfos.get(i);
			path = new StringBuilder();
			path.append(this.taskRootPath).append("/").append(taskType).append("/").append(taskName).append("/").append(serverId);
			taskPath = path.toString();
			if(!client.checkExists(taskPath)){
				continue;
			}
			int stat = queryTaskState(taskName, taskType);
			// 不成功的跳过
			if(stat != TaskStat.FINISH.code()){
				lastLostIndex = i;
				continue;
			}
			return taskInfos.get(i);
		}
		if(lastLostIndex != -1){
			return taskInfos.get(lastLostIndex);
		}
		return null;
	}
	@Override
	public boolean deleteTask(String taskName, String taskType) {
		if(BrStringUtils.isEmpty(taskName)){
			return false;
		}
		if(BrStringUtils.isEmpty(taskType)){
			return false;
		}
		StringBuilder pathBuilder = new StringBuilder();
		pathBuilder.append(this.taskRootPath).append("/")
		.append(taskType).append("/")
		.append(taskName);
		String path = pathBuilder.toString();
		if(!client.checkExists(path)){
			return false;
		}
		client.delete(path, true);
		return true;
	}
	@Override
	public int deleteTasks(long deleteTime, String taskType) {
		if(BrStringUtils.isEmpty(taskType)){
			return -1;
		}
		// TODO Auto-generated method stub
		List<String> nodes = getOrderTaskInfos(taskType);
		int size = nodes.size();
		if(size == 0){
			return 0;
		}
		long firstTime = getTaskCreateTime(nodes.get(0), taskType);
		if(deleteTime < firstTime){
			return 0;
		}
		//循环删除数据
		int count  = 0;
		long cTime = 0l;
		for(String taskName : nodes){
			if(BrStringUtils.isEmpty(taskName)){
				continue;
			}
			cTime = getTaskCreateTime(taskName, taskType);
			if(cTime > deleteTime){
				continue;
			}
			if(deleteTask(taskName, taskType)){
				count ++;
			}
		}
		return count;
	}
	private long getTaskCreateTime(String taskName, String taskType){
		if(BrStringUtils.isEmpty(taskName)){
			return -1;
		}
		if(BrStringUtils.isEmpty(taskType)){
			return -1;
		}
		StringBuilder pathBuilder = new StringBuilder();
		pathBuilder.append(this.taskRootPath).append("/")
		.append(taskType).append("/")
		.append(taskName);
		String path = pathBuilder.toString();
		if(!client.checkExists(path)){
			return -1;
		}
		byte[] data = null;
		data = client.getData(path);
		if(data == null || data.length == 0){
			return -1;
		}
		TaskContent taskInfo = JsonUtils.toObject(data, TaskContent.class);
		return taskInfo.getCreateTime();
	}
}
