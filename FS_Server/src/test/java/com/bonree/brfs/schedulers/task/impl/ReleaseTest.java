package com.bonree.brfs.schedulers.task.impl;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.task.TaskState;
import com.bonree.brfs.common.task.TaskType;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.common.utils.Pair;
import com.bonree.brfs.common.zookeeper.ZookeeperClient;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.configuration.Configuration;
import com.bonree.brfs.schedulers.task.manager.MetaTaskManagerInterface;
import com.bonree.brfs.schedulers.task.manager.impl.DefaultReleaseTask;
import com.bonree.brfs.schedulers.task.model.TaskModel;
import com.bonree.brfs.schedulers.task.model.TaskServerNodeModel;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import ch.qos.logback.core.util.StatusPrinter;

public class ReleaseTest {
	private static final Logger LOG = LoggerFactory.getLogger("TEST");
	public static String zkUrl = "192.168.101.86:2181";
	public static String taskRootPath = "/zcg_test/task";
//	@Test
	public static void main(String[] args) throws Exception{
		// 0.创建并初始化初始化接口
		MetaTaskManagerInterface release = DefaultReleaseTask.getInstance();
		release.setPropreties(zkUrl, taskRootPath);
		// 创建任务信息
		String taskType = TaskType.SYSTEM_DELETE.name();
		int stat =  TaskState.EXCEPTION.code();
		
		// 1.发布任务
		Pair<String, List<String>> current = createTask(release, taskType, stat);
		String taskName = current.getKey();
		List<String> servers = current.getValue();
		TaskModel tmp = release.getTaskContentNodeInfo(taskType, taskName);
		if(tmp != null){
			LOG.info("after {}, {}", taskName, tmp.getCreateTime());
		}
		TaskServerNodeModel a = new TaskServerNodeModel();
		a.setTaskState(TaskState.UNKNOW.code());
		release.updateServerTaskContentNode("9999", taskName, TaskType.SYSTEM_DELETE.name(), a);
		
		// 3.查找指定服务最后一次执行成功的
//		LOG.info("success server {} , taskName : {}","0", release.getLastSuccessTaskIndex(TaskType.SYSTEM_DELETE.name(), "0"));
		// 4.查询指定任务的状态
//		LOG.info("query stat : {}", release.queryTaskState(taskName, TaskType.SYSTEM_DELETE.name()));
		// 5.获取当前任务最大序号节点
		LOG.info("current :{}", release.getCurrentTaskIndex(TaskType.SYSTEM_DELETE.name()));
		LOG.info("==============================");
		tmp = release.getTaskContentNodeInfo(taskType, taskName);
		if(tmp != null){
			LOG.info("after {}, {}", taskName, TaskState.valueOf(tmp.getTaskState()).name());
		}
		TaskServerNodeModel server = release.getTaskServerContentNodeInfo(taskType, taskName, "9999");
		if(server != null){
			LOG.info("after {}, {} {}", taskName, TaskState.valueOf(server.getTaskState()).name(), "9999");
		}
		// 7.维护任务的节点状态
		Pair<Integer, Integer> pair = release.reviseTaskStat(TaskType.SYSTEM_DELETE.name(), 10000000, servers);
		LOG.info("delete : {}, recover :{}", pair.getKey(), pair.getValue());
		tmp = release.getTaskContentNodeInfo(taskType, taskName);
		if(tmp != null){
			LOG.info("after {}, {}", taskName, TaskState.valueOf(tmp.getTaskState()).name());
		}
		server = release.getTaskServerContentNodeInfo(taskType, taskName, "9999");
		if(server != null){
			LOG.info("after {}, {} {}", taskName, TaskState.valueOf(server.getTaskState()).name(), "9999");
		}
		
		// 8.获取任务对象
		TaskModel task1 = release.getTaskContentNodeInfo(TaskType.SYSTEM_DELETE.name(), taskName);
		TaskServerNodeModel serverTask = release.getTaskServerContentNodeInfo(TaskType.SYSTEM_DELETE.name(), taskName, "0");
		tmp = release.getTaskContentNodeInfo(taskType, taskName);
		if(tmp != null){
			LOG.info("after {}, {} {}", taskName, TaskState.valueOf(tmp.getTaskState()).name(), tmp.getCreateTime());
		}
		// 9.删除任务
//		release.deleteTask(taskName, TaskType.SYSTEM_DELETE.name());
//		LOG.info("delete time {}, delete index {}", System.currentTimeMillis() - 60*1000, release.deleteTasks(System.currentTimeMillis() - 60*60*1000, TaskType.SYSTEM_DELETE.name()));
	}
	
	public static Pair<String,List<String>>createTask(MetaTaskManagerInterface release, String taskType, int stat){
		// 创建任务信息
		Pair<String, List<String>> test = new Pair<String,List<String>>();
		TaskModel task1 = new TaskModel();
		task1.setCreateTime(System.currentTimeMillis());
		task1.setTaskState(stat);
		TaskServerNodeModel serverTask = new TaskServerNodeModel();
		serverTask.setTaskState(stat);

		// 1.发布任务
		String taskName = "";
		taskName = release.updateTaskContentNode(task1, TaskType.SYSTEM_DELETE.name(), null);
		List<String> servers = new ArrayList<String>();
		// 2.发布服务任务节点
		for (int i = 0; i < 2; i++) {
			servers.add(i + "");
			release.updateServerTaskContentNode(i + "", taskName, TaskType.SYSTEM_DELETE.name(), serverTask);
		}
		test.setKey(taskName);
		test.setValue(servers);
		return test;
	}
}
