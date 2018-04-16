package com.bonree.brfs.schedulers.task.impl;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.common.utils.Pair;
import com.bonree.brfs.common.zookeeper.ZookeeperClient;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.configuration.Configuration;
import com.bonree.brfs.schedulers.task.TaskStat;
import com.bonree.brfs.schedulers.task.TaskType;
import com.bonree.brfs.schedulers.task.manager.MetaTaskManagerInterface;
import com.bonree.brfs.schedulers.task.manager.impl.DefaultReleaseTask;
import com.bonree.brfs.schedulers.task.model.TaskModel;
import com.bonree.brfs.schedulers.task.model.TaskServerNodeModel;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import ch.qos.logback.core.util.StatusPrinter;

public class ReleaseTest {
	static {
        // 加载 logback配置信息
        try {
            LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
            JoranConfigurator configurator = new JoranConfigurator();
            configurator.setContext(lc);
            lc.reset();
            configurator.doConfigure(Configuration.class.getResourceAsStream("/logback.xml"));
            StatusPrinter.printInCaseOfErrorsOrWarnings(lc);
        } catch (JoranException e) {
            e.printStackTrace();
            System.exit(0);
        }
    }
	private static final Logger LOG = LoggerFactory.getLogger("TEST");
	public static String zkUrl = "192.168.101.86:2181";
	public static String serverPath = "/zcg/servers";
	public static String taskRootPath = "/zcg/task";
//	@Test
	public void test() throws Exception{
		// 0.创建并初始化初始化接口
		MetaTaskManagerInterface release = DefaultReleaseTask.getInstance();
		release.setPropreties(zkUrl, taskRootPath);
		// 创建任务信息
		TaskModel task1 = new TaskModel();
		task1.setCreateTime(System.currentTimeMillis());
		task1.setTaskState(TaskStat.RUN.code());
		TaskServerNodeModel serverTask = new TaskServerNodeModel();
		serverTask.setTaskState(TaskStat.RUN.code());

		
		// 1.发布任务
		String taskName = release.updateTaskContentNode(task1, TaskType.SYSTEM_DELETE.name(), null);
		List<String> servers = new ArrayList<String>();
		// 2.发布服务任务节点
		for(int i = 0; i< 7; i++){
			servers.add(i+"");
			release.updateServerTaskContentNode(i+"", taskName, TaskType.SYSTEM_DELETE.name(), serverTask);
		}
		release.updateServerTaskContentNode("9999", taskName, TaskType.SYSTEM_DELETE.name(), serverTask);
		// 3.查找指定服务最后一次执行成功的
		LOG.info("success server {} , taskName : {}","0", release.getLastSuccessTaskIndex(TaskType.SYSTEM_DELETE.name(), "0"));
		// 4.查询指定任务的状态
		LOG.info("query stat : {}", release.queryTaskState(taskName, TaskType.SYSTEM_DELETE.name()));
		// 5.获取当前任务最大序号节点
		LOG.info("current :{}", release.getCurrentTaskIndex(TaskType.SYSTEM_DELETE.name()));
		
		// 7.维护任务的节点状态
		serverTask.setTaskStartTime(TaskStat.RUN.code());
		Pair<Integer, Integer> pair = release.reviseTaskStat(TaskType.SYSTEM_DELETE.name(), 600000l, servers);
		LOG.info("delete : {}, recover :{}", pair.getKey(), pair.getValue());
		// 8.获取任务对象
		task1 = release.getTaskContentNodeInfo(TaskType.SYSTEM_DELETE.name(), taskName);
		serverTask = release.getTaskServerContentNodeInfo(TaskType.SYSTEM_DELETE.name(), taskName, "0");
		// 9.删除任务
		release.deleteTask(taskName, TaskType.SYSTEM_DELETE.name());
		LOG.info("delete time {}, delete index {}", System.currentTimeMillis() - 60*1000, release.deleteTasks(System.currentTimeMillis() - 60*60*1000, TaskType.SYSTEM_DELETE.name()));
	}
	public static TaskModel createTaskContent(int index){
		TaskModel task = new TaskModel();
		task.setCreateTime(System.currentTimeMillis());
		task.setTaskType(1);
		task.setTaskState(index);
		return task;
	}
	public static void updateServerInfo(String zkUrl, List<Service> servers, String serverPath){
		ZookeeperClient zkClient = CuratorClient.getClientInstance(zkUrl);
		byte[] data = null;
		String sPath = null;
		for(Service server : servers){
			data = JsonUtils.toJsonBytes(server);
			if(data == null || data.length == 0){
				continue;
			}
			sPath = serverPath + "/"+server.getServiceId();
			LOG.info("server --- {}", sPath);
			if(zkClient.checkExists(sPath)){
				continue;
			}
			zkClient.createPersistent(sPath, true, data);
			
		}
		zkClient.close();
	}
	public static List<Service> createServerInfo(int count){
		List<Service> servers = new ArrayList<Service>();
		Service tmp = null;
		for(int i = 0; i< count ; i++){
			tmp = new Service();
			tmp.setServiceId(i+"");
			servers.add(tmp);
		}
		return servers;
	}
}
