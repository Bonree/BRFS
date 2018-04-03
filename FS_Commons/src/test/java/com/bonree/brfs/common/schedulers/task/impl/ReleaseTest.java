package com.bonree.brfs.common.schedulers.task.impl;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.schedulers.model.TaskContent;
import com.bonree.brfs.common.schedulers.task.MetaTaskManagerInterface;
import com.bonree.brfs.common.schedulers.task.TaskType;
import com.bonree.brfs.common.schedulers.task.impl.ReleaseTaskOperation;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.common.zookeeper.ZookeeperClient;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.configuration.Configuration;

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
	@Test
	public void test() throws Exception{
		List<Service> sss = createServerInfo(4);
		updateServerInfo(zkUrl, sss, serverPath);
		LOG.info("release task ......................................................................");
		ZookeeperClient zkClient = CuratorClient.getClientInstance(zkUrl);
		LOG.info("USERDELETE's child {}",zkClient.getChildren("/zcg/task/USER_DELETE"));
		MetaTaskManagerInterface release = ReleaseTaskOperation.getInstance().setPropreties(zkUrl, taskRootPath, serverPath);
		TaskContent tt = createTaskContent(4);
		byte[] data = JsonUtils.toJsonBytes(tt);
		// 1.发布任务
//		release.releaseTaskContentNode(data, "USER_DELETE");
		// 删除单个任务
//		release.deleteTask("40000000004", "USER_DELETE");
		// 2.删除任务
		LOG.info("delete time {}, delete index {}", System.currentTimeMillis() - 60*1000, release.deleteTasks(System.currentTimeMillis() - 60*60*1000, "USER_DELETE"));
		// 3.获取最近成功的任务
		int serverId = 1;
		LOG.info("success server {} , taskName : {}",serverId, release.getLastSuccessTaskIndex("USER_DELETE", serverId +""));
		LOG.info("query stat : {}", release.queryTaskState("40000000006", "USER_DELETE"));
		LOG.info("query stat : {}", release.queryTaskState("40000000007", "USER_DELETE"));
		LOG.info("query stat : {}", release.queryTaskState("40000000008", "USER_DELETE"));
		// 获取当前任务最大序号节点
//		LOG.info("current :{}", release.getCurrentTaskIndex("USER_DELETE"));
		//添加server
//		LOG.info("{}'s childe{}","40000000007",zkClient.getChildren("/zcg/task/USER_DELETE/40000000007"));
//		release.releaseServerTaskContentNode("5", "40000000007", "USER_DELETE", null);
//		LOG.info("{}",zkClient.getChildren("/zcg/task/USER_DELETE/40000000007"));
		// 修改状态
//		release.changeTaskContentNodeStat("40000000007", "USER_DELETE", 0);
//		LOG.info("stat after : {}", release.queryTaskState("40000000007", "USER_DELETE"));
//		
	}
	public static TaskContent createTaskContent(int index){
		TaskContent task = new TaskContent();
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
