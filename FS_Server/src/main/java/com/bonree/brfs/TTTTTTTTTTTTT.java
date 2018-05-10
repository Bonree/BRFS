
package com.bonree.brfs;

import static org.junit.Assert.*;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.service.ServiceStateListener;
import com.bonree.brfs.common.service.impl.DefaultServiceManager;
import com.bonree.brfs.common.task.TaskType;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.configuration.Configuration;
import com.bonree.brfs.configuration.Configuration.ConfigException;
import com.bonree.brfs.duplication.storagename.DefaultStorageNameManager;
import com.bonree.brfs.duplication.storagename.StorageNameManager;
import com.bonree.brfs.duplication.storagename.StorageNameNode;
import com.bonree.brfs.resourceschedule.commons.GatherResource;
import com.bonree.brfs.resourceschedule.model.BaseMetaServerModel;
import com.bonree.brfs.resourceschedule.model.ResourceModel;
import com.bonree.brfs.resourceschedule.model.ServerModel;
import com.bonree.brfs.resourceschedule.model.StatServerModel;
import com.bonree.brfs.resourceschedule.model.StateMetaServerModel;
import com.bonree.brfs.resourceschedule.service.AvailableServerInterface;
import com.bonree.brfs.configuration.ResourceTaskConfig;
import com.bonree.brfs.configuration.ServerConfig;
import com.bonree.brfs.schedulers.InitTaskManager;
import com.bonree.brfs.schedulers.ManagerContralFactory;
import com.bonree.brfs.schedulers.exception.ParamsErrorException;
import com.bonree.brfs.schedulers.task.manager.RunnableTaskInterface;
import com.bonree.brfs.schedulers.task.manager.SchedulerManagerInterface;
import com.bonree.brfs.schedulers.task.model.TaskModel;
import com.bonree.brfs.schedulers.task.model.TaskRunPattern;

public class TTTTTTTTTTTTT {
	private static final Logger LOG = LoggerFactory.getLogger("Demo");

	public static void main(String[] args) {
				testInit();
	}
	//	@Test
	public static void testInit() {
		Configuration configuration = Configuration.getInstance();
		try {
			String serviceId = "2";
			configuration.parse("E:/tmp/server_default.properties");
			configuration.printConfigDetail();
			ServerConfig config = ServerConfig.parse(configuration, "E:/");
			String clusterName = config.getClusterName();
			CuratorFramework client = null;
			RetryPolicy retryPolicy = new RetryNTimes(3, 1000);
			client = CuratorFrameworkFactory.newClient(config.getZkHosts(), retryPolicy);
			client.start();
			client.blockUntilConnected();
			ServiceManager sm = new DefaultServiceManager(client.usingNamespace("brfs"));
			sm.start();
			Service service = createService(clusterName, serviceId);
			sm.registerService(service);
			sm.addServiceStateListener(service.getServiceGroup(), new ServiceStateListener() {
				@Override
				public void serviceRemoved(Service service) {
					System.out.println("remove--" + service.getServiceId());
				}

				@Override
				public void serviceAdded(Service service) {
					System.out.println("add--" + service.getServiceId());
				}
			});
			StorageNameManager snm = new DefaultStorageNameManager(client.usingNamespace("brfs"));
			snm.start();
//			snm.createStorageName("zcg", 1, 1000);
			InitTaskManager.initManager(configuration, sm,snm,clusterName,serviceId,clusterName,true);
			System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
//			Thread.sleep(100000);
//			// 元数据操作
//			ManagerContralFactory mcf = ManagerContralFactory.getInstance();
//			AvailableServerInterface asi = mcf.getAsm();
//			String MetaserverId = asi.selectAvailableServer(0, "zcg");
//			LOG.info("server Id :{}", MetaserverId);
//			// 写操作
//			String WriteServerId = asi.selectAvailableServer(1, "zcg");
//			LOG.info("server Id :{}", WriteServerId);
//			// 读操作
//			String ReadServerId = asi.selectAvailableServer(2, "zcg");
//			LOG.info("server Id :{}", ReadServerId);
//			// 可用任务接口测试
//			RunnableTaskInterface rti = mcf.getRt();
//			SchedulerManagerInterface schd = mcf.getStm();
//			for(TaskType taskType : TaskType.values()){
//				boolean isRun = rti.taskRunnable(taskType.code(), 3,schd.getSumbitedTaskCount(taskType.name()));
//				if(isRun){
//					TaskModel task = new TaskModel();
//					task.setTaskType(taskType.code());
//					TaskRunPattern runPattern = rti.taskRunnPattern(task);
//					LOG.info("is Run :{}, pattern :{}", isRun, JsonUtils.toJsonString(runPattern));
//				}
//				
//			}
			Thread.sleep(Long.MAX_VALUE);
		}
		catch (ConfigException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		catch (ParamsErrorException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	// TODO:此方法中需要添加报警日志
	private static void updateResource(String zkUrl, String groupName, String serverId, String dataDir, long inverTime)
			throws Exception {

		CuratorFramework client = null;
		StorageNameManager snManager = null;
		ServiceManager sm = null;
		try {
			// 0.计算原始状态信息
			//				List<StatServerModel> lists = GatherResource.calcState(queue);
			//				if(lists == null || lists.isEmpty()){
			//					return;
			//				}
			// 1.创建zk客户端
			RetryPolicy retryPolicy = new RetryNTimes(3, 1000);
			client = CuratorFrameworkFactory.newClient(zkUrl, retryPolicy);
			client.start();
			client.blockUntilConnected();

			// 1-1初始化storagename管理器
			// TODO:俞朋 的 获取storageName
//			snManager = new DefaultStorageNameManager(client.usingNamespace(groupName));
//			snManager.start();
			// 1-2.初始化service管理器
			sm = new DefaultServiceManager(client);
			sm.start();
			sm.addServiceStateListener(groupName, new ServiceStateListener() {
				@Override
				public void serviceRemoved(Service service) {
					LOG.error("{} {}----- remover", service.getServiceGroup(), service.getServiceId());
				}
				@Override
				public void serviceAdded(Service service) {
					// TODO Auto-generated method stub
					System.out.println("---------------"+JsonUtils.toJsonString(service));
				}
			});

			// 2.获取storage信息
//			List<StorageNameNode> storageNames = snManager.getStorageNameNodeList();
			LOG.info("storage name test -----");
			//				List<String> storagenameList = getStorageNames(storageNames);
			// 3.计算状态值
			//				StatServerModel sum = GatherResource.calcStatServerModel(lists, storagenameList, inverTime, dataDir);
			//				if (sum == null) {
			//					return;
			//				}
			// 4.获取集群基础信息
			List<Service> serverList = sm.getServiceListByGroup(groupName);
			
			LOG.info("server id test -----{}", serverList);
			// 5.计算集群基础基础信息
			//				BaseMetaServerModel base = calcCluster(serverList);
			//				if (base == null) {
			//					return;
			//				}
			// 6.获取本机信息
			Service server = sm.getServiceById(groupName, serverId);
			System.out.println(server);
			//				String content = server.getPayload();
			//				ServerModel sinfo = checkAndCreateServerModel(content, serverId, dataDir);
			// 7.计算Resource值
			//				ResourceModel resource = GatherResource.calcResourceValue(base, sum);
			//				if (resource == null) {
			//					return;
			//				}
			//				sinfo.setResource(resource);
			//				String result = JsonUtils.toJsonString(sinfo);
			//				if (BrStringUtils.isEmpty(result)) {
			//					return;
			//				}
			// 8.更新到zk
			Thread.sleep(20000);
			sm.updateService(groupName, serverId, "wtf");
			LOG.info("update zookeeper complete");
		}
		finally {
//			if (client != null) {
//				client.close();
//			}
		}

	}

	public static Service createService(String groupName, String servericeId) {
		Service service = new Service();
		service.setHost("localhost");
		service.setServiceGroup(groupName);
		service.setServiceId(servericeId);
		service.setPort(122);
		return service;
	}

}
