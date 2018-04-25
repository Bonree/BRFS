
package com.bonree.brfs.schedulers;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.task.TaskType;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.common.zookeeper.curator.leader.CuratorLeaderSelectorClient;
import com.bonree.brfs.configuration.Configuration;
import com.bonree.brfs.configuration.ResourceTaskConfig;
import com.bonree.brfs.configuration.ServerConfig;
import com.bonree.brfs.duplication.storagename.StorageNameManager;
import com.bonree.brfs.resourceschedule.commons.GatherResource;
import com.bonree.brfs.resourceschedule.model.BaseMetaServerModel;
import com.bonree.brfs.resourceschedule.model.ResourceModel;
import com.bonree.brfs.resourceschedule.model.ServerModel;
import com.bonree.brfs.resourceschedule.model.StatServerModel;
import com.bonree.brfs.resourceschedule.model.StateMetaServerModel;
import com.bonree.brfs.resourceschedule.service.AvailableServerInterface;
import com.bonree.brfs.resourceschedule.service.impl.RandomAvailable;
import com.bonree.brfs.resourceschedule.utils.LibUtils;
import com.bonree.brfs.schedulers.exception.ParamsErrorException;
import com.bonree.brfs.schedulers.jobs.JobDataMapConstract;
import com.bonree.brfs.schedulers.jobs.resource.AsynJob;
import com.bonree.brfs.schedulers.jobs.resource.GatherResourceJob;
import com.bonree.brfs.schedulers.task.manager.SchedulerManagerInterface;
import com.bonree.brfs.schedulers.task.manager.impl.DefaultSchedulersManager;
import com.bonree.brfs.schedulers.task.meta.SumbitTaskInterface;
import com.bonree.brfs.schedulers.task.meta.impl.QuartzSimpleInfo;

public class InitTaskManager {
	private static final Logger LOG = LoggerFactory.getLogger("InitTaskManager");
	public static final String RESOURCE_MANAGER = "RESOURCE_MANAGER";
	public static final String META_TASK_MANAGER = "META_TASK_MANAGER";
	public static final String TASK_OPERATION_MANAGER = "TASK_OPERATION_MANAGER";
	// 任务服务初始化
	public static class TaskLeader extends LeaderSelectorListenerAdapter {

		private SchedulerManagerInterface manager;
		private ZookeeperPaths zkPaths;
		private ResourceTaskConfig config;

		public TaskLeader(SchedulerManagerInterface manager, ZookeeperPaths zkPaths,ResourceTaskConfig config) {
			this.manager = manager;
			this.zkPaths = zkPaths;
			this.config = config;
		}

		// 身为leader时，会执行该函数的代码，执行完毕后，会放弃leader。并会参与下次竞选
		@Override
		public void takeLeadership(CuratorFramework client) throws Exception {
			// 若接口为空则返回空
			if (manager == null) {
				return;
			}
			Properties prop = InitTaskManager.createSimplePrope(2, 1000l);
			boolean createFlag = this.manager.createTaskPool(META_TASK_MANAGER, prop);
			// 若创建不成功则返回
			if(!createFlag){
				LOG.error("create task manager server fail !!!!");
				return ;
			}
			LOG.info("get leader success and create task manager server success !!!");
			// 提交任务线程
			Thread.sleep(Long.MAX_VALUE);
			manager.destoryTaskPool(META_TASK_MANAGER, false);
			LOG.info("loss the leader !!!");
		}
		
		private void sumbitTask(){
			
		}

	};

	/**
	 * 概述：初始化任务服务系统
	 * @param taskConf
	 * @param zkConf
	 * @param serverConf
	 * @throws ParamsErrorException 
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static void initManager(Configuration configuration, String homePath) throws Exception {
		ResourceTaskConfig managerConfig = ResourceTaskConfig.parse(configuration);
		ServerConfig serverConfig = ServerConfig.parse(configuration, homePath);
		ZookeeperPaths zkPath = ZookeeperPaths.create(serverConfig.getClusterName(),serverConfig.getZkHosts());
		
		ManagerContralFactory mcf = ManagerContralFactory.getInstance();
		
		SchedulerManagerInterface manager = DefaultSchedulersManager.getInstance();
		mcf.setStm(manager);
		
		AvailableServerInterface as = RandomAvailable.getInstance();
		mcf.setAsm(as);
		
		Map<String, Boolean> switchMap = managerConfig.getTaskPoolSwitchMap();
		Map<String, Integer> sizeMap = managerConfig.getTaskPoolSizeMap();
		Properties prop = null;
		String poolName = null;
		
		// 创建任务线程池
		if (managerConfig.isTaskFrameWorkSwitch()) {
			// 1.创建任务管理服务
//			createMetaTaskManager(manager, zkPath, managerConfig, serverConfig);
			// 2.启动任务线程池
//			createAndStartThreadPool(manager, switchMap, sizeMap);
			// 3.创建执行任务线程池
		}

		// 创建资源调度服务
		createResourceManager(manager, zkPath, managerConfig, serverConfig);
		
	}
	/**
	 * 概述：创建管理工厂
	 * @param sm
	 * @param snm
	 * @param serverId
	 * @param groupname
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static void createManagerContralFactory(ServiceManager sm, StorageNameManager snm,String serverId, String groupname){
		ManagerContralFactory mcf = ManagerContralFactory.getInstance();
		mcf.setSm(sm);
		mcf.setSnm(snm);
		mcf.setServerId(serverId);
		mcf.setGroupName(groupname);
	}
	/**
	 * 概述：创建资源管理
	 * @param manager
	 * @param zkPaths
	 * @param config
	 * @param serverConfig
	 * @throws Exception 
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	private static void createResourceManager(SchedulerManagerInterface manager, ZookeeperPaths zkPaths,ResourceTaskConfig config, ServerConfig serverConfig) throws Exception{
		// 1.引入第三方lib库，资源采集时需要用到
		LibUtils.loadLibraryPath(config.getLibPath());
		// 2.采集基本信息上传到 zk
		ServiceManager sm = ManagerContralFactory.getInstance().getSm();
		String serverId = ManagerContralFactory.getInstance().getServerId();
		BaseMetaServerModel base = GatherResource.gatherBase(serverId, serverConfig.getDataPath());
		ServerModel smodel = new ServerModel();
		smodel.setBase(base);
		String str = JsonUtils.toJsonString(smodel);
		LOG.info("base {}", str);
		sm.updateService(serverConfig.getClusterName(), serverId, str);
		
		//test code
		
//		Queue<StateMetaServerModel> queue = new ConcurrentLinkedQueue<StateMetaServerModel>();
//		StateMetaServerModel stat1 = GatherResource.gatherResource("E:/", "192.168.3.162");
//		queue.add(stat1);
//		Thread.sleep(1000);
//		stat1 = GatherResource.gatherResource("E:/", "192.168.3.162");
//		queue.add(stat1);
//		List<StatServerModel> statList = GatherResource.calcState(queue);
//		StatServerModel sum = GatherResource.calcStatServerModel(statList, new ArrayList<String>(), 1000, "E:/");
//		ResourceModel resource = GatherResource.calcResourceValue(base, sum);
//		ManagerContralFactory mcf = ManagerContralFactory.getInstance();
//		Service s = sm.getServiceById(mcf.getGroupName(), mcf.getServerId());
//		LOG.info("");
//		String strcontet = s.getPayload();
//		if(BrStringUtils.isEmpty(strcontet)){
//			System.out.println("content "+ strcontet);
//			return;
//		}
//		ServerModel ss = JsonUtils.toObject(strcontet, ServerModel.class);
//		if(ss == null){
//			System.out.println("serverModel is null");
//			return;
//		}
//		ss.setResource(resource);
//		String result = JsonUtils.toJsonString(ss);
//		if(BrStringUtils.isEmpty(result)){
//			System.out.println("result is null");
//		}
//		sm.updateService(mcf.getGroupName(), mcf.getServerId(), result);
//		
//		s = sm.getServiceById(mcf.getGroupName(), mcf.getServerId());
//		ss = JsonUtils.toObject(s.getPayload(), ServerModel.class);
//		
		
		
		// 3.创建资源采集线程池
		Properties  prop = createSimplePrope(2, 1000);
		manager.createTaskPool(RESOURCE_MANAGER, prop);
		manager.startTaskPool(RESOURCE_MANAGER);
		// 4.创建采集任务信息
		Map<String, String> gatherMap = JobDataMapConstract.createGatherResourceDataMap(serverConfig, config, serverId);
		SumbitTaskInterface gatherInterface = createResource(GatherResourceJob.class.getSimpleName(), config.getGatherResourceInveralTime(), gatherMap, GatherResourceJob.class);
		manager.addTask(RESOURCE_MANAGER, gatherInterface);
		// 2.创建同步信息
		Map<String,String> syncMap = JobDataMapConstract.createAsynResourceDataMap(serverConfig, config);
		SumbitTaskInterface syncInterface = createResource(AsynJob.class.getSimpleName(), config.getGatherResourceInveralTime(), syncMap, AsynJob.class);
		manager.addTask(RESOURCE_MANAGER, syncInterface);
	}
	/**
	 * 概述：创建任务执行线程池
	 * @param manager
	 * @param zkPaths
	 * @param config
	 * @param serverConfig
	 * @throws ParamsErrorException
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static void createTaskOperationManager(SchedulerManagerInterface manager, ZookeeperPaths zkPaths,ResourceTaskConfig config, ServerConfig serverConfig) throws ParamsErrorException{
		// 1.创建执行线程池
		Properties  prop = createSimplePrope(1, 1000);
		manager.createTaskPool(TASK_OPERATION_MANAGER, prop);
		manager.startTaskPool(TASK_OPERATION_MANAGER);
		
	}
	/**
	 * 概述：创建立即执行循环任务
	 * @param intervalTime 间隔时间
	 * @param jobMap 传入的数据
	 * @param clazz 对应的Class
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static QuartzSimpleInfo createResource(String name, long intervalTime, Map<String, String> jobMap, Class<?> clazz) {
		if(BrStringUtils.isEmpty(name)|| intervalTime <=0){
			return null;
		}
		QuartzSimpleInfo simple = new QuartzSimpleInfo();
		simple.setTaskName(name);
		simple.setTaskGroupName(name);
		simple.setClassInstanceName(clazz.getCanonicalName());
		simple.setCycleFlag(true);
		simple.setInterval(intervalTime);
		simple.setRunNowFlag(false);
		simple.setDelayTime(5000);
		if(jobMap != null && !jobMap.isEmpty()){
			simple.setTaskContent(jobMap);
		}
		return simple;
	}
	/**
	 * 概述：创建集群任务管理服务
	 * @param manager
	 * @param zkPaths
	 * @param config
	 * @param serverConfig
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	private static void createMetaTaskManager(SchedulerManagerInterface manager, ZookeeperPaths zkPaths,ResourceTaskConfig config, ServerConfig serverConfig){
		TaskLeader leader = new TaskLeader(manager, zkPaths, config);
		CuratorLeaderSelectorClient leaderSelector = CuratorLeaderSelectorClient.getLeaderSelectorInstance(serverConfig.getZkHosts());
	    leaderSelector.addSelector(zkPaths.getBaseLocksPath() + "/MetaTaskLeaderLock", leader);
	}
	/**
	 * 概述：根据switchMap 创建线程池
	 * @param manager
	 * @param switchMap
	 * @param sizeMap
	 * @throws ParamsErrorException 
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	private static void createAndStartThreadPool(SchedulerManagerInterface manager, Map<String, Boolean> switchMap, Map<String, Integer> sizeMap) throws ParamsErrorException{
		Properties prop = null;
		String poolName = null;
		int count = 0;
		int size = 0;
		for (TaskType taskType : TaskType.values()) {
			poolName = taskType.name();
			if (!switchMap.containsKey(poolName)) {
				continue;
			}
			if (!switchMap.get(poolName)) {
				continue;
			}
			size = sizeMap.get(poolName);
			if (size == 0) {
				//TODO:打印报警信息
				LOG.warn("pool :{} config pool size is 0 ,will change to 1", poolName);
				size = 1;
			}
			prop = createSimplePrope(size, 1000l);
			boolean createState = manager.createTaskPool(poolName, prop);
			if (createState) {
				//TODO:打印成功信息
				manager.startTaskPool(poolName);
			}
			count++;
		}
		LOG.info("pool :{} count: {} started !!!", manager.getAllPoolKey(), count);
	}

	private static Properties createSimplePrope(int poolSize, long misfireTime) {
		Properties prop = new Properties();
		prop.put(StdSchedulerFactory.PROP_THREAD_POOL_CLASS, "org.quartz.simpl.SimpleThreadPool");
		prop.put("org.quartz.threadPool.threadCount", poolSize + "");
		prop.put("quartz.jobStore.misfireThreshold", misfireTime + "");
		return prop;
	}
}
