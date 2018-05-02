
package com.bonree.brfs.schedulers;

import java.util.ArrayList;
import java.util.HashMap;
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
import com.bonree.brfs.schedulers.jobs.task.CreateSystemTaskJob;
import com.bonree.brfs.schedulers.jobs.task.ManagerMetaTaskJob;
import com.bonree.brfs.schedulers.jobs.task.OperationTaskJob;
import com.bonree.brfs.schedulers.task.manager.MetaTaskManagerInterface;
import com.bonree.brfs.schedulers.task.manager.RunnableTaskInterface;
import com.bonree.brfs.schedulers.task.manager.SchedulerManagerInterface;
import com.bonree.brfs.schedulers.task.manager.impl.DefaultReleaseTask;
import com.bonree.brfs.schedulers.task.manager.impl.DefaultRunnableTask;
import com.bonree.brfs.schedulers.task.manager.impl.DefaultSchedulersManager;
import com.bonree.brfs.schedulers.task.meta.SumbitTaskInterface;
import com.bonree.brfs.schedulers.task.meta.impl.QuartzSimpleInfo;
import com.bonree.brfs.schedulers.task.model.TaskExecutablePattern;

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

		public TaskLeader(SchedulerManagerInterface manager, ResourceTaskConfig config) {
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
			this.manager.startTaskPool(META_TASK_MANAGER);
			LOG.info("get leader success and create task manager server success !!!");
			sumbitTask();
			LOG.info("sumbit meta manager task success !!!!");
			// 提交任务线程
			Thread.sleep(Long.MAX_VALUE);
			manager.destoryTaskPool(META_TASK_MANAGER, false);
			LOG.info("loss the leader !!!");
		}
		
		private void sumbitTask() throws ParamsErrorException{
			Map<String,String> createDataMap = new HashMap<String,String>();
			SumbitTaskInterface createJob = createCycleTaskInfo("CREATE_SYSTEM_TASK", config.getCreateTaskIntervalTime(),-1, createDataMap, CreateSystemTaskJob.class);
			Map<String,String> metaDataMap = JobDataMapConstract.createMetaDataMap(config);
			SumbitTaskInterface metaJob = createCycleTaskInfo("META_MANAGER_TASK", config.getCreateTaskIntervalTime(), -1, metaDataMap, ManagerMetaTaskJob.class);
			this.manager.addTask(META_TASK_MANAGER, createJob);
			this.manager.addTask(META_TASK_MANAGER, metaJob);
			
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
	//TODO:临时参数serverId，groupName
	public static void initManager(Configuration configuration, ServiceManager sm, StorageNameManager snm, String homePath, String serverId,String groupName,boolean isReboot) throws Exception {
		ResourceTaskConfig managerConfig = ResourceTaskConfig.parse(configuration);
		ServerConfig serverConfig = ServerConfig.parse(configuration, homePath);
		ZookeeperPaths zkPath = ZookeeperPaths.create(serverConfig.getClusterName(),serverConfig.getZkHosts());
		
		ManagerContralFactory mcf = ManagerContralFactory.getInstance();
		//TODO:临时代码 工厂类添加serverId与groupName
		mcf.setServerId(serverId);
		mcf.setGroupName(groupName);
		
		// 工厂类添加服务管理
		mcf.setSm(sm);
		
		// 工厂类添加storageName管理服务
		mcf.setSnm(snm);
		
		// 1.工厂类添加调度管理
		SchedulerManagerInterface manager = DefaultSchedulersManager.getInstance();
		mcf.setStm(manager);
		
		// 2.工厂类添加可用服务
		AvailableServerInterface as = RandomAvailable.getInstance();
		mcf.setAsm(as);
		
		// 工厂类添加发布接口
		MetaTaskManagerInterface release = DefaultReleaseTask.getInstance();
		release.setPropreties(serverConfig.getZkHosts(), zkPath.getBaseTaskPath());
		mcf.setTm(release);
		// 工厂类添加任务可执行接口
		RunnableTaskInterface run = DefaultRunnableTask.getInstance();
		TaskExecutablePattern limit = createLimits(managerConfig);
		run.setLimitParameter(limit);
		mcf.setRt(run);
		
		Map<String, Boolean> switchMap = managerConfig.getTaskPoolSwitchMap();
		Map<String, Integer> sizeMap = managerConfig.getTaskPoolSizeMap();
		Properties prop = null;
		String poolName = null;
		
		// 创建任务线程池
		if (managerConfig.isTaskFrameWorkSwitch()) {
			// 1.创建任务管理服务
			createMetaTaskManager(manager, zkPath, managerConfig, serverConfig);
			// 2.启动任务线程池
			List<TaskType> tasks = createAndStartThreadPool(manager, switchMap, sizeMap);
			if(tasks == null || tasks.isEmpty()){
				throw new NullPointerException("switch task on  but task type list is empty !!!");
			}
			mcf.setTaskOn(tasks);
			// 3.创建执行任务线程池
			createOperationPool(managerConfig, tasks, isReboot);
		}
		
		if(managerConfig.isResourceFrameWorkSwitch()){
			// 创建资源调度服务
			createResourceManager(manager, zkPath, managerConfig, serverConfig);
		}
	}
	/**
	 * 概述：创建任务执行线程池
	 * @param confg
	 * @param switchList
	 * @param isReboot
	 * @throws Exception
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	private static void createOperationPool(ResourceTaskConfig confg, List<TaskType> switchList, boolean isReboot) throws Exception{
		ManagerContralFactory mcf = ManagerContralFactory.getInstance();
		SchedulerManagerInterface manager = mcf.getStm();
		MetaTaskManagerInterface release = mcf.getTm();
		String serverId = mcf.getServerId();
		
		Properties prop = createSimplePrope(1, 1000);
		boolean createFlag = manager.createTaskPool(TASK_OPERATION_MANAGER, prop);
		if(!createFlag){
			LOG.error("start task operation error !!!");
			throw new NullPointerException("start task operation error !!!");
		}
		manager.startTaskPool(TASK_OPERATION_MANAGER);
		Map<String,String> dataMap = new HashMap<>();
		if(isReboot){
			dataMap = JobDataMapConstract.createRebootTaskOpertionDataMap(switchList, release, serverId);
		}
		SumbitTaskInterface task = createCycleTaskInfo(TASK_OPERATION_MANAGER, confg.getExecuteTaskIntervalTime(), -1, dataMap, OperationTaskJob.class);
		boolean sumbitFlag = manager.addTask(TASK_OPERATION_MANAGER, task);
		if(sumbitFlag){
			LOG.info("operation task sumbit complete !!!");
		}
	}
	/***
	 * 概述：创建限制资源对象
	 * @param conf
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	private static TaskExecutablePattern createLimits(ResourceTaskConfig conf){
		TaskExecutablePattern limit = new TaskExecutablePattern();
		limit.setCpuRate(conf.getLimitCpuRate());
		limit.setMemoryRate(conf.getLimitMemoryRate());
		limit.setDiskRemainRate(conf.getLimitDiskRemaintRate());
		limit.setDiskReadRate(conf.getLimitDiskReadRate());
		limit.setDiskWriteRate(conf.getLimitDiskWriteRate());
		limit.setNetRxRate(conf.getLimitNetRxRate());
		limit.setNetTxRate(conf.getLimitNetTxRate());
		return limit;
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
		
		// 3.创建资源采集线程池
		Properties  prop = createSimplePrope(2, 1000);
		manager.createTaskPool(RESOURCE_MANAGER, prop);
		manager.startTaskPool(RESOURCE_MANAGER);
		// 4.创建采集任务信息
		Map<String, String> gatherMap = JobDataMapConstract.createGatherResourceDataMap(serverConfig, config, serverId);
		SumbitTaskInterface gatherInterface = createCycleTaskInfo(GatherResourceJob.class.getSimpleName(), config.getGatherResourceInveralTime(), 2000, gatherMap, GatherResourceJob.class);
		manager.addTask(RESOURCE_MANAGER, gatherInterface);
		// 2.创建同步信息
		Map<String,String> syncMap = JobDataMapConstract.createAsynResourceDataMap(serverConfig, config);
		SumbitTaskInterface syncInterface = createCycleTaskInfo(AsynJob.class.getSimpleName(), config.getGatherResourceInveralTime(), 2000, syncMap, AsynJob.class);
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
	public static QuartzSimpleInfo createCycleTaskInfo(String name, long intervalTime,long delayTime, Map<String, String> jobMap, Class<?> clazz) {
		if(BrStringUtils.isEmpty(name)|| intervalTime <=0){
			return null;
		}
		QuartzSimpleInfo simple = new QuartzSimpleInfo();
		simple.setTaskName(name);
		simple.setTaskGroupName(name);
		simple.setClassInstanceName(clazz.getCanonicalName());
		simple.setCycleFlag(true);
		simple.setInterval(intervalTime);
		if(delayTime <0){
			simple.setRunNowFlag(true);
		}else{
			simple.setRunNowFlag(false);
			simple.setDelayTime(delayTime);
		}
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
		TaskLeader leader = new TaskLeader(manager, config);
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
	private static List<TaskType> createAndStartThreadPool(SchedulerManagerInterface manager, Map<String, Boolean> switchMap, Map<String, Integer> sizeMap) throws ParamsErrorException{
		Properties prop = null;
		String poolName = null;
		int count = 0;
		int size = 0;
		List<TaskType> tasks = new ArrayList<TaskType>();
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
			tasks.add(taskType);
			count++;
		}
		LOG.info("pool :{} count: {} started !!!", manager.getAllPoolKey(), count);
		return tasks;
	}

	private static Properties createSimplePrope(int poolSize, long misfireTime) {
		Properties prop = new Properties();
		prop.put(StdSchedulerFactory.PROP_THREAD_POOL_CLASS, "org.quartz.simpl.SimpleThreadPool");
		prop.put("org.quartz.threadPool.threadCount", poolSize + "");
		prop.put("quartz.jobStore.misfireThreshold", misfireTime + "");
		return prop;
	}
}
