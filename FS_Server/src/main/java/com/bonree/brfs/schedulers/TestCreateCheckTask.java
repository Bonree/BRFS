
package com.bonree.brfs.schedulers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.service.impl.DefaultServiceManager;
import com.bonree.brfs.common.task.TaskState;
import com.bonree.brfs.common.task.TaskType;
import com.bonree.brfs.common.utils.Attributes;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.common.zookeeper.curator.cache.CuratorCacheFactory;
import com.bonree.brfs.configuration.Configuration;
import com.bonree.brfs.configuration.Configuration.ConfigException;
import com.bonree.brfs.configuration.ResourceTaskConfig;
import com.bonree.brfs.configuration.ServerConfig;
import com.bonree.brfs.duplication.storagename.DefaultStorageNameManager;
import com.bonree.brfs.duplication.storagename.StorageNameManager;
import com.bonree.brfs.duplication.storagename.StorageNameNode;
import com.bonree.brfs.duplication.storagename.StorageNameStateListener;
import com.bonree.brfs.duplication.storagename.ZkStorageIdBuilder;
import com.bonree.brfs.resourceschedule.service.AvailableServerInterface;
import com.bonree.brfs.resourceschedule.service.impl.RandomAvailable;
import com.bonree.brfs.schedulers.exception.ParamsErrorException;
import com.bonree.brfs.schedulers.jobs.JobDataMapConstract;
import com.bonree.brfs.schedulers.jobs.biz.FileRecoveryJob;
import com.bonree.brfs.schedulers.jobs.biz.WatchSomeThingJob;
import com.bonree.brfs.schedulers.jobs.system.OperationTaskJob;
import com.bonree.brfs.schedulers.task.manager.MetaTaskManagerInterface;
import com.bonree.brfs.schedulers.task.manager.RunnableTaskInterface;
import com.bonree.brfs.schedulers.task.manager.SchedulerManagerInterface;
import com.bonree.brfs.schedulers.task.manager.impl.DefaultBaseSchedulers;
import com.bonree.brfs.schedulers.task.manager.impl.DefaultReleaseTask;
import com.bonree.brfs.schedulers.task.manager.impl.DefaultRunnableTask;
import com.bonree.brfs.schedulers.task.manager.impl.DefaultSchedulersManager;
import com.bonree.brfs.schedulers.task.meta.SumbitTaskInterface;
import com.bonree.brfs.schedulers.task.meta.impl.QuartzSimpleInfo;
import com.bonree.brfs.schedulers.task.model.TaskExecutablePattern;
import com.bonree.brfs.schedulers.task.model.TaskServerNodeModel;
import com.bonree.brfs.server.identification.ServerIDManager;

public class TestCreateCheckTask {
	private static final Logger LOG = LoggerFactory.getLogger("TestCreateCheckTask");
	public static final String RESOURCE_MANAGER = "RESOURCE_MANAGER";
	public static final String TASK_OPERATION_MANAGER = "TASK_OPERATION_MANAGER";

	public static void main(String[] args) throws Exception {
		String brfsHome = "E:/zhuchenggang/project/eclipse/Config";
		Configuration conf = Configuration.getInstance();
		conf.parse(brfsHome + "/config/server.properties");
		conf.printConfigDetail();
		ServerConfig serverConfig = ServerConfig.parse(conf, brfsHome);
		ResourceTaskConfig resourceConfig = ResourceTaskConfig.parse(conf);

		CuratorCacheFactory.init(serverConfig.getZkHosts());
		ZookeeperPaths zookeeperPaths = ZookeeperPaths.create(serverConfig.getClusterName(), serverConfig.getZkHosts());
		ServerIDManager idManager = new ServerIDManager(serverConfig, zookeeperPaths);
		idManager.getFirstServerID();

		CuratorClient leaderClient = CuratorClient.getClientInstance(serverConfig.getZkHosts(), 1000, 1000);
		CuratorClient client = CuratorClient.getClientInstance(serverConfig.getZkHosts());

		StorageNameManager snManager = new DefaultStorageNameManager(
			client.getInnerClient().usingNamespace(zookeeperPaths.getBaseClusterName().substring(1)), new ZkStorageIdBuilder(serverConfig.getZkHosts(), zookeeperPaths.getBaseSequencesPath()));
		snManager.addStorageNameStateListener(new StorageNameStateListener() {

			@Override
			public void storageNameAdded(StorageNameNode node) {
				idManager.getSecondServerID(node.getId());
			}

			@Override
			public void storageNameUpdated(StorageNameNode node) {
			}

			@Override
			public void storageNameRemoved(StorageNameNode node) {
			}
		});
		snManager.start();
		

		ServiceManager sm = new DefaultServiceManager(
			client.getInnerClient().usingNamespace(zookeeperPaths.getBaseClusterName().substring(1)));
		sm.start();

		Service selfService = new Service();
		selfService.setHost(serverConfig.getHost());
		selfService.setPort(serverConfig.getDiskPort());
		selfService.setServiceGroup(ServerConfig.DEFAULT_DISK_NODE_SERVICE_GROUP);
		selfService.setServiceId(idManager.getFirstServerID());
		sm.registerService(selfService);
		
		
		// 初始化基本参数
		initManager(serverConfig, resourceConfig, zookeeperPaths, sm, snManager, idManager);
		ManagerContralFactory mcf = ManagerContralFactory.getInstance();
		SchedulerManagerInterface manager = mcf.getStm();

//		Attributes prop = new Attributes();
//		prop.putInt(StorageNameNode.ATTR_REPLICATION, 1);
//		prop.putInt(StorageNameNode.ATTR_TTL, 1000);
//		snManager.createStorageName("zcgtodo", prop);
		String snName = "Test";
		Map<String,Object> config = new HashMap<>();
		config.put(StorageNameNode.ATTR_ENABLE, true);
		config.put(StorageNameNode.ATTR_REPLICATION, 2);
		config.put(StorageNameNode.ATTR_TTL, 20000);
		
		if(!snManager.exists(snName)){
			snManager.createStorageName(snName, new Attributes(config));
		}
		
		
		
//		MetaTaskLeaderManager leader = new MetaTaskLeaderManager(manager, resourceConfig, serverConfig);
//		leader.isLeader();
		Thread.sleep(Long.MAX_VALUE);
	}

	public static void initManager(ServerConfig serverConfig, ResourceTaskConfig managerConfig, ZookeeperPaths zkPath,
			ServiceManager sm, StorageNameManager snm, ServerIDManager sim) throws Exception {
		ManagerContralFactory mcf = ManagerContralFactory.getInstance();
		String serverId = sim.getFirstServerID();
		boolean isReboot = !sim.isNewService();
		//TODO:临时代码 工厂类添加serverId与groupName
		mcf.setServerId(serverId);
		mcf.setGroupName(ServerConfig.DEFAULT_DISK_NODE_SERVICE_GROUP);

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
		release.setPropreties(serverConfig.getZkHosts(), zkPath.getBaseTaskPath(), zkPath.getBaseLocksPath());
		mcf.setTm(release);
		// 工厂类添加任务可执行接口
		RunnableTaskInterface run = DefaultRunnableTask.getInstance();
		TaskExecutablePattern limit = TaskExecutablePattern.parse(managerConfig);
		run.setLimitParameter(limit);
		mcf.setRt(run);

		Map<String, Boolean> switchMap = managerConfig.getTaskPoolSwitchMap();
		Map<String, Integer> sizeMap = managerConfig.getTaskPoolSizeMap();
		Properties prop = null;
		String poolName = null;

		// 创建任务线程池
		if (managerConfig.isTaskFrameWorkSwitch()) {
			// 1.创建任务管理服务
//			createMetaTaskManager(manager, zkPath, managerConfig, serverConfig, release);
			// 2.启动任务线程池
			List<TaskType> tasks = createAndStartThreadPool(manager, switchMap, sizeMap);
			if (tasks == null || tasks.isEmpty()) {
				throw new NullPointerException("switch task on  but task type list is empty !!!");
			}
			if (tasks.contains(TaskType.SYSTEM_COPY_CHECK)) {
				SumbitTaskInterface copyJob = createSimpleTask(1000, TaskType.SYSTEM_COPY_CHECK.name(), serverId,
					FileRecoveryJob.class.getCanonicalName(), serverConfig.getZkHosts(), zkPath.getBaseRoutePath());
				manager.addTask(TaskType.SYSTEM_COPY_CHECK.name(), copyJob);
			}
			mcf.setTaskOn(tasks);
			//3.创建执行任务线程池
			createOperationPool(serverConfig, managerConfig, tasks, isReboot);
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
	private static void createOperationPool(ServerConfig server, ResourceTaskConfig confg, List<TaskType> switchList,
			boolean isReboot) throws Exception {
		ManagerContralFactory mcf = ManagerContralFactory.getInstance();
		SchedulerManagerInterface manager = mcf.getStm();
		MetaTaskManagerInterface release = mcf.getTm();
		String serverId = mcf.getServerId();

		Properties prop = DefaultBaseSchedulers.createSimplePrope(3, 1000);
		boolean createFlag = manager.createTaskPool(TASK_OPERATION_MANAGER, prop);
		if (!createFlag) {
			LOG.error("create task operation error !!!");
			throw new NullPointerException("create task operation error !!!");
		}
		boolean sFlag = manager.startTaskPool(TASK_OPERATION_MANAGER);
		if (!sFlag) {
			LOG.error("create task operation error !!!");
			throw new NullPointerException("start task operation error !!!");
		}
		Map<String, String> dataMap = new HashMap<>();
		Map<String, String> switchMap = null;
		if (isReboot) {
			// 将任务信息不完全的任务补充完整
			LOG.info("========================================================================================");
//			switchMap = recoveryTask(switchList, release, serverId);
			LOG.info("========================================================================================");
		}
		boolean sumbitFlag = false;
//		dataMap = JobDataMapConstract.createRebootTaskOpertionDataMap(server.getDataPath(), switchMap);
//		SumbitTaskInterface task = QuartzSimpleInfo.createCycleTaskInfo(TASK_OPERATION_MANAGER,
//			confg.getExecuteTaskIntervalTime(), -1, dataMap, OperationTaskJob.class);
//		sumbitFlag = manager.addTask(TASK_OPERATION_MANAGER, task);
//		if (sumbitFlag) {
//			LOG.info("operation task sumbit complete !!!");
//		}
		Map<String, String> watchMap = JobDataMapConstract.createWatchJobMap(server.getZkHosts());
		SumbitTaskInterface watchJob = QuartzSimpleInfo.createCycleTaskInfo("WATCH_TASK", 5000, -1, watchMap,
			WatchSomeThingJob.class);
		sumbitFlag = manager.addTask(TASK_OPERATION_MANAGER, watchJob);
		if (sumbitFlag) {
			LOG.info("watch task sumbit complete !!!");
		}
	}

	/**
	 * 概述：根据switchMap 创建线程池
	 * @param manager
	 * @param switchMap
	 * @param sizeMap
	 * @throws ParamsErrorException 
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	private static List<TaskType> createAndStartThreadPool(SchedulerManagerInterface manager,
			Map<String, Boolean> switchMap, Map<String, Integer> sizeMap) throws ParamsErrorException {
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
				LOG.warn("pool :{} config pool size is 0 ,will change to 1", poolName);
				size = 1;
			}
			prop = DefaultBaseSchedulers.createSimplePrope(size, 1000l);
			boolean createState = manager.createTaskPool(poolName, prop);
			if (createState) {
				manager.startTaskPool(poolName);
			}
			tasks.add(taskType);
			count++;
		}
		LOG.info("pool :{} count: {} started !!!", manager.getAllPoolKey(), count);

		return tasks;
	}
	
	/**
	 * 概述：将因服务挂掉而错失的任务重建
	 * @param release
	 * @param taskType
	 * @param currentTask
	 * @param serverId
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	private static void recoveryTask(MetaTaskManagerInterface release, String taskType, String currentTask, String serverId){
		List<String> tasks = release.getTaskList(taskType);
		if(tasks == null || tasks.isEmpty()){
			return;
		}
		int index = tasks.indexOf(currentTask);
		if(index < 0 ){
			index = 0;
		}
		int size = tasks.size();
		String taskName = null;
		List<String> cList = null;
		for(int i = index; i < size; i++ ){
			taskName = tasks.get(i);
			if(BrStringUtils.isEmpty(taskName)){
				continue;
			}
			cList = release.getTaskServerList(taskType, taskName);
			if(cList == null || cList.isEmpty() || !cList.contains(serverId)){
				release.updateServerTaskContentNode(serverId, taskName, taskType, new TaskServerNodeModel());
				int stat = release.queryTaskState(taskName, taskType);
				if(TaskState.FINISH.code() == stat){
					release.changeTaskContentNodeState(taskName, taskType, TaskState.RERUN.code());
				}
				LOG.info("Recover {} task's {} serverId  {} ",taskType, taskName, serverId);
			}
		}
	}
	
	/**
	 * 概述：生成任务信息
	 * @param taskModel
	 * @param runPattern
	 * @param taskName
	 * @param serverId
	 * @param clazzName
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	private  static SumbitTaskInterface createSimpleTask(long invertalTime, String taskName, String serverId,String clazzName, String zkHost, String path){
		QuartzSimpleInfo task = new QuartzSimpleInfo();
		task.setRunNowFlag(true);
		task.setCycleFlag(true);
		task.setTaskName(taskName);
		task.setTaskGroupName(TaskType.SYSTEM_COPY_CHECK.name());
		task.setRepeateCount(-1);
		task.setInterval(invertalTime);
		Map<String,String> dataMap = JobDataMapConstract.createCOPYDataMap(taskName, serverId, invertalTime, zkHost, path);
		if(dataMap != null && !dataMap.isEmpty()){
			task.setTaskContent(dataMap);
		}
		
		task.setClassInstanceName(clazzName);
		return task;
	}
}
