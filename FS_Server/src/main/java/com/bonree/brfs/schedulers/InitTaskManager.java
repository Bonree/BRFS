
package com.bonree.brfs.schedulers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.retry.RetryNTimes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.task.TaskState;
import com.bonree.brfs.common.task.TaskType;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.ResourceTaskConfig;
import com.bonree.brfs.configuration.units.CommonConfigs;
import com.bonree.brfs.configuration.units.DiskNodeConfigs;
import com.bonree.brfs.duplication.storagename.StorageNameManager;
import com.bonree.brfs.resourceschedule.commons.GatherResource;
import com.bonree.brfs.resourceschedule.model.BaseMetaServerModel;
import com.bonree.brfs.resourceschedule.model.ServerModel;
import com.bonree.brfs.resourceschedule.service.AvailableServerInterface;
import com.bonree.brfs.resourceschedule.service.impl.RandomAvailable;
import com.bonree.brfs.resourceschedule.utils.LibUtils;
import com.bonree.brfs.schedulers.exception.ParamsErrorException;
import com.bonree.brfs.schedulers.jobs.JobDataMapConstract;
import com.bonree.brfs.schedulers.jobs.biz.CopyRecoveryJob;
import com.bonree.brfs.schedulers.jobs.biz.WatchDogJob;
import com.bonree.brfs.schedulers.jobs.biz.WatchSomeThingJob;
import com.bonree.brfs.schedulers.jobs.resource.AsynJob;
import com.bonree.brfs.schedulers.jobs.resource.GatherResourceJob;
import com.bonree.brfs.schedulers.jobs.system.OperationTaskJob;
import com.bonree.brfs.schedulers.task.manager.MetaTaskManagerInterface;
import com.bonree.brfs.schedulers.task.manager.RunnableTaskInterface;
import com.bonree.brfs.schedulers.task.manager.SchedulerManagerInterface;
import com.bonree.brfs.schedulers.task.manager.impl.DefaultBaseSchedulers;
import com.bonree.brfs.schedulers.task.manager.impl.DefaultReleaseTask;
import com.bonree.brfs.schedulers.task.manager.impl.DefaultRunnableTask;
import com.bonree.brfs.schedulers.task.manager.impl.DefaultSchedulersManager;
import com.bonree.brfs.schedulers.task.meta.SumbitTaskInterface;
import com.bonree.brfs.schedulers.task.meta.impl.QuartzCronInfo;
import com.bonree.brfs.schedulers.task.meta.impl.QuartzSimpleInfo;
import com.bonree.brfs.schedulers.task.model.TaskExecutablePattern;
import com.bonree.brfs.schedulers.task.model.TaskServerNodeModel;
import com.bonree.brfs.server.identification.ServerIDManager;

public class InitTaskManager {
	private static final Logger LOG = LoggerFactory.getLogger("InitTaskManager");
	public static final String RESOURCE_MANAGER = "RESOURCE_MANAGER";
	public static final String TASK_OPERATION_MANAGER = "TASK_OPERATION_MANAGER";
	
	private static LeaderLatch leaderLatch = null;
			
	/**
	 * 概述：初始化任务服务系统
	 * @param taskConf
	 * @param zkConf
	 * @param serverConf
	 * @throws ParamsErrorException 
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static void initManager(ResourceTaskConfig managerConfig,ZookeeperPaths zkPath, ServiceManager sm,StorageNameManager snm, ServerIDManager sim) throws Exception {
		managerConfig.printDetail();
		ManagerContralFactory mcf = ManagerContralFactory.getInstance();
		String serverId = sim.getFirstServerID();
		mcf.setServerId(serverId);
		mcf.setGroupName(Configs.getConfiguration().GetConfig(CommonConfigs.CONFIG_DISK_SERVICE_GROUP_NAME));
		
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
		String zkAddresses = Configs.getConfiguration().GetConfig(CommonConfigs.CONFIG_ZOOKEEPER_ADDRESSES);
		release.setPropreties(zkAddresses, zkPath.getBaseTaskPath(), zkPath.getBaseLocksPath());
		if(release == null) {
			LOG.error("Meta task is empty");
			System.exit(1);
		}
		mcf.setTm(release);
		// 工厂类添加任务可执行接口
		RunnableTaskInterface run = DefaultRunnableTask.getInstance();
		TaskExecutablePattern limit = TaskExecutablePattern.parse(managerConfig);
		run.setLimitParameter(limit);
		mcf.setRt(run);
		mcf.setZkPath(zkPath);
		mcf.setSim(sim);
		
		Map<String, Boolean> switchMap = managerConfig.getTaskPoolSwitchMap();
		Map<String, Integer> sizeMap = managerConfig.getTaskPoolSizeMap();
		Properties prop = null;
		String poolName = null;
		
		// 创建任务线程池
		if (managerConfig.isTaskFrameWorkSwitch()) {
			// 1.创建任务管理服务
			createMetaTaskManager(manager, zkPath, managerConfig, serverId);
			// 2.启动任务线程池
			List<TaskType> tasks = managerConfig.getSwitchOnTaskType();
			if(tasks == null || tasks.isEmpty()){
				LOG.warn("switch task on  but task type list is empty !!!");
			}
			createAndStartThreadPool(manager, managerConfig);
			if(tasks.contains(TaskType.SYSTEM_COPY_CHECK)){
				SumbitTaskInterface copyJob = createCopySimpleTask(managerConfig.getExecuteTaskIntervalTime(),
						TaskType.SYSTEM_COPY_CHECK.name(), serverId,
						CopyRecoveryJob.class.getCanonicalName(), zkAddresses,
						zkPath.getBaseRoutePath(),
						Configs.getConfiguration().GetConfig(DiskNodeConfigs.CONFIG_DATA_ROOT));
				manager.addTask(TaskType.SYSTEM_COPY_CHECK.name(), copyJob);
			}
			mcf.setTaskOn(tasks);
			//3.创建执行任务线程池
			createOperationPool(managerConfig,zkPath, tasks, true);
		}
		
		if(managerConfig.isResourceFrameWorkSwitch()){
			// 创建资源调度服务
			createResourceManager(manager, zkPath, managerConfig);
		}
	}
	/**
	 * 概述：创建集群任务管理服务
	 * @param manager
	 * @param zkPaths
	 * @param config
	 * @param serverConfig
	 * @throws Exception 
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static void createMetaTaskManager(SchedulerManagerInterface manager, ZookeeperPaths zkPaths,ResourceTaskConfig config,String serverId) throws Exception{
		MetaTaskLeaderManager leader = new MetaTaskLeaderManager(manager, config);
		RetryPolicy retryPolicy = new RetryNTimes(3, 1000);
		String zkAddresses = Configs.getConfiguration().GetConfig(CommonConfigs.CONFIG_ZOOKEEPER_ADDRESSES);
		CuratorFramework client = CuratorFrameworkFactory.newClient(zkAddresses, retryPolicy);
		client.start();
		leaderLatch = new LeaderLatch(client, zkPaths.getBaseLocksPath() + "/TaskManager/MetaTaskLeaderLock", serverId);
		leaderLatch.addListener(leader);
		leaderLatch.start();
	}
	/**
	 * 概述：创建任务执行线程池
	 * @param confg
	 * @param switchList
	 * @param isReboot
	 * @throws Exception
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	private static void createOperationPool(ResourceTaskConfig confg, ZookeeperPaths zkPath, List<TaskType> switchList, boolean isReboot) throws Exception{
		ManagerContralFactory mcf = ManagerContralFactory.getInstance();
		SchedulerManagerInterface manager = mcf.getStm();
		MetaTaskManagerInterface release = mcf.getTm();
		String serverId = mcf.getServerId();
		
		Properties prop = DefaultBaseSchedulers.createSimplePrope(3, 1000);
		boolean createFlag = manager.createTaskPool(TASK_OPERATION_MANAGER, prop);
		if(!createFlag){
			LOG.error("create task operation error !!!");
			throw new NullPointerException("create task operation error !!!");
		}
		boolean sFlag = manager.startTaskPool(TASK_OPERATION_MANAGER);
		if(!sFlag){
			LOG.error("create task operation error !!!");
			throw new NullPointerException("start task operation error !!!");
		}
		Map<String,String> dataMap = new HashMap<>();
		Map<String,String> switchMap = null;
		if(isReboot){
			// 将任务信息不完全的任务补充完整
			LOG.info("========================================================================================");
			switchMap = recoveryTask(switchList, release, serverId);
			LOG.info("========================================================================================");
		}
		dataMap = JobDataMapConstract.createRebootTaskOpertionDataMap(Configs.getConfiguration().GetConfig(DiskNodeConfigs.CONFIG_DATA_ROOT), switchMap);
		SumbitTaskInterface task = QuartzSimpleInfo.createCycleTaskInfo(TASK_OPERATION_MANAGER, confg.getExecuteTaskIntervalTime(), 60000, dataMap, OperationTaskJob.class);
		boolean sumbitFlag = manager.addTask(TASK_OPERATION_MANAGER, task);
		if(sumbitFlag){
			LOG.info("operation task sumbit complete !!!");
		}
		
		String zkAddresses = Configs.getConfiguration().GetConfig(CommonConfigs.CONFIG_ZOOKEEPER_ADDRESSES);
		Map<String,String>watchMap = JobDataMapConstract.createWatchJobMap(zkAddresses);
		SumbitTaskInterface watchJob = QuartzSimpleInfo.createCycleTaskInfo("WATCH_TASK", 5000, -1, watchMap, WatchSomeThingJob.class);
		sumbitFlag = manager.addTask(TASK_OPERATION_MANAGER, watchJob);
		if(sumbitFlag){
			LOG.info("watch task sumbit complete !!!");
		}
		Map<String,String> watchDogMap = JobDataMapConstract.createWatchDogDataMap(zkAddresses, zkPath.getBaseRoutePath(),
				Configs.getConfiguration().GetConfig(DiskNodeConfigs.CONFIG_DATA_ROOT));
		LOG.info("watch dog map {}",watchDogMap);
		if(watchDogMap == null|| watchDogMap.isEmpty()) {
			System.exit(1);
		}
		SumbitTaskInterface watchDogTask = QuartzCronInfo.getInstance("WATCH_DOG", "WATCH_DOG", confg.getWatchDogCron(), watchDogMap, WatchDogJob.class);
		sumbitFlag = manager.addTask(TASK_OPERATION_MANAGER, watchDogTask);
		if(sumbitFlag){
			LOG.info("watch dog task sumbit complete !!!");
		}
	}
	/**
	 * 概述：修复任务状态
	 * @param swtichList
	 * @param release
	 * @param serverId
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	private static Map<String,String> recoveryTask(List<TaskType> swtichList, MetaTaskManagerInterface release, String serverId){
		Map<String,String> swtichMap = new HashMap<>();
		if(swtichList == null || swtichList.isEmpty()){
			return swtichMap;
		}
		String typeName = null;
		String currentTask = null;
		for(TaskType taskType : swtichList){
			typeName = taskType.name();
			currentTask = release.getLastSuccessTaskIndex(typeName, serverId);
			if(BrStringUtils.isEmpty(currentTask)){
				currentTask = release.getFirstServerTask(typeName, serverId);
			}
			if(BrStringUtils.isEmpty(currentTask)){
				currentTask = release.getFirstTaskName(typeName);
			}
			if(BrStringUtils.isEmpty(currentTask)){
				LOG.info("{} task queue is empty", currentTask);
				continue;
			}
			// 修复任务
			recoveryTask(release, typeName, currentTask, serverId);
			if(BrStringUtils.isEmpty(currentTask)){
				continue;
			}
			swtichMap.put(typeName, currentTask);
		}
		return swtichMap;
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
		TaskServerNodeModel serverNode =  TaskServerNodeModel.getInitInstance();
		TaskServerNodeModel change = null;
		for(int i = index; i < size; i++ ){
			taskName = tasks.get(i);
			if(BrStringUtils.isEmpty(taskName)){
				continue;
			}
			cList = release.getTaskServerList(taskType, taskName);
			if(cList.contains(serverId)) {
				change = release.getTaskServerContentNodeInfo(taskType, taskName, serverId);
				if(change == null) {
					change = TaskServerNodeModel.getInitInstance();
				}
				if(change.getTaskState() == TaskState.RUN.code()||change.getTaskState() == TaskState.RERUN.code()) {
					change.setTaskState(TaskState.INIT.code());
					release.updateServerTaskContentNode(serverId, taskName, taskType, change);
				}
			}
			
			if(cList == null || cList.isEmpty() || !cList.contains(serverId)){
				release.updateServerTaskContentNode(serverId, taskName, taskType, serverNode);
				int stat = release.queryTaskState(taskName, taskType);
				if(TaskState.FINISH.code() == stat){
					release.changeTaskContentNodeState(taskName, taskType, TaskState.RERUN.code());
				}
				LOG.info("Recover {} task's {} serverId  {} ",taskType, taskName, serverId);
			}
		}
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
	private static void createResourceManager(SchedulerManagerInterface manager, ZookeeperPaths zkPaths,ResourceTaskConfig config) throws Exception{
		// 1.引入第三方lib库，资源采集时需要用到
		LibUtils.loadLibraryPath(config.getLibPath());
		// 2.采集基本信息上传到 zk
		ServiceManager sm = ManagerContralFactory.getInstance().getSm();
		String serverId = ManagerContralFactory.getInstance().getServerId();
		BaseMetaServerModel base = GatherResource.gatherBase(serverId, Configs.getConfiguration().GetConfig(DiskNodeConfigs.CONFIG_DATA_ROOT));
		ServerModel smodel = new ServerModel();
		smodel.setBase(base);
		String str = JsonUtils.toJsonString(smodel);
		sm.updateService(Configs.getConfiguration().GetConfig(CommonConfigs.CONFIG_DISK_SERVICE_GROUP_NAME), serverId, str);
		
		// 3.创建资源采集线程池
		Properties  prop = DefaultBaseSchedulers.createSimplePrope(2, 1000);
		manager.createTaskPool(RESOURCE_MANAGER, prop);
		boolean cFlag = manager.startTaskPool(RESOURCE_MANAGER);
		if(!cFlag){
			LOG.error("{} start fail !!!", RESOURCE_MANAGER);
		}
		// 4.创建采集任务信息
		Map<String, String> gatherMap = JobDataMapConstract.createGatherResourceDataMap(config, serverId);
		SumbitTaskInterface gatherInterface = QuartzSimpleInfo.createCycleTaskInfo(GatherResourceJob.class.getSimpleName(), config.getGatherResourceInveralTime(), 2000, gatherMap, GatherResourceJob.class);
		boolean taskFlag = manager.addTask(RESOURCE_MANAGER, gatherInterface);
		if(!taskFlag){
			LOG.error("sumbit gather job fail !!!");
		}
		// 2.创建同步信息
		Map<String,String> syncMap = JobDataMapConstract.createAsynResourceDataMap(config);
		SumbitTaskInterface syncInterface = QuartzSimpleInfo.createCycleTaskInfo(AsynJob.class.getSimpleName(), config.getGatherResourceInveralTime(), 2000, syncMap, AsynJob.class);
		taskFlag = manager.addTask(RESOURCE_MANAGER, syncInterface);
		if(!taskFlag){
			LOG.error("sumbit asyn job fail !!!");
		}
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
	public static void createTaskOperationManager(SchedulerManagerInterface manager, ZookeeperPaths zkPaths,ResourceTaskConfig config) throws ParamsErrorException{
		// 1.创建执行线程池
		Properties  prop = DefaultBaseSchedulers.createSimplePrope(1, 1000);
		manager.createTaskPool(TASK_OPERATION_MANAGER, prop);
		manager.startTaskPool(TASK_OPERATION_MANAGER);
		
	}
	
	/**
	 * 概述：根据switchMap 创建线程池
	 * @param manager
	 * @param switchMap
	 * @param sizeMap
	 * @throws ParamsErrorException 
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	private static void createAndStartThreadPool(SchedulerManagerInterface manager,ResourceTaskConfig config ) throws ParamsErrorException{
		Map<String, Boolean> switchMap = config.getTaskPoolSwitchMap();
		Map<String, Integer> sizeMap = config.getTaskPoolSizeMap();
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
	private  static SumbitTaskInterface createCopySimpleTask(long invertalTime, String taskName, String serverId,String clazzName, String zkHost, String path,String dataPath){
		QuartzSimpleInfo task = new QuartzSimpleInfo();
		task.setRunNowFlag(true);
		task.setCycleFlag(true);
		task.setTaskName(taskName);
		task.setTaskGroupName(TaskType.SYSTEM_COPY_CHECK.name());
		task.setRepeateCount(-1);
		task.setInterval(invertalTime);
		Map<String,String> dataMap = JobDataMapConstract.createCOPYDataMap(taskName, serverId, invertalTime, zkHost, path, dataPath);
		if(dataMap != null && !dataMap.isEmpty()){
			task.setTaskContent(dataMap);
		}
		
		task.setClassInstanceName(clazzName);
		return task;
	}
}
