
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
import com.bonree.brfs.configuration.ResourceTaskConfig;
import com.bonree.brfs.configuration.ServerConfig;
import com.bonree.brfs.duplication.storagename.StorageNameManager;
import com.bonree.brfs.resourceschedule.commons.GatherResource;
import com.bonree.brfs.resourceschedule.model.BaseMetaServerModel;
import com.bonree.brfs.resourceschedule.model.ServerModel;
import com.bonree.brfs.resourceschedule.service.AvailableServerInterface;
import com.bonree.brfs.resourceschedule.service.impl.RandomAvailable;
import com.bonree.brfs.resourceschedule.utils.LibUtils;
import com.bonree.brfs.schedulers.exception.ParamsErrorException;
import com.bonree.brfs.schedulers.jobs.JobDataMapConstract;
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
	//TODO:临时参数groupName
	public static void initManager(ServerConfig serverConfig,ResourceTaskConfig managerConfig,ZookeeperPaths zkPath, ServiceManager sm,StorageNameManager snm, ServerIDManager sim) throws Exception {
		
		LOG.info("ttttttttttt : {}",sm.getServiceListByGroup(ServerConfig.DEFAULT_DISK_NODE_SERVICE_GROUP));
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
			createMetaTaskManager(manager, zkPath, managerConfig, serverConfig, release);
			// 2.启动任务线程池
			List<TaskType> tasks = createAndStartThreadPool(manager, switchMap, sizeMap);
			if(tasks == null || tasks.isEmpty()){
				throw new NullPointerException("switch task on  but task type list is empty !!!");
			}
			mcf.setTaskOn(tasks);
			//3.创建执行任务线程池
			createOperationPool(serverConfig, managerConfig, tasks, isReboot);
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
	private static void createOperationPool(ServerConfig server, ResourceTaskConfig confg, List<TaskType> switchList, boolean isReboot) throws Exception{
		ManagerContralFactory mcf = ManagerContralFactory.getInstance();
		SchedulerManagerInterface manager = mcf.getStm();
		MetaTaskManagerInterface release = mcf.getTm();
		String serverId = mcf.getServerId();
		
		Properties prop = DefaultBaseSchedulers.createSimplePrope(1, 1000);
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
		dataMap = JobDataMapConstract.createRebootTaskOpertionDataMap(server.getDataPath(), switchMap);
		SumbitTaskInterface task = QuartzSimpleInfo.createCycleTaskInfo(TASK_OPERATION_MANAGER, confg.getExecuteTaskIntervalTime(), -1, dataMap, OperationTaskJob.class);
		boolean sumbitFlag = manager.addTask(TASK_OPERATION_MANAGER, task);
		if(sumbitFlag){
			LOG.info("operation task sumbit complete !!!");
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
			if(!BrStringUtils.isEmpty(currentTask)){
			}else{
				currentTask = release.getFirstServerTask(typeName, serverId);
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
		sm.updateService(ServerConfig.DEFAULT_DISK_NODE_SERVICE_GROUP, serverId, str);
		
		// 3.创建资源采集线程池
		Properties  prop = DefaultBaseSchedulers.createSimplePrope(2, 1000);
		manager.createTaskPool(RESOURCE_MANAGER, prop);
		boolean cFlag = manager.startTaskPool(RESOURCE_MANAGER);
		if(!cFlag){
			LOG.error("{} start fail !!!", RESOURCE_MANAGER);
		}
		// 4.创建采集任务信息
		Map<String, String> gatherMap = JobDataMapConstract.createGatherResourceDataMap(serverConfig, config, serverId);
		SumbitTaskInterface gatherInterface = QuartzSimpleInfo.createCycleTaskInfo(GatherResourceJob.class.getSimpleName(), config.getGatherResourceInveralTime(), 2000, gatherMap, GatherResourceJob.class);
		boolean taskFlag = manager.addTask(RESOURCE_MANAGER, gatherInterface);
		if(!taskFlag){
			LOG.error("sumbit gather job fail !!!");
		}
		// 2.创建同步信息
		Map<String,String> syncMap = JobDataMapConstract.createAsynResourceDataMap(serverConfig, config);
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
	public static void createTaskOperationManager(SchedulerManagerInterface manager, ZookeeperPaths zkPaths,ResourceTaskConfig config, ServerConfig serverConfig) throws ParamsErrorException{
		// 1.创建执行线程池
		Properties  prop = DefaultBaseSchedulers.createSimplePrope(1, 1000);
		manager.createTaskPool(TASK_OPERATION_MANAGER, prop);
		manager.startTaskPool(TASK_OPERATION_MANAGER);
		
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
	private static void createMetaTaskManager(SchedulerManagerInterface manager, ZookeeperPaths zkPaths,ResourceTaskConfig config, ServerConfig serverConfig,MetaTaskManagerInterface release) throws Exception{
		MetaTaskLeaderManager leader = new MetaTaskLeaderManager(manager, config,serverConfig);
		RetryPolicy retryPolicy = new RetryNTimes(3, 1000);
		CuratorFramework client = CuratorFrameworkFactory.newClient(serverConfig.getZkHosts(), retryPolicy);
		client.start();
		leaderLatch = new LeaderLatch(client, zkPaths.getBaseLocksPath() + "/MetaTaskLeaderLock");
		leaderLatch.addListener(leader);
		leaderLatch.start();
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
			prop = DefaultBaseSchedulers.createSimplePrope(size, 1000l);
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
}
