
package com.bonree.brfs.schedulers;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.inject.Inject;

import com.bonree.brfs.configuration.units.ResourceConfigs;
import com.bonree.brfs.identification.IDSManager;
import com.bonree.brfs.resourceschedule.model.LimitServerResource;
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
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.ResourceTaskConfig;
import com.bonree.brfs.configuration.units.CommonConfigs;
import com.bonree.brfs.configuration.units.DataNodeConfigs;
import com.bonree.brfs.duplication.storageregion.StorageRegionManager;
import com.bonree.brfs.resourceschedule.utils.LibUtils;
import com.bonree.brfs.schedulers.exception.ParamsErrorException;
import com.bonree.brfs.schedulers.jobs.biz.CopyRecoveryJob;
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
import com.bonree.brfs.schedulers.utils.JobDataMapConstract;

public class InitTaskManager {
	private static final Logger LOG = LoggerFactory.getLogger("InitTaskManager");
	public static final String RESOURCE_MANAGER = "RESOURCE_MANAGER";
	public static final String TASK_OPERATION_MANAGER = "TASK_OPERATION_MANAGER";

	private static LeaderLatch leaderLatch = null;

	/**
	 * 概述：初始化任务服务系统
	 * @throws ParamsErrorException
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	@Inject
	public static void initManager(
	        ResourceTaskConfig managerConfig,
	        ZookeeperPaths zkPath,
	        ServiceManager sm,
	        StorageRegionManager snm,
	        IDSManager sim,
	        CuratorFramework client) throws Exception {
        managerConfig.printDetail();
        ManagerContralFactory mcf = ManagerContralFactory.getInstance();
        String serverId = sim.getFirstSever();
        mcf.setServerId(serverId);
        mcf.setGroupName(Configs.getConfiguration().GetConfig(CommonConfigs.CONFIG_DATA_SERVICE_GROUP_NAME));


        LimitServerResource lmit = new LimitServerResource();

        mcf.setLimitServerResource(lmit);

        // 工厂类添加服务管理
        mcf.setSm(sm);

        // 工厂类添加storageName管理服务
        mcf.setSnm(snm);

        // 1.工厂类添加调度管理
        SchedulerManagerInterface manager = DefaultSchedulersManager.getInstance();
        mcf.setStm(manager);

        // 工厂类添加发布接口
        MetaTaskManagerInterface release = DefaultReleaseTask.getInstance();
        if (release == null) {
            LOG.error("Meta task is empty");
            System.exit(1);
        }
        String zkAddresses = Configs.getConfiguration().GetConfig(CommonConfigs.CONFIG_ZOOKEEPER_ADDRESSES);
        release.setPropreties(zkAddresses, zkPath.getBaseTaskPath(), zkPath.getBaseLocksPath());
        if (client == null) {
            LOG.error("zk client is empty");
            System.exit(1);
        }
        mcf.setClient(CuratorClient.wrapClient(client));
        mcf.setTm(release);
        // 工厂类添加任务可执行接口
        RunnableTaskInterface run = DefaultRunnableTask.getInstance();
        TaskExecutablePattern limit = TaskExecutablePattern.parse(managerConfig);
        run.setLimitParameter(limit);
        mcf.setRt(run);
        mcf.setZkPath(zkPath);
        // todo 缺少赋值操作------------------------------------------------
//        mcf.setSim();

        // 创建任务线程池
        if (managerConfig.isTaskFrameWorkSwitch()) {
            // 1.创建任务管理服务
            createMetaTaskManager(manager, zkPath, managerConfig, serverId);
            // 2.启动任务线程池
            List<TaskType> tasks = managerConfig.getSwitchOnTaskType();
            if (tasks == null || tasks.isEmpty()) {
                LOG.warn("switch task on  but task type list is empty !!!");
                return;
            }
            createAndStartThreadPool(manager, managerConfig);
            if (tasks.contains(TaskType.SYSTEM_COPY_CHECK)) {
                SumbitTaskInterface copyJob = createCopySimpleTask(managerConfig.getExecuteTaskIntervalTime(),
                        TaskType.SYSTEM_COPY_CHECK.name(), serverId, CopyRecoveryJob.class.getCanonicalName(),
                        zkAddresses, zkPath.getBaseRoutePath(),
                        Configs.getConfiguration().GetConfig(DataNodeConfigs.CONFIG_DATA_ROOT));
                manager.addTask(TaskType.SYSTEM_COPY_CHECK.name(), copyJob);
            }
            mcf.setTaskOn(tasks);
            // 3.创建执行任务线程池
            createOperationPool(managerConfig, zkPath, tasks, true);
        }

        if (managerConfig.isResourceFrameWorkSwitch()) {
            // 创建资源调度服务
            createResourceManager(manager, zkPath, managerConfig);
        }
    }
	/**
	 * 概述：创建集群任务管理服务
	 * @param manager
	 * @param zkPaths
	 * @param config
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
		Map<String,String> dataMap;
		Map<String,String> switchMap = null;
		if(isReboot){
			// 将任务信息不完全的任务补充完整
			LOG.info("========================================================================================");
			switchMap = recoveryTask(switchList, release, serverId);
			LOG.info("========================================================================================");
		}
		dataMap = JobDataMapConstract.createRebootTaskOpertionDataMap(Configs.getConfiguration().GetConfig(DataNodeConfigs.CONFIG_DATA_ROOT), switchMap);
		SumbitTaskInterface task = QuartzSimpleInfo.createCycleTaskInfo(TASK_OPERATION_MANAGER, confg.getExecuteTaskIntervalTime(), 60000, dataMap, OperationTaskJob.class);
		boolean sumbitFlag = manager.addTask(TASK_OPERATION_MANAGER, task);
		if(sumbitFlag){
			LOG.info("operation task sumbit complete !!!");
		}

		String zkAddresses = Configs.getConfiguration().GetConfig(CommonConfigs.CONFIG_ZOOKEEPER_ADDRESSES);
		Map<String,String>watchMap = JobDataMapConstract.createWatchJobMap(zkAddresses);
		Map<String,String> watchDogMap = JobDataMapConstract.createWatchDogDataMap(zkAddresses, zkPath.getBaseRoutePath(),
				Configs.getConfiguration().GetConfig(DataNodeConfigs.CONFIG_DATA_ROOT));
		LOG.info("watch dog map {}",watchDogMap);
		if(watchDogMap == null|| watchDogMap.isEmpty()) {
			System.exit(1);
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
		String typeName;
		String currentTask;
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
		String taskName;
		List<String> cList;
		TaskServerNodeModel serverNode =  TaskServerNodeModel.getInitInstance();
		TaskServerNodeModel change;
		for(int i = index; i < size; i++ ){
			taskName = tasks.get(i);
			if(BrStringUtils.isEmpty(taskName)){
				continue;
			}
			cList = release.getTaskServerList(taskType, taskName);
			if(cList == null || cList.isEmpty() || !cList.contains(serverId)){
				release.updateServerTaskContentNode(serverId, taskName, taskType, serverNode);
				int stat = release.queryTaskState(taskName, taskType);
				if(TaskState.FINISH.code() == stat){
					release.changeTaskContentNodeState(taskName, taskType, TaskState.RERUN.code());
				}
				LOG.info("Recover {} task's {} serverId  {} ",taskType, taskName, serverId);
				continue;
			}
			change = release.getTaskServerContentNodeInfo(taskType, taskName, serverId);
			if(change == null) {
				change = TaskServerNodeModel.getInitInstance();
			}
			if(change.getTaskState() == TaskState.RUN.code()||change.getTaskState() == TaskState.RERUN.code()) {
				change.setTaskState(TaskState.INIT.code());
				release.updateServerTaskContentNode(serverId, taskName, taskType, change);
			}
		}
	}
	/**
	 * 概述：创建资源管理
	 * @param manager
	 * @param zkPaths
	 * @param config
	 * @throws Exception
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	private static void createResourceManager(SchedulerManagerInterface manager, ZookeeperPaths zkPaths,ResourceTaskConfig config) throws Exception{
		// 1.引入第三方lib库，资源采集时需要用到
		LibUtils.loadLibraryPath(config.getLibPath());
		// 2.采集基本信息上传到 zk
		String serverId = ManagerContralFactory.getInstance().getServerId();
		String zkAddress = Configs.getConfiguration().GetConfig(CommonConfigs.CONFIG_ZOOKEEPER_ADDRESSES);
		// 3.创建资源采集线程池
		Properties  prop = DefaultBaseSchedulers.createSimplePrope(2, 1000);
		manager.createTaskPool(RESOURCE_MANAGER, prop);
		boolean cFlag = manager.startTaskPool(RESOURCE_MANAGER);
		if(!cFlag){
			LOG.error("{} start fail !!!", RESOURCE_MANAGER);
		}
		// 4.创建采集任务信息
		Map<String, String> gatherMap = JobDataMapConstract.createGatherResourceDataMap(config, serverId,zkPaths.getBaseResourcesPath(),zkAddress);
		SumbitTaskInterface gatherInterface = QuartzSimpleInfo.createCycleTaskInfo(GatherResourceJob.class.getSimpleName(), config.getGatherResourceInveralTime(), 2000, gatherMap, GatherResourceJob.class);
		boolean taskFlag = manager.addTask(RESOURCE_MANAGER, gatherInterface);
		if(!taskFlag){
			LOG.error("sumbit gather job fail !!!");
		}
		LOG.info("GATHER successful !!!");
	}


	/**
	 * 概述：根据switchMap 创建线程池
	 * @param manager
	 * @throws ParamsErrorException
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	private static void createAndStartThreadPool(SchedulerManagerInterface manager,ResourceTaskConfig config ) throws ParamsErrorException{
		Map<String, Boolean> switchMap = config.getTaskPoolSwitchMap();
		Map<String, Integer> sizeMap = config.getTaskPoolSizeMap();
		Properties prop;
		String poolName;
		int count = 0;
		int size;
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
			count++;
		}
		LOG.info("pool :{} count: {} started !!!", manager.getAllPoolKey(), count);

	}
	/**
	 * 概述：生成任务信息
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
