
package com.bonree.brfs.schedulers;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent.Type;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.task.TaskType;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.zookeeper.curator.cache.AbstractTreeCacheListener;
import com.bonree.brfs.common.zookeeper.curator.cache.CuratorCacheFactory;
import com.bonree.brfs.common.zookeeper.curator.cache.CuratorTreeCache;
import com.bonree.brfs.configuration.ResourceTaskConfig;
import com.bonree.brfs.configuration.ServerConfig;
import com.bonree.brfs.schedulers.exception.ParamsErrorException;
import com.bonree.brfs.schedulers.jobs.JobDataMapConstract;
import com.bonree.brfs.schedulers.jobs.system.CheckCycleJob;
import com.bonree.brfs.schedulers.jobs.system.CopyCheckJob;
import com.bonree.brfs.schedulers.jobs.system.CopyCheckJob;
import com.bonree.brfs.schedulers.jobs.system.CreateSystemTaskJob;
import com.bonree.brfs.schedulers.jobs.system.ManagerMetaTaskJob;
import com.bonree.brfs.schedulers.task.manager.MetaTaskManagerInterface;
import com.bonree.brfs.schedulers.task.manager.SchedulerManagerInterface;
import com.bonree.brfs.schedulers.task.manager.impl.DefaultBaseSchedulers;
import com.bonree.brfs.schedulers.task.meta.SumbitTaskInterface;
import com.bonree.brfs.schedulers.task.meta.impl.QuartzCronInfo;
import com.bonree.brfs.schedulers.task.meta.impl.QuartzSimpleInfo;
import com.bonree.brfs.schedulers.task.model.TaskTypeModel;

/*****************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年5月10日 下午5:16:45
 * @Author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @Description: 选取任务管理leader
 *****************************************************************************
 */
public class MetaTaskLeaderManager implements LeaderLatchListener {
	private static final Logger LOG = LoggerFactory.getLogger(MetaTaskLeaderManager.class);
	public static final String META_TASK_MANAGER = "META_TASK_MANAGER";
	public static final String COPY_CYCLE_POOL = "COPY_CYCLE_POOL";
	private SchedulerManagerInterface manager;
	private ZookeeperPaths zkPaths;
	private ResourceTaskConfig config;
	private ServerConfig serverConfig;

	public MetaTaskLeaderManager(SchedulerManagerInterface manager, ResourceTaskConfig config,
			ServerConfig serverConfig) {
		this.manager = manager;
		this.zkPaths = zkPaths;
		this.config = config;
		this.serverConfig = serverConfig;
	}

	@Override
	public void isLeader() {
		try {
			LOG.info("==========================LEADER=================================");
			// 若接口为空则返回空
			if (manager == null) {
				LOG.warn("SchedulerManagerInterface is null, Loss biggerst !!!");
				return;
			}
			Properties prop = DefaultBaseSchedulers.createSimplePrope(3, 1000l);
			boolean createFlag;
			createFlag = this.manager.createTaskPool(META_TASK_MANAGER, prop);
			// 若创建不成功则返回
			if (!createFlag) {
				LOG.warn("create task manager server fail !!!!");
				return;
			}
			boolean cFlag = this.manager.startTaskPool(META_TASK_MANAGER);
			if (!cFlag) {
				LOG.info("Follower will quiting  !!!");
				return;
			}
			LOG.info("Leader create task manager server success !!!");
			checkSwitchTask();
			sumbitTask();
			createCheckCyclePool();
			LOG.info("==========================LEADER FINISH=================================");
			// 提交任务线程
			Thread.sleep(Long.MAX_VALUE);
			manager.destoryTaskPool(META_TASK_MANAGER, false);
			LOG.info("loss the leader !!!");
		}
		catch (ParamsErrorException | InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void notLeader() {
		try {
			// 若接口为空则返回空
			if (manager == null) {
				LOG.warn("SchedulerManagerInterface is null, No to do");
				return;
			}
			manager.destoryTaskPool(META_TASK_MANAGER, false);
			manager.destoryTaskPool(COPY_CYCLE_POOL, false);
			LOG.info("loss the leader !!!");
		}
		catch (ParamsErrorException e) {
			e.printStackTrace();
		}
	}

	public void checkSwitchTask() {
		ManagerContralFactory mcf = ManagerContralFactory.getInstance();
		MetaTaskManagerInterface release = mcf.getTm();
		Map<String, Boolean> switchTask = config.getTaskPoolSwitchMap();
		TaskTypeModel type = null;
		String taskTypeName = null;
		boolean flag = false;
		for (Map.Entry<String, Boolean> entry : switchTask.entrySet()) {
			taskTypeName = entry.getKey();
			flag = entry.getValue();
			if (BrStringUtils.isEmpty(taskTypeName)) {
				continue;
			}
			type = release.getTaskTypeInfo(taskTypeName);
			if (type == null) {
				type = new TaskTypeModel();
			}
			else if (type.isSwitchFlag() == flag) {
				continue;
			}
			type.setSwitchFlag(flag);
			release.setTaskTypeModel(taskTypeName, type);
		}
	}

	/**
	 * 概述：提交任务
	 * @throws ParamsErrorException
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	private void sumbitTask() throws ParamsErrorException {
		Map<String, String> createDataMap = JobDataMapConstract.createCreateDataMap(serverConfig, config);
		SumbitTaskInterface createJob = QuartzSimpleInfo.createCycleTaskInfo("CREATE_SYSTEM_TASK",
			config.getCreateTaskIntervalTime(), 60000, createDataMap, CreateSystemTaskJob.class);
		Map<String, String> metaDataMap = JobDataMapConstract.createMetaDataMap(config);
		SumbitTaskInterface metaJob = QuartzSimpleInfo.createCycleTaskInfo("META_MANAGER_TASK",
			config.getCreateTaskIntervalTime(), 60000, metaDataMap, ManagerMetaTaskJob.class);
		Map<String, String> copyJobMap = JobDataMapConstract.createCopyCheckMap(config);
		SumbitTaskInterface checkJob = QuartzSimpleInfo.createCycleTaskInfo("COPY_CHECK_TASK",
			config.getCreateCheckJobTaskervalTime(), 60000, copyJobMap, CopyCheckJob.class);

		boolean isSuccess = false;
		isSuccess = this.manager.addTask(META_TASK_MANAGER, createJob);
		LOG.info("sumbit create Job {} ", isSuccess ? " Sucess" : "Fail");
		isSuccess = this.manager.addTask(META_TASK_MANAGER, metaJob);
		LOG.info("sumbit meta Job {} ", isSuccess ? " Sucess" : "Fail");
		boolean createFlag = config.getTaskPoolSwitchMap().get(TaskType.SYSTEM_COPY_CHECK.name());
		if (createFlag) {
			isSuccess = this.manager.addTask(META_TASK_MANAGER, checkJob);
			LOG.info("sumbit Create Check Job {} ", isSuccess ? " Sucess" : "Fail");
		}
		else {
			LOG.info("=======> create copy check thread fail");
		}
	}

	public void createCheckCyclePool() throws ParamsErrorException {
		Properties prop = DefaultBaseSchedulers.createSimplePrope(1, 1000L);

		boolean createFlag = this.manager.createTaskPool(COPY_CYCLE_POOL, prop);

		if (!createFlag) {
			LOG.warn("create check pool fail !!!!");
			return;
		}
		boolean cFlag = this.manager.startTaskPool(COPY_CYCLE_POOL);
		if (!cFlag) {
			LOG.info("Follower will quiting  !!!");
			return;
		}

		Map content = JobDataMapConstract.createCylcCheckDataMap(this.config.getCheckTimeRange());
		SumbitTaskInterface sumbit = QuartzCronInfo.getInstance("CYCLE_CHECK_JOB", "CYCLE_CHECK_JOB",
			this.config.getCheckCronStr(), content, CheckCycleJob.class);
		cFlag = this.manager.addTask("COPY_CYCLE_POOL", sumbit);
		LOG.info("sumbit Cycle task :{}", Boolean.valueOf(cFlag));
	}
}
