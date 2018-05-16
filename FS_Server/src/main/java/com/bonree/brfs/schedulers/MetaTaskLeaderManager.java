
package com.bonree.brfs.schedulers;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent.Type;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.shaded.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.zookeeper.curator.cache.AbstractTreeCacheListener;
import com.bonree.brfs.common.zookeeper.curator.cache.CuratorCacheFactory;
import com.bonree.brfs.common.zookeeper.curator.cache.CuratorTreeCache;
import com.bonree.brfs.configuration.ResourceTaskConfig;
import com.bonree.brfs.configuration.ServerConfig;
import com.bonree.brfs.schedulers.exception.ParamsErrorException;
import com.bonree.brfs.schedulers.jobs.JobDataMapConstract;
import com.bonree.brfs.schedulers.jobs.system.CreateSystemTaskJob;
import com.bonree.brfs.schedulers.jobs.system.ManagerMetaTaskJob;
import com.bonree.brfs.schedulers.task.manager.SchedulerManagerInterface;
import com.bonree.brfs.schedulers.task.manager.impl.DefaultBaseSchedulers;
import com.bonree.brfs.schedulers.task.meta.SumbitTaskInterface;
import com.bonree.brfs.schedulers.task.meta.impl.QuartzSimpleInfo;

/*****************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年5月10日 下午5:16:45
 * @Author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @Description: 选取任务管理leader
 *****************************************************************************
 */
public class MetaTaskLeaderManager implements LeaderLatchListener{
	private static final Logger LOG = LoggerFactory.getLogger(MetaTaskLeaderManager.class);
	public static final String META_TASK_MANAGER = "META_TASK_MANAGER";
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
			sumbitTask();
			LOG.info("==========================LEADER FINISH=================================");
			// 提交任务线程
			Thread.sleep(Long.MAX_VALUE);
			manager.destoryTaskPool(META_TASK_MANAGER, false);
			LOG.info("loss the leader !!!");
		}
		catch (ParamsErrorException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void notLeader() {
		LOG.info(" ======================== Slave run away !!!");
	}
	/**
	 * 概述：提交任务
	 * @throws ParamsErrorException
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	private void sumbitTask() throws ParamsErrorException {
		Map<String, String> createDataMap = JobDataMapConstract.createCreateDataMap(serverConfig, config);
		SumbitTaskInterface createJob = QuartzSimpleInfo.createCycleTaskInfo("CREATE_SYSTEM_TASK",
			config.getCreateTaskIntervalTime(), -1, createDataMap, CreateSystemTaskJob.class);
		Map<String, String> metaDataMap = JobDataMapConstract.createMetaDataMap(config);
		SumbitTaskInterface metaJob = QuartzSimpleInfo.createCycleTaskInfo("META_MANAGER_TASK",
			config.getCreateTaskIntervalTime(), -1, metaDataMap, ManagerMetaTaskJob.class);
		SumbitTaskInterface checkJob = QuartzSimpleInfo.createCycleTaskInfo("", intervalTime, delayTime, jobMap, clazz)
		
		boolean isSuccess = this.manager.addTask(META_TASK_MANAGER, createJob);
		LOG.info("sumbit create Job {} ", isSuccess ? " Sucess" : "Fail");
		isSuccess = this.manager.addTask(META_TASK_MANAGER, metaJob);
		LOG.info("sumbit meta Job {} ", isSuccess ? " Sucess" : "Fail");
	}
}
