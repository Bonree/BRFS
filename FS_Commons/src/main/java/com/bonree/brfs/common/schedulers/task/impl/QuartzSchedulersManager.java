package com.bonree.brfs.common.schedulers.task.impl;

import java.beans.PropertyEditorSupport;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.common.schedulers.task.SchedulerManagerInterface;
import com.bonree.brfs.common.utils.StringUtils;
import com.bonree.brfs.common.schedulers.model.TaskInterface;
/******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年3月28日 下午4:19:33
 * @Author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @Description: 单例模式的调度接口
 *****************************************************************************
 */
public class QuartzSchedulersManager implements SchedulerManagerInterface<String, QuartzBaseSchedulers, TaskInterface>{
	Map<String,QuartzBaseSchedulers> taskPoolMap = new ConcurrentHashMap<String,QuartzBaseSchedulers>();
	private static final Logger logger = LoggerFactory.getLogger("TaskManagerServer");
	private static class SingletonInstance {
		public static QuartzSchedulersManager instance = new QuartzSchedulersManager();
	}

	private QuartzSchedulersManager() {
	}

	public static QuartzSchedulersManager getInstance() {
		return SingletonInstance.instance;
	}
	@Override
	public boolean addTask(String taskpoolkey, TaskInterface task) {
		if(!taskPoolMap.containsKey(taskpoolkey)){
			return false;
		}
		QuartzBaseSchedulers pool = taskPoolMap.get(taskpoolkey);
		if(pool == null){
			return false;
		}
		try {
			return pool.addTask(task);
		}
		catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	}

	@Override
	public boolean pauseTask(String taskpoolkey, TaskInterface task) {
		// TODO Auto-generated method stub
		if(!taskPoolMap.containsKey(taskpoolkey)){
			return false;
		}
		QuartzBaseSchedulers pool = taskPoolMap.get(taskpoolkey);
		if(pool == null){
			return false;
		}
		try {
			pool.pauseTask(task);
		}
		catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
		return true;
	}

	@Override
	public boolean resumeTask(String taskpoolKey, TaskInterface task) {
		// TODO Auto-generated method stub
		if(!taskPoolMap.containsKey(taskpoolKey)){
			return false;
		}
		QuartzBaseSchedulers pool = taskPoolMap.get(taskpoolKey);
		if(pool == null){
			return false;
		}
		try {
			pool.resumeTask(task);
		}
		catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
		return true;
	}

	@Override
	public boolean deleteTask(String taskpoolKey, TaskInterface task) {
		// TODO Auto-generated method stub
		if (!taskPoolMap.containsKey(taskpoolKey)) {
			return false;
		}
		QuartzBaseSchedulers pool = taskPoolMap.get(taskpoolKey);
		if (pool == null) {
			return false;
		}
		try {
			pool.killTask(task);
		}
		catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
		return true;
	}

	@Override
	public boolean getTaskStat(String taskpoolKey, TaskInterface task) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean closeTaskPool(String taskpoolKey, boolean isWaitTaskCompleted) {
		// TODO Auto-generated method stub
		if (!taskPoolMap.containsKey(taskpoolKey)) {
			return false;
		}
		QuartzBaseSchedulers pool = taskPoolMap.get(taskpoolKey);
		if (pool == null) {
			return false;
		}
		try {
			pool.close(isWaitTaskCompleted);
		}
		catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
		return true;
	}

	@Override
	public boolean createTaskPool(String taskpoolKey, Properties prop){
		if (taskPoolMap.containsKey(taskpoolKey)) {
			return false;
		}
		QuartzBaseSchedulers pool = new QuartzBaseSchedulers();
		String instanceName = taskpoolKey;
		if(prop != null&& StringUtils.isEmpty(prop.get("org.quartz.scheduler.instanceName").toString())){
			instanceName = prop.get("org.quartz.scheduler.instanceName").toString();
		}
		pool.setInstanceName(instanceName);
		try {
			pool.initProperties(prop);
			this.taskPoolMap.put(taskpoolKey, pool);
		}
		catch (Exception e) {
			logger.error("Exception : {}",e);
			
			return false;
		}
		return true;
	}

	@Override
	public boolean startTaskPool(String taskpoolKey) {
		// TODO Auto-generated method stub
		if (!taskPoolMap.containsKey(taskpoolKey)) {
			return false;
		}
		QuartzBaseSchedulers pool = taskPoolMap.get(taskpoolKey);
		if (pool == null) {
			return false;
		}
		try {
			if(pool.isStart()){
				return false;
			}
			pool.start();
		}
		catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
		return true;
	}

	@Override
	public boolean pauseTaskPool(String taskpoolKey) {
		// TODO Auto-generated method stub
		if (!taskPoolMap.containsKey(taskpoolKey)) {
			return false;
		}
		QuartzBaseSchedulers pool = taskPoolMap.get(taskpoolKey);
		if (pool == null) {
			return false;
		}
		try {
			pool.pauseAllTask();
		}
		catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
		return true;
	}

	@Override
	public boolean resumeTaskPool(String taskpoolKey) {
		// TODO Auto-generated method stub
		if (!taskPoolMap.containsKey(taskpoolKey)) {
			return false;
		}
		QuartzBaseSchedulers pool = taskPoolMap.get(taskpoolKey);
		if (pool == null) {
			return false;
		}
		try {
			pool.resumeAllTask();
		}
		catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
		return true;
	}

	@Override
	public int getTaskPoolStat(String taskpoolKey) {
		if (!taskPoolMap.containsKey(taskpoolKey)) {
			return -1;
		}
		QuartzBaseSchedulers pool = taskPoolMap.get(taskpoolKey);
		if (pool == null) {
			return -2;
		}
		try {
			return pool.getPoolStat();
		}
		catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return -3;
		}
	}

}
