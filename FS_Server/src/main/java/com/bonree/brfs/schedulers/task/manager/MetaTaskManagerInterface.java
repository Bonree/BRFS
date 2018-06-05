package com.bonree.brfs.schedulers.task.manager;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.bonree.brfs.common.utils.Pair;
import com.bonree.brfs.schedulers.task.model.TaskModel;
import com.bonree.brfs.schedulers.task.model.TaskServerNodeModel;
import com.bonree.brfs.schedulers.task.model.TaskTypeModel;

/******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年4月1日 下午4:17:24
 * @Author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @Description: 发布任务到zk上
 *****************************************************************************
 */
public interface MetaTaskManagerInterface {
	/**
	 * 概述：修改或发布任务数据 
	 * 当taskName为空时 则为发布任务
	 * @param data
	 * @param taskType
	 * @param taskName
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	String updateTaskContentNode(TaskModel data, String taskType, String taskName);
	
	/**
	 * 概述：修改或发布服务节点任务
	 * @param serverId
	 * @param taskName
	 * @param taskType
	 * @return
	 * @throws Exception
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	boolean updateServerTaskContentNode(String serverId, String taskName, String taskType, TaskServerNodeModel data);
	/**
	 * 概述：获取节点任务信息
	 * @param taskType
	 * @param taskName
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	TaskModel getTaskContentNodeInfo(String taskType, String taskName);
	/**
	 * 概述：获取服务任务信息
	 * @param taskType
	 * @param taskName
	 * @param serverId
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	TaskServerNodeModel getTaskServerContentNodeInfo(String taskType, String taskName, String serverId);
	/**
	 * 概述：修改任务节点状态
	 * @param taskName
	 * @param taskType
	 * @return
	 * @throws Exception
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	boolean changeTaskContentNodeState(String taskName,String taskType, int taskState);
	/**
	 * 概述：修改任务节点状态，加锁方式
	 * @param taskName
	 * @param taskType
	 * @param taskState
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	boolean changeTaskContentNodeStateByLock(String serverId,String taskName,String taskType, int taskState);
	/**
	 * 概述：修改服务任务节点状态
	 * @param taskName
	 * @param taskType
	 * @param serverId
	 * @param taskState
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	boolean changeTaskServerNodeContentState(String taskName, String taskType,String serverId, int taskState);
	/**
	 * 概述：获取任务最新序号
	 * @param zkUrl
	 * @param taskType
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	String getCurrentTaskIndex(String taskType);
	/**
	 * 概述：获取任务状态
	 * @param zkUrl
	 * @param taskname
	 * @param taskType
	 * @return
	 * @throws Exception
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	int queryTaskState(String taskname, String taskType);
	
	/**
	 * 概述：获取指定类型 指定服务 最后一次成功执行的记录
	 * @param taskType
	 * @param serverId
	 * @return
	 * @throws Exception
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	String getLastSuccessTaskIndex(String taskType, String serverId);
	
	/***
	 * 概述：删除指定类型的任务
	 * @param taskName
	 * @param taskType
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	boolean deleteTask(String taskName, String taskType);
	/**
	 * 概述：删除指定类型 指定日期以前的任务信息
	 * @param deleteTime
	 * @param taskType
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	int deleteTasks(long deleteTime, String taskType);
	/**
	 * 概述：判断是否初始化
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	boolean isInit();
	/**
	 * 概述：初始化接口
	 * @param zkUrl
	 * @param taskPath
	 * @param args
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	void setPropreties(String zkUrl, String taskPath, String lockPath, String... args);
	/**
	 * 概述：维护任务数据状态，包括删除及任务状态校验
	 * @param taskType
	 * @param ttl
	 * @param aliveServers
	 * @return pair key 删除任务个数， value 修正的任务个数
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	Pair<Integer,Integer> reviseTaskStat(String taskType, long ttl, Collection<String> aliveServers);
	/**
	 * 概述：获取下一个任务的名称
	 * @param taskType
	 * @param taskName
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	String getNextTaskName(String taskType, String taskName);
	/**
	 * 概述：获取队列第一个任务名称
	 * @param taskType
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	String getFirstTaskName(String taskType);
	/**
	 * 概述：获取子任务所有状态
	 * 当taskName为空时 则为发布任务
	 * @param data
	 * @param taskType
	 * @param taskName
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	List<Pair<String, Integer>> getServerStatus(String taskType, String taskName);
	/***
	 * 概述：获取servid第一次出现的任务
	 * @param taskType
	 * @param serverId
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	String getFirstServerTask(String taskType,String serverId);
	/**
	 * 概述：获取任务的server任务列表
	 * @param taskType
	 * @param taskName
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	List<String> getTaskServerList(String taskType, String taskName);
	/**
	 * 概述：获取指定任务的列表
	 * @param taskType
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	List<String> getTaskList(String taskType);
	/**
	 * 概述：获取任务类型列表
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	List<String> getTaskTypeList();
	/**
	 * 概述：获取taskType元信息
	 * @param taskType
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	TaskTypeModel getTaskTypeInfo(String taskType);
	/**
	 * 概述：设置taskType元信息
	 * @param taskType
	 * @param sn
	 * @param time
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	boolean setTaskTypeModel(String taskType,TaskTypeModel type);
}
