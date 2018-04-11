package com.bonree.brfs.common.schedulers.task;

import java.util.Collection;

import com.bonree.brfs.common.schedulers.model.TaskContent;
import com.bonree.brfs.common.schedulers.model.TaskServerNodeContent;

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
	 * 概述：发布任务到zk
	 * @param taskContent
	 * @param zkUrl
	 * @param path
	 * @return
	 * @throws Exception
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	String releaseTaskContentNode(byte[] data, String taskType) throws Exception;
	/**
	 * 概述：发布服务节点任务
	 * @param serverId
	 * @param taskName
	 * @param taskType
	 * @return
	 * @throws Exception
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	boolean releaseServerTaskContentNode(String serverId, String taskName, String taskType, byte[] data) throws Exception;
	/**
	 * 概述：修改任务节点状态
	 * @param taskName
	 * @param taskType
	 * @return
	 * @throws Exception
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	boolean changeTaskContentNodeStat(String taskName,String taskType, int taskState) throws Exception;
	/**
	 * 概述：获取任务最新序号
	 * @param zkUrl
	 * @param taskType
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	String getCurrentTaskIndex(String taskType) throws Exception;
	/**
	 * 概述：获取任务状态
	 * @param zkUrl
	 * @param taskname
	 * @param taskType
	 * @return
	 * @throws Exception
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	int queryTaskState(String taskname, String taskType)throws Exception;
	
	/**
	 * 概述：获取指定类型 指定服务 最后一次成功执行的记录
	 * @param taskType
	 * @param serverId
	 * @return
	 * @throws Exception
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	String getLastSuccessTaskIndex(String taskType, String serverId) throws Exception;
	
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
	void setPropreties(String zkUrl, String taskPath, String... args);
}
