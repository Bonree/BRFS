package com.bonree.brfs.common.schedulers.task;

import java.util.List;
import java.util.Properties;

import org.quartz.SchedulerException;

/*****************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年3月15日 下午5:44:36
 * @Author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @Description: 调度基础接口
 *****************************************************************************
 */
public interface BaseSchedulerInterface <T1,T2> {
	/**
	 * 概述：初始化服务配置
	 * @param props
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public void initProperties(T1 props) throws Exception;
	/**
	 * 概述：加载任务到服务
	 * @param jobConfig
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public boolean addTask(T2 task) throws Exception;
	/**
	 * 概述：启动周期服务
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public void start() throws Exception;
	/**
	 * 概述：关闭周期服务
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public void close(boolean isWaitTaskComplete) throws Exception;
	/**
	 * 概述：判断服务是否已经启动
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public boolean isStart() throws Exception;
	/**
	 * 概述：服务已经关闭
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public boolean isShuttdown() throws Exception;
	/**
	 * 概述：杀死指定的job
	 * @param jobConfig
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public boolean killTask(T2 task) throws Exception;
	/**
	 * 概述：暂停任务
	 * @param task
	 * @return
	 * @throws Exception
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public boolean pauseTask(T2 task) throws Exception;
	/**
	 * 概述：暂停所有任务
	 * @return
	 * @throws Exception
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public boolean pauseAllTask() throws Exception;
	/**
	 * 概述：获取暂停的任务
	 * @return
	 * @throws Exception
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public List<T2> getPauseTask() throws Exception;
	/**
	 * 概述：获取所有任务
	 * @return
	 * @throws Exception
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public List<T2> getAllTask() throws Exception;
	/**
	 * 概述：重启任务
	 * @param task
	 * @return
	 * @throws Exception
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public boolean resumeTask(T2 task) throws Exception;
	/**
	 * 概述：重启所有任务
	 * @return
	 * @throws Exception
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public boolean resumeAllTask() throws Exception;
	/**
	 * 概述：获取线程池状态
	 * @return
	 * @throws Exception
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public int getPoolStat() throws Exception;
	/**
	 * 概述：获取线程池名称
	 * @return
	 * @throws Exception
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public String getInstanceName() throws Exception;
	/**
	 * 概述：检查任务
	 * @param task
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public boolean checkTask(T2 task);	
	/**
	 * 概述：获取任务状态
	 * @param task
	 * @return
	 * @throws SchedulerException
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public int getTaskStat(T2 task) throws Exception;
	/**
	 * 概述：判断任务是否执行
	 * @param task
	 * @return
	 * @throws SchedulerException
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public boolean isExecuting(T2 task) throws Exception;
	/**
	 * 概述：获取任务线程池数
	 * @return
	 * @throws Exception
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public int getPoolThreadCount() throws Exception;
	/**
	 * 概述：获取提交任务数
	 * @return
	 * @throws Exception
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public int getTaskThreadCount() throws Exception;
}
