package com.bonree.brfs.schedulers.task.manager;

import java.util.Collection;
import java.util.List;
import java.util.Properties;

import com.bonree.brfs.schedulers.exception.ParamsErrorException;
/******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年3月27日 下午3:44:57
 * @Author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @Description: 任务服务接口
 *****************************************************************************
 */
public interface SchedulerManagerInterface <T1,T2,T3>{
	/**
	 * 概述：添加任务
	 * @param taskpoolkey 对应的任务
	 * @param task 任务信息
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	boolean addTask(T1 taskpoolkey, T3 task) throws ParamsErrorException;
	/**
	 * 概述：暂停任务
	 * @param taskpoolkey 对应的任务
	 * @param task 任务信息
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	boolean pauseTask(T1 taskpoolkey,T3 task) throws ParamsErrorException;
	/**
	 * 概述：恢复任务
	 * @param taskpoolkey 对应的任务
	 * @param task 任务信息
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	boolean resumeTask(T1 taskpoolKey,T3 task) throws ParamsErrorException;
	/**
	 * 概述：删除任务
	 * @param taskpoolkey 对应的任务
	 * @param task 任务信息
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	boolean deleteTask(T1 taskpoolKey,T3 task) throws ParamsErrorException;
	/**
	 * 概述：关闭任务线程池
	 * @param taskpoolkey 对应的任务
	 * @param isWaitTaskCompleted 等待任务完成
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	boolean closeTaskPool(T1 taskpoolKey, boolean isWaitTaskCompleted) throws ParamsErrorException;
	/**
	 * 概述：创建任务线程池
	 * @param taskpoolKey 对应的任务
	 * @param prop 线程池配置
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	boolean createTaskPool(T1 taskpoolKey, Properties prop) throws ParamsErrorException;
	/**
	 * 概述：启动任务线程池
	 * @param taskpoolKey
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	boolean startTaskPool(T1 taskpoolKey) throws ParamsErrorException;
	/**
	 * 概述：暂停任务线程池所有任务
	 * @param taskpoolKey 对应的任务
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	boolean pauseTaskPool(T1 taskpoolKey) throws ParamsErrorException;
	/**
	 * 概述：重新运行线程池被暂停的
	 * @param taskpoolKey
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	boolean resumeTaskPool(T1 taskpoolKey) throws ParamsErrorException;
	/**
	 * 概述：获取执行任务个数
	 * @param taskpoolKey
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	int getRunningTaskCount(T1 taskpoolKey) throws ParamsErrorException;
	/**
	 * 概述：获取任务线程池个数
	 * @param taskpoolKey
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	int getTaskPoolThreadCount(T1 taskpoolKey) throws ParamsErrorException;
	/**
	 * 概述：获取所有线程池的key
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	Collection<String> getAllPoolKey();
	/**
	 * 概述：获取已经开始的线程池key的集合
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	Collection<String> getStartedPoolKeys();
	/**
	 * 概述：获取已经关闭的线程池key的集合
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	Collection<String> getClosedPoolKeys();
	/**
	 * 概述：获取已经暂停的线程池
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	Collection<String> getPausePoolKeys();
	/**
	 * 概述：指定线程池是否启动
	 * @param taskpoolKey
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	boolean isStarted(T1 taskpoolKey)throws ParamsErrorException;;
	/**
	 * 概述：指定线程池是否关闭
	 * @param taskpoolKey
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	boolean isClosed(T1 taskpoolKey)throws ParamsErrorException;;
	/**
	 * 概述：指定线程池是否暂停
	 * @param taskpoolKey
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	boolean isPaused(T1 taskpoolKey)throws ParamsErrorException;;
	
}
