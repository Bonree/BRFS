package com.bonree.brfs.schedulers.task.manager;

import java.util.Map;

import com.bonree.brfs.resourceschedule.model.StatServerModel;
import com.bonree.brfs.schedulers.task.model.TaskExecutablePattern;
import com.bonree.brfs.schedulers.task.model.TaskModel;
import com.bonree.brfs.schedulers.task.model.TaskRunPattern;

/*****************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年3月16日 上午10:51:07
 * @Author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @Description: 任务资源管理接口
 *****************************************************************************
 */
public interface RunnableTaskInterface {
	/**
	 * 概述：判断任务是否可以执行
	 * @param taskType
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	boolean taskRunnable(int taskType, int poolSize, int threadCount) throws Exception;
	/**
	 * 概述：任务执行模式
	 * @param task
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	TaskRunPattern taskRunnPattern(TaskModel task) throws Exception;
	
	/**
	 * 概述：更新资源数据
	 * @param 
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	void update(StatServerModel resources);
	/**
	 * 概述：获取上次更新时间
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	long getLastUpdateTime();
	/**
	 * 概述：设置异常过滤指标
	 * @param limits
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	void setLimitParameter(TaskExecutablePattern limits);
	/**
	 * 概述：设置任务级别
	 * @param taskLevel
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	void setTaskLevel(Map<Integer, Integer> taskLevel);
}
