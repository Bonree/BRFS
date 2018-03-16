package com.bonree.brfs.resourceschedule.service;

import java.util.Properties;

import com.bonree.brfs.resourceschedule.config.JobConfig;
/*****************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年3月15日 下午5:44:36
 * @Author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @Description: 定时服务接口
 *****************************************************************************
 */
public interface CycleServerInterface {
	/**
	 * 概述：初始化服务配置
	 * @param props
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public void initProperties(Properties props) throws Exception;
	/**
	 * 概述：加载任务到服务
	 * @param jobConfig
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public boolean loadJob(JobConfig jobConfig) throws Exception;
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
	public void close() throws Exception;
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
	public boolean killJob(JobConfig jobConfig) throws Exception;

}
