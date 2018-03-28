package com.bonree.brfs.common.schedulers.model;

import java.util.Map;
/******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年3月28日 下午3:58:04
 * @Author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @Description: 任务信息
 *****************************************************************************
 */
public interface TaskInterface {
	/**
	 * 概述：任务名称
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	String getTaskName();
	/**
	 * 概述：任务分组
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	String getTaskGroupName();
	/**
	 * 概述：任务class路径
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	String getClassInstanceName();
	/**
	 * 概述：调度周期及循环
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	String getCycleContent();
	/**
	 * 概述：任务初始参数
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	Map<String,String> getTaskContent();
	/**
	 * 概述：任务类型 用创建不同的执行方式的任务
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	int getTaskKind();
}
