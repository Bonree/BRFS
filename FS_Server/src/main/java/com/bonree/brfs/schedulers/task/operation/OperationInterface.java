package com.bonree.brfs.schedulers.task.operation;
/******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年3月28日 上午11:13:46
 * @Author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @Description: 任务执行接口
 *****************************************************************************
 */
public interface OperationInterface<T>{
	/**
	 * 概述：执行任务
	 * @param context
	 * @throws Exception
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	void operation(T context) throws Exception;
}
