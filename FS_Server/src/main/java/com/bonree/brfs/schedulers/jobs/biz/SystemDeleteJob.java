package com.bonree.brfs.schedulers.jobs.biz;

import org.quartz.JobExecutionContext;
import org.quartz.UnableToInterruptJobException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.schedulers.task.operation.impl.QuartzOperationStateTask;
/******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年5月3日 下午4:29:44
 * @Author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @Description:系统删除任务 
 *****************************************************************************
 */
public class SystemDeleteJob extends QuartzOperationStateTask {
	private static final Logger LOG = LoggerFactory.getLogger("SystemDeleteJob");
	@Override
	public void caughtException(JobExecutionContext context) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void interrupt() throws UnableToInterruptJobException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void operation(JobExecutionContext context) throws Exception {
		// TODO Auto-generated method stub
		LOG.info("Delete data .........");
	}

}
