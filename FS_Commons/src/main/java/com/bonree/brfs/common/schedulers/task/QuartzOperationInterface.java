package com.bonree.brfs.common.schedulers.task;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
/*****************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年3月28日 下午3:49:38
 * @Author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @Description: quartz的任务执行接口，不可中断的
 *****************************************************************************
 */
public interface QuartzOperationInterface extends OperationInterface<JobExecutionContext>,Job{

}
