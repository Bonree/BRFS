package com.bonree.brfs.resourceschedule.gather.job;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import com.bonree.brfs.resourceschedule.commons.Cache;
import com.bonree.brfs.resourceschedule.commons.Constant;
import com.bonree.brfs.resourceschedule.commons.impl.GatherResource;
import com.bonree.brfs.resourceschedule.model.BaseServerModel;
import com.bonree.brfs.resourceschedule.model.ResourceModel;
import com.bonree.brfs.resourceschedule.model.ServerStatModel;

/*****************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年3月13日 下午2:54:05
 * @Author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @Description: 采集原始数据job
 *****************************************************************************/
public class CalcResourceJob implements Job {

	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		// 1.汇总状态信息
		ServerStatModel stat = GatherResource.sumServerStatus(Constant.cache);
		// 2.汇总集群信息
		BaseServerModel base = GatherResource.sumBaseClusterModel(Constant.cache.SERVER_ID, Constant.cache.BASE_CLUSTER_INFO);
		// 3.计算可用server信息
		ResourceModel resource = GatherResource.calcResource(base, stat);
	}

}
