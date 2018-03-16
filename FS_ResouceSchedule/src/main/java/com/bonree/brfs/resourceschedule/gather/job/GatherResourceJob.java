package com.bonree.brfs.resourceschedule.gather.job;

import java.util.Map;

import org.hyperic.sigar.SigarException;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import com.bonree.brfs.resourceschedule.commons.GatherResource;
import com.bonree.brfs.resourceschedule.config.ResConfig;
import com.bonree.brfs.resourceschedule.model.BaseNetModel;
import com.bonree.brfs.resourceschedule.model.BasePatitionModel;
import com.bonree.brfs.resourceschedule.model.BaseServerModel;
import com.bonree.brfs.resourceschedule.model.NetStatModel;
import com.bonree.brfs.resourceschedule.model.PatitionStatModel;
import com.bonree.brfs.resourceschedule.model.ServerStatModel;
import com.bonree.brfs.resourceschedule.utils.SigarUtils;
/******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年3月13日 下午2:21:19
 * @Author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @Description: 定时采集原始数据信息
 *****************************************************************************
 */
public class GatherResourceJob implements Job {
	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		// TODO Auto-generated method stub
		ServerStatModel stat = GatherResource.gatherServerStatInfo(ResConfig.DATA_DIRECTORY);
		ResConfig.SERVER_INFO.addServerStat(stat);
	}
}
