package com.bonree.brfs.resourceschedule.gather.job;

import java.util.Map;

import org.hyperic.sigar.SigarException;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import com.bonree.brfs.resouceschedule.commons.Commons;
import com.bonree.brfs.resouceschedule.commons.SigarUtils;
import com.bonree.brfs.resouceschedule.utils.Globals;
import com.bonree.brfs.resouceschedule.vo.BaseNetInfo;
import com.bonree.brfs.resouceschedule.vo.BasePatitionInfo;
import com.bonree.brfs.resouceschedule.vo.BaseServerInfo;
import com.bonree.brfs.resouceschedule.vo.NetStatInfo;
import com.bonree.brfs.resouceschedule.vo.PatitionStatInfo;
import com.bonree.brfs.resouceschedule.vo.ServerStatInfo;
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
		ServerStatInfo stat = Commons.gatherServerStatInfo(Globals.DATA_DIRECTORY);
		Globals.SERVER_INFO.addServerStat(stat);
	}
}
