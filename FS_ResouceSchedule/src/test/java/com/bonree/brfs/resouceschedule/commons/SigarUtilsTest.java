
package com.bonree.brfs.resouceschedule.commons;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import org.hyperic.sigar.Sigar;
import org.hyperic.sigar.SigarException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.bonree.brfs.resouceschedule.commons.Commons;
import com.bonree.brfs.resouceschedule.commons.SigarUtils;

/*******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年3月8日 下午6:06:38
 * @Author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @Description: 测试sigarUtils工具类
 ******************************************************************************/
public class SigarUtilsTest {
	
	/**
	 * 概述：测试采集效率
	 * 
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	@Test
	public void testGatherBaseInfo(){
		try {
			if(!CommonsTest.initLibrary()){
				System.out.println("test skip gatherBaseInfo");
				return;
			}
			String path = ClassLoader.getSystemResource(".").getPath();
			String tmpPath = path.substring(0, path.lastIndexOf("FS_ResouceSchedule"));
			long startTime = System.currentTimeMillis();
			SigarUtils.instance.gatherBasePatitionInfos(tmpPath, 1000L, 1000L);
			long stopTime1 = System.currentTimeMillis();
			SigarUtils.instance.gatherBaseNetInfos(1000L, 1000L);
			SigarUtils.instance.gatherMemSize();
			SigarUtils.instance.gatherCpuCoreCount();
			long stopTime2 = System.currentTimeMillis();
			SigarUtils.instance.gatherCpuStatInfo();
			SigarUtils.instance.gatherMemoryStatInfo();
			SigarUtils.instance.gatherNetStatInfos();
			SigarUtils.instance.gatherPatitionStatInfos(tmpPath);
			long stopTime3 = System.currentTimeMillis();
			
			System.out.println("gather conf Time :" + (stopTime1 - startTime) + "ms");
			System.out.println("gather base Time :" + (stopTime2 - stopTime1) + "ms");
			System.out.println("gather stat Time :" + (stopTime3 - stopTime2) + "ms");
			System.out.println("gather gath Time :" + (stopTime3 - stopTime1) + "ms");
			long stopTime4 = System.currentTimeMillis();
			SigarUtils.instance.gatherPatitionStatInfos2("E:/");
			long stopTime5 = System.currentTimeMillis();
			SigarUtils.instance.gatherPatitionStatInfos("E:/");
			long stopTime6 = System.currentTimeMillis();
			System.out.println("gather pat2 Time :" + (stopTime5 - stopTime4) + "ms");
			System.out.println("gather pat Time :" + (stopTime6 - stopTime5) + "ms");
			
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
}
