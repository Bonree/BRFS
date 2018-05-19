package com.bonree.brfs.common.utils;

import java.io.File;
import java.time.format.DateTimeFormatter;

import org.joda.time.DateTime;

/******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年5月2日 上午11:17:12
 * @Author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @Description:storageName目录生成工具 
 *****************************************************************************
 */
public class StorageNameFileUtils {
	/**
	 * 概述：获取指定时间的目录
	 * @param sn
	 * @param dataPath
	 * @param time 单位为ms
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static String createSNDir(String sn, int copyIndex, final long time){
		if(BrStringUtils.isEmpty(sn)){
			return null;
		}
		String strTime = TimeUtils.timeInterval(time, 60*60*1000);
		StringBuilder str = new StringBuilder();
		str.append(sn).append(File.separator)
		.append(copyIndex).append(File.separator)
		.append(strTime);
		return str.toString();
	}
}
