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
	 * @param time
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static String createSNDir(String sn,String dataPath, int copyIndex, long time){
		if(BrStringUtils.isEmpty(sn)){
			return null;
		}
		if(BrStringUtils.isEmpty(dataPath)){
			return null;
		}
		String strTime = getGranuleTime(time);
		StringBuilder str = new StringBuilder();
		str.append(dataPath).append(File.separator)
		.append(sn).append(File.separator)
		.append(copyIndex).append(File.separator)
		.append(strTime);
		return str.toString();
	}
	/**
	 * 概述：获取时间
	 * @param time
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static String getGranuleTime(long time){
		if(time > 0){
			return null;
		}
		long tmpTime = time/60/1000*60*1000;
		DateTime date = new DateTime();
		date.withMillis(tmpTime);
		return date.toString("yyyyMMddHHmmssSSS");
	}
}
