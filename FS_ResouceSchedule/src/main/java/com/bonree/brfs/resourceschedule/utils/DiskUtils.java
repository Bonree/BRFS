package com.bonree.brfs.resourceschedule.utils;

import java.io.File;
/*****************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年3月16日 下午1:38:10
 * @Author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @Description:磁盘工具类 
 *****************************************************************************
 */
public class DiskUtils {
	/**
     * 概述：过滤非法的挂载点
     * @param mountPoint 文件分区挂载点
     * @return
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public static Boolean filterMountPoint(String mountPoint){
    	// 1.挂载点为空返回NULL
    	if(StringUtils.isEmpty(mountPoint)){
    		return true;
    	}
    	File mountFile = new File(mountPoint);
    	// 2.目录不存在返回NULL
    	if(!mountFile.exists()){
    		return true;
    	}
    	// 3.挂载点为文件返回NULL
    	if(mountFile.isFile()){
    		return true;
    	}
    	return false;
}
}
