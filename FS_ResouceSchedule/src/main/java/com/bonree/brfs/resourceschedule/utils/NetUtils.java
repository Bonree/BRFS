package com.bonree.brfs.resourceschedule.utils;
/******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年3月16日 下午1:39:24
 * @Author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @Description:网络信息处理工具 
 *****************************************************************************
 */
public class NetUtils {
	/**
     * 概述：过滤非法的ip地址
     * @param ip
     * @return
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public static Boolean filterIp(String ip){
    	// 1.过滤为空的ip
        if(StringUtils.isEmpty(ip)){
            return true;
        }
        // 2.过滤长度超过的ip地址
        if(ip.length() > 15){
        	return true;
        }
        String[] ipEles = StringUtils.getSplit(ip, ".");
        // 3.过滤格式不对的ip地址
        if(ipEles.length != 4){
        	return true;
        }
        // 4.过滤内容不对的ip地址
        for(String ipEle : ipEles){
        	if(!StringUtils.isMathNumeric(ipEle)){
        		return true;
        	}
        	long value = Long.valueOf(ipEle);
        	if(value < 0 || value >255){
        		return true;
        	}
        }
        // 5.过滤回环地址
        if("127.0.0.1".equals(ip)){
            return true;
        }
        // 6.过滤空地址
        if("0.0.0.0".equals(ip)){
            return true;
        }
        return false;
    }
}
