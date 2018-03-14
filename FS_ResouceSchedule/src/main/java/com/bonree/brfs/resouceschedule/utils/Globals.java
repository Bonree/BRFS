package com.bonree.brfs.resouceschedule.utils;

import java.util.ArrayList;
import java.util.List;

import com.bonree.brfs.resouceschedule.vo.BaseServerInfo;
import com.bonree.brfs.resouceschedule.vo.ServerVo;

/*****************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年3月12日 上午10:55:34
 * @Author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @Description: 
 *****************************************************************************/
public class Globals {
	/**
	 * 服务器状态
	 */
	public static ServerVo SERVER_INFO;
	/**
	 * 服务器基础信息
	 */
	public static List<BaseServerInfo> BASE_CLUSTER_INFO = new ArrayList<BaseServerInfo>(); 
	/**
	 * 资源信息
	 */
	/**
	 * 网卡最大发送速度 单位byte（按10000Mbit/s网卡计算）
	 */
	public static long NET_MAX_T_SPEED = 1310720000L;
	/**
	 * 网卡最大接收速度 单位byte（按10000Mbit/s网卡计算）
	 */
	public static long NET_MAX_R_SPEED = 1310720000L;
	/**
	 * 磁盘最大写入速度 单位byte（按500MB/s的ssd硬盘计算）
	 */
	public static long DISK_MAX_WRITE_SPEED = 616448000L;
	/**
	 * 磁盘最大读取速度 单位byte（按500MB/s的ssd硬盘计算）
	 */
	public static long DISK_MAX_READ_SPEED = 616448000L;
	/**
	 * 服务数据目录
	 */
	public static String DATA_DIRECTORY = "/data/brfs/data";
	/**
	 * 服务标识
	 */
	public static int SERVER_ID = 0;
}
