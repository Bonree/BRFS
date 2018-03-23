package com.bonree.brfs.resourceschedule.commons;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.jboss.netty.util.internal.ConcurrentHashMap;

import com.bonree.brfs.resourceschedule.model.BaseServerModel;
import com.bonree.brfs.resourceschedule.model.ServerStatModel;

/*****************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年3月12日 上午10:55:34
 * @Author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @Description: 
 *****************************************************************************/
public class Cache {
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
     * 服务基本信息
     */
    public BaseServerModel baseInfo = new BaseServerModel();
    /**
     * 服务状态信息
     */
    public Queue<ServerStatModel> statInfoQueue = new ConcurrentLinkedQueue<ServerStatModel>();
	/**
	 * 服务器基础信息
	 */
	public List<BaseServerModel> BASE_CLUSTER_INFO = new ArrayList<BaseServerModel>();
	/**
	 * sn与磁盘的对应关系 key：sn根目录，value：分区挂载点
	 */
	public Map<String,String> snWithDisk = new ConcurrentHashMap<String,String>();
	/**
	 * sn剩余空间大小，Key：sn根目录，value：剩余大小
	 */
	public Map<String,Long> snRemainSizeMap = new ConcurrentHashMap<String,Long>();
	
	/**
	 * 服务数据目录
	 */
	public String DATA_DIRECTORY = "C:/";
	/**
	 * 服务标识
	 */
	public int SERVER_ID = 0;
	/**
	 * sn信息队列
	 */
	public List<String> snList = new ArrayList<String>();
}
