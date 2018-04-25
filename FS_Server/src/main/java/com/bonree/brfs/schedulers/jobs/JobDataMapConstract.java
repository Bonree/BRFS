package com.bonree.brfs.schedulers.jobs;

import java.util.HashMap;
import java.util.Map;

import com.bonree.brfs.configuration.ResourceTaskConfig;
import com.bonree.brfs.configuration.ServerConfig;
import com.bonree.brfs.resourceschedule.service.impl.RandomAvailable;

public class JobDataMapConstract {
	/**
	 * zookeeper地址
	 */
//	public static final String ZOOKEEPER_ADDRESS = "ZOOKEEPER_ADDRESS";
	/**
	 * serverid
	 */
	public static final String SERVER_ID = "SERVER_ID";
	/**
	 * 数据目录
	 */
	public static final String DATA_PATH = "DATA_PATH";
	/**
	 * 集群分组
	 */
	public static final String CLUSTER_NAME = "CLUSTER_NAME";
	/**
	 * ip地址
	 */
	public static final String IP = "IP";
	/**
	 * 采集样本的间隔
	 */
	public static final String GATHER_INVERAL_TIME = "GATHER_INVERAL_TIME";
	/**
	 * 当样本数为几个是计算
	 */
	public static final String CALC_RESOURCE_COUNT = "CALC_RESOURCE_COUNT";
	/**
	 * 可用server的实现类
	 */
	public static final String AVAIABLE_SERVER_CLASS = "AVAIABLE_SERVER_CLASS";
	/**
	 * 概述：生成采集job需要的参数
	 * @param server
	 * @param resource
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	//TODO: 存在临时参数，需要确认serverId在启动时哪里获取到
	public static Map<String,String> createGatherResourceDataMap(ServerConfig server, ResourceTaskConfig resource, String serverId){
		Map<String, String>  dataMap = new HashMap<>();
		//TODO: 存在临时参数，需要确认serverId在启动时哪里获取到
		dataMap.put(DATA_PATH, server.getDataPath());
		dataMap.put(IP, server.getHost());
		dataMap.put(GATHER_INVERAL_TIME, resource.getGatherResourceInveralTime() + "");
		dataMap.put(CALC_RESOURCE_COUNT, resource.getCalcResourceValueCount() + "");
		return dataMap;
	}
	public static Map<String,String> createAsynResourceDataMap(ServerConfig server, ResourceTaskConfig resource){
		Map<String, String>  dataMap = new HashMap<>();
		dataMap.put(GATHER_INVERAL_TIME, resource.getGatherResourceInveralTime() + "");
		dataMap.put(CALC_RESOURCE_COUNT, resource.getCalcResourceValueCount() + "");
		dataMap.put(AVAIABLE_SERVER_CLASS, RandomAvailable.class.getCanonicalName());
		return dataMap;
	}
}
