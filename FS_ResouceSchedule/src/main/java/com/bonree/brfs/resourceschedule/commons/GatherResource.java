package com.bonree.brfs.resourceschedule.commons;

import java.io.File;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.hyperic.sigar.NetStat;
import org.hyperic.sigar.SigarException;

import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.common.zookeeper.ZookeeperClient;
import com.bonree.brfs.common.zookeeper.curator.CuratorClient;
import com.bonree.brfs.resourceschedule.model.BaseMetaServerModel;
import com.bonree.brfs.resourceschedule.model.ResourceModel;
import com.bonree.brfs.resourceschedule.model.ResourcePair;
import com.bonree.brfs.resourceschedule.model.StatServerModel;
import com.bonree.brfs.resourceschedule.model.StateMetaServerModel;
import com.bonree.brfs.resourceschedule.utils.CalcUtils;
import com.bonree.brfs.resourceschedule.utils.DiskUtils;
import com.bonree.brfs.resourceschedule.utils.SigarUtils;
import com.bonree.brfs.resourceschedule.utils.OSCheckUtils;
/*****************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年3月22日 下午3:18:24
 * @Author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @Description: 统计信息
 *****************************************************************************
 */
public class GatherResource {
	/**
	 * 概述：采集状态信息
	 * @param dataDir
	 * @param ipSet
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static StateMetaServerModel gatherResource(String dataDir, Collection<String> ipSet){
		StateMetaServerModel obj = new StateMetaServerModel();
		try {
			Set<String> ipDevSet = SigarUtils.instance.gatherBaseNetDevSet(ipSet);
			int cpuCore = SigarUtils.instance.gatherCpuCoreCount();
			obj.setCpuCoreCount(cpuCore);
			double cpuRate = SigarUtils.instance.gatherCpuRate();
			obj.setCpuRate(cpuRate);
			double memoryRate = SigarUtils.instance.gatherMemoryRate();
			obj.setMemoryRate(memoryRate);
			long memorySize = SigarUtils.instance.gatherMemSize();
			obj.setMemorySize(memorySize);
			Map<Integer,Map<String, Long>> netStatMap = SigarUtils.instance.gatherNetStatInfos(ipDevSet);
			if(netStatMap.containsKey(0)){
				obj.setNetTByteMap(netStatMap.get(0));
			}
			if(netStatMap.containsKey(1)){
				obj.setNetRByteMap(netStatMap.get(1));
			}
			Map<Integer,Map<String,Long>> partition = SigarUtils.instance.gatherPartitionInfo(dataDir);
			if(partition.containsKey(0)){
				obj.setPartitionTotalSizeMap(partition.get(0));
			}
			if(partition.containsKey(1)){
				obj.setPartitionRemainSizeMap(partition.get(1));
			}
			if(partition.containsKey(2)){
				obj.setPartitionReadByteMap(partition.get(2));
			}
			if(partition.containsKey(3)){
				obj.setPartitionWriteByteMap(partition.get(3));
			}
		} catch (SigarException e) {
			e.printStackTrace();
		}
		return obj;
	}
	
	/**
	 * 概述：采集状态信息
	 * @param dataDir
	 * @param ipSet
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static StateMetaServerModel gatherResource(String dataDir, String ip){
		StateMetaServerModel obj = new StateMetaServerModel();
		try {
			if(BrStringUtils.isEmpty(dataDir) || BrStringUtils.isMathNumeric(ip)){
				return null;
			}
			int cpuCore = SigarUtils.instance.gatherCpuCoreCount();
			obj.setCpuCoreCount(cpuCore);
			double cpuRate = SigarUtils.instance.gatherCpuRate();
			obj.setCpuRate(cpuRate);
			double memoryRate = SigarUtils.instance.gatherMemoryRate();
			obj.setMemoryRate(memoryRate);
			long memorySize = SigarUtils.instance.gatherMemSize();
			obj.setMemorySize(memorySize);
			ResourcePair<Long, Long> netData = SigarUtils.instance.gatherNetStatInfos(ip);
			if(netData != null){
				obj.setNetRByte(netData.getKey());
				obj.setNetTByte(netData.getValue());
			}
			Map<Integer,Map<String,Long>> partition = SigarUtils.instance.gatherPartitionInfo(dataDir);
			if(partition.containsKey(0)){
				obj.setPartitionTotalSizeMap(partition.get(0));
			}
			if(partition.containsKey(1)){
				obj.setPartitionRemainSizeMap(partition.get(1));
			}
			if(partition.containsKey(2)){
				obj.setPartitionReadByteMap(partition.get(2));
			}
			if(partition.containsKey(3)){
				obj.setPartitionWriteByteMap(partition.get(3));
			}
		} catch (SigarException e) {
			e.printStackTrace();
		}
		return obj;
	}
	
	/**
	 * 概述：基本信息
	 * @param dataDir
	 * @param ipSet
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static BaseMetaServerModel gatherBase(String serverId, String dataDir){
		BaseMetaServerModel obj = new BaseMetaServerModel();
		try {
			int cpuCore = SigarUtils.instance.gatherCpuCoreCount();
			obj.setCpuCoreCount(cpuCore);
		
			long memorySize = SigarUtils.instance.gatherMemSize();
			obj.setMemoryTotalSize(memorySize);
			Map<Integer,Map<String,Long>> partition = SigarUtils.instance.gatherPartitionInfo(dataDir);
			if(partition.containsKey(0)){
				long totalDiskSize = CalcUtils.collectDataMap(partition.get(0));
				obj.setDiskTotalSize(totalDiskSize);
			}
		} catch (SigarException e) {
			e.printStackTrace();
		}
		return obj;
	}
	/**
	 * 概述：统计原始信息
	 * @param queue
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static List<StatServerModel> calcState(Queue<StateMetaServerModel> queue){
		List<StatServerModel> orderList = new ArrayList<StatServerModel>();
		if(queue == null){
			return orderList;
		}
		int size = queue.size();
		if(size < 2){
			return orderList;
		}
		StatServerModel obj = null;
		StateMetaServerModel pre = null;
		StateMetaServerModel current = null;
		for(int i = 0; i < size; i++){
			if(i == 0 || pre == null){
				pre = queue.poll();
				continue;
			}
			current = queue.poll();
			obj = current.converObject(pre);
			orderList.add(obj);
			pre = current;
		}
		
		return orderList;
	}
	/**
	 * 概述：计算状态信息
	 * @param arrays
	 * @param snList
	 * @param inverTime
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static StatServerModel calcStatServerModel(final List<StatServerModel> arrays, List<String> snList, long inverTime, String dataPath){
		if(arrays == null || arrays.isEmpty()){
			return null;
		}
		StatServerModel obj = null;
		for(StatServerModel tmp : arrays){
			if(obj == null){
				obj = tmp;
				continue;
			}
			obj = tmp.sum(obj);
		}
		obj.calc(snList,  dataPath, inverTime);
		
		Map<String,String> snToDiskMap = matchSnToPatition(snList,obj.getPartitionTotalSizeMap().keySet(),dataPath);
		System.out.println("calcStat : "+snToDiskMap +"--  "+snList +"---"+obj.getPartitionTotalSizeMap().keySet());
		if(snToDiskMap !=null && !snToDiskMap.isEmpty()){
			obj.setStorageNameOnPartitionMap(snToDiskMap);
		}
		return obj;
	}
	/**
	 * 概述：查找最大值
	 * @param source
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static void selectMaxValue(final Collection<StatServerModel> source, BaseMetaServerModel base){
		if(source == null){
			return ;
		}
		for(StatServerModel stat : source){
			long mNetRx = stat.getNetRSpeed();
			if(base.getNetRxMaxSpeed() < mNetRx){
				base.setNetRxMaxSpeed(mNetRx);
			}
			long mNetTx = stat.getNetTSpeed();
			if(base.getNetTxMaxSpeed() < mNetTx){
				base.setNetTxMaxSpeed(mNetTx);
			}
			long mDiskW = CalcUtils.maxDataMap(stat.getPartitionWriteSpeedMap());
			if(base.getDiskWriteMaxSpeed() < mDiskW){
				base.setDiskWriteMaxSpeed(mDiskW);
			}
			long mDiskR = CalcUtils.maxDataMap(stat.getPartitionReadSpeedMap());
			if(base.getDiskReadMaxSpeed() < mDiskR){
				base.setDiskReadMaxSpeed(mDiskR);
			}
		}
	}
	/**
	 * 概述：汇总基础信息
	 * @param source
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static BaseMetaServerModel collectBaseMetaServer(final Collection<BaseMetaServerModel> source){
		BaseMetaServerModel obj = new BaseMetaServerModel();
		if(source == null){
			return obj;
		}
		for(BaseMetaServerModel base : source){
			obj = obj.sum(base);
		}
		return obj;
	}
	/***
	 * 概述：计算resource
	 * @param local
	 * @param cluster
	 * @param stat
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static ResourceModel calcResourceValue(final BaseMetaServerModel cluster, final StatServerModel stat){
		ResourceModel obj = new ResourceModel();
		Map<String,Double> cacheMap = null;
		long cacheNum = 0l;
		double cpuValue = (1 - stat.getCpuRate()) * stat.getCpuCoreCount() / cluster.getCpuCoreCount();
		double memoryValue = (1 - stat.getMemoryRate()) * stat.getMemorySize() / cluster.getMemoryTotalSize();
		double diskRemainRate = (double)stat.getRemainDiskSize()/stat.getTotalDiskSize();
		obj.setCpuRate(stat.getCpuRate());
		obj.setMemoryRate(stat.getMemoryRate());
		obj.setDiskSize(stat.getTotalDiskSize());
		obj.setCpuValue(cpuValue);
		obj.setMemoryValue(memoryValue);
		obj.setDiskRemainRate(diskRemainRate);
		// 磁盘剩余
		cacheNum = cluster.getDiskTotalSize();
		cacheMap = CalcUtils.divDataDoubleMap(stat.getPartitionRemainSizeMap(), cacheNum);
		obj.setDiskRemainValue(cacheMap);
		// 磁盘读
		cacheNum = cluster.getDiskReadMaxSpeed();
		cacheMap = CalcUtils.divDiffDataDoubleMap(stat.getPartitionReadSpeedMap(), cacheNum);
		obj.setDiskReadValue(cacheMap);
		// 磁盘写
		cacheNum = cluster.getDiskWriteMaxSpeed();
		cacheMap = CalcUtils.divDiffDataDoubleMap(stat.getPartitionWriteSpeedMap(), cacheNum);
		obj.setDiskWriteValue(cacheMap);
		// 网卡接收
		cacheNum = cluster.getNetRxMaxSpeed();
		double netRS = stat.getNetRSpeed()/cacheNum;
		obj.setNetRxValue(netRS);
		// 网卡发送
		cacheNum = cluster.getNetTxMaxSpeed();
		double netTS = stat.getNetTSpeed()/cacheNum;
		obj.setNetTxValue(netTS);
		obj.setStorageNameOnPartitionMap(stat.getStorageNameOnPartitionMap());
		return obj;
	}
	
	 /**
     * 概述：匹配sn与分区
     * @param snList sn目录信息
     * @param mountPoints 挂载点目录信息
     * @return
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public static Map<String,String> matchSnToPatition(Collection<String> snList, Collection<String> mountPoints, String dataDir){
    	Map<String, String> objMap = new ConcurrentHashMap<String,String>();
    	if(snList == null || mountPoints == null){
    		return objMap;
    	}
    	// 获取每个sn对应的空间大小
		String mountPoint = null;
		String path = null;
		// 匹配sn与挂载点
		for(String sn : snList){
			path = dataDir +File.separator +sn;
			mountPoint = DiskUtils.selectPartOfDisk(path, mountPoints);
			if(BrStringUtils.isEmpty(mountPoint)){
				continue;
			}
			if(!objMap.containsKey(sn)){
				objMap.put(sn, mountPoint);
			}
		}
		return objMap;
    }
    /***
     * 概述：更新base信息
     * @param serverID
     * @param dataDir
     * @param bZkNode
     * @param zkUrl
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public static void updateBaseInfo(String serverID, String dataDir, String bZkNode,String zkUrl){
		BaseMetaServerModel local = GatherResource.gatherBase(serverID, dataDir);
		byte[] content = JsonUtils.toJsonBytes(local);
		ZookeeperClient client =  CuratorClient.getClientInstance(zkUrl);
		String baseNode = bZkNode + "/"+serverID;
		if(client.checkExists(baseNode)){
			client.setData(baseNode, content);
		}else{
			client.createPersistent(baseNode, true, content);
		}
		client.close();
	}
    /**
     * 概述：获取资源
     * @param zkUrl
     * @param resourcePath
     * @return
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public static List<ResourceModel> getResourceS(final String zkUrl, final String resourcePath){
    	
		List<ResourceModel> dataList = new ArrayList<ResourceModel>();
		if(BrStringUtils.isEmpty(zkUrl)|| BrStringUtils.isEmpty(resourcePath)){
			return dataList;
		}
		ZookeeperClient client = CuratorClient.getClientInstance(zkUrl);
		List<String> baseNodes = client.getChildren(resourcePath);
		if(baseNodes == null || baseNodes.isEmpty()){
			return dataList;
		}
		String pathNode = null;
		byte[] data = null;
		ResourceModel tmpBase = null;
		for(String base : baseNodes){
			pathNode = resourcePath + "/" + base;
			data = client.getData(pathNode);
			if(data == null){
				continue;
			}
			tmpBase = JsonUtils.toObject(data, ResourceModel.class);
			dataList.add(tmpBase);
		}
		client.close();
		return dataList;
	}
    /**
     * 概述：获取基础信息
     * @param zkUrl
     * @param basePath
     * @return
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public static List<BaseMetaServerModel> getClusterBase(final String zkUrl, final String basePath){
    	
		List<BaseMetaServerModel> dataList = new ArrayList<BaseMetaServerModel>();
		if(BrStringUtils.isEmpty(zkUrl) || BrStringUtils.isEmpty(basePath)){
			return dataList;
		}
		ZookeeperClient client = CuratorClient.getClientInstance(zkUrl);
		List<String> baseNodes = client.getChildren(basePath);
		if(baseNodes == null || baseNodes.isEmpty()){
			return dataList;
		}
		String pathNode = null;
		byte[] data = null;
		BaseMetaServerModel tmpBase = null;
		for(String base : baseNodes){
			pathNode = basePath + "/" + base;
			data = client.getData(pathNode);
			if(data == null){
				continue;
			}
			tmpBase = JsonUtils.toObject(data, BaseMetaServerModel.class);
			dataList.add(tmpBase);
		}
		client.close();
		return dataList;
	}
   
    
}
