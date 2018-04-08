package com.bonree.brfs.resourceschedule.commons;

import java.io.File;
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

import com.bonree.brfs.resourceschedule.model.BaseMetaServerModel;
import com.bonree.brfs.resourceschedule.model.ResourceModel;
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
	 * 概述：基本信息
	 * @param dataDir
	 * @param ipSet
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static BaseMetaServerModel gatherBase(String serverId, String dataDir, Collection<String> ipSet){
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
	public static StatServerModel calcStatServerModel(final List<StatServerModel> arrays, List<String> snList, long inverTime){
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
		obj.calc(snList, inverTime);
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
			long mNetRx = CalcUtils.maxDataMap(stat.getNetRSpeedMap());
			if(base.getNetRxMaxSpeed() < mNetRx){
				base.setNetRxMaxSpeed(mNetRx);
			}
			long mNetTx = CalcUtils.maxDataMap(stat.getNetTSpeedMap());
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
	 * 概述：
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
		double cpuRate = stat.getCpuRate() * stat.getCpuCoreCount() / cluster.getCpuCoreCount();
		double memoryRate = stat.getMemoryRate() * stat.getMemorySize() / cluster.getMemoryTotalSize();
		double diskRemainRate = stat.getRemainDiskSize()/stat.getTotalDiskSize();
		obj.setCpuValue(cpuRate);
		obj.setMemoryValue(memoryRate);
		obj.setDiskRemainRate(diskRemainRate);
		// 磁盘剩余
		cacheNum = cluster.getDiskTotalSize();
		cacheMap = CalcUtils.divDataDoubleMap(stat.getPartitionRemainSizeMap(), cacheNum);
		obj.setDiskRemainValue(cacheMap);
		// 磁盘读
		cacheNum = cluster.getDiskReadMaxSpeed();
		cacheMap = CalcUtils.divDataDoubleMap(stat.getPartitionReadSpeedMap(), cacheNum);
		obj.setDiskReadValue(cacheMap);
		// 磁盘写
		cacheNum = cluster.getDiskWriteMaxSpeed();
		cacheMap = CalcUtils.divDataDoubleMap(stat.getPartitionWriteSpeedMap(), cacheNum);
		obj.setDiskWriteValue(cacheMap);
		// 网卡接收
		cacheNum = cluster.getNetRxMaxSpeed();
		cacheMap = CalcUtils.divDataDoubleMap(stat.getNetRSpeedMap(), cacheNum);
		obj.setNetRxValue(cacheMap);
		// 网卡发送
		cacheNum = cluster.getNetTxMaxSpeed();
		cacheMap = CalcUtils.divDataDoubleMap(stat.getNetTSpeedMap(), cacheNum);
		obj.setNetTxValue(cacheMap);
		return obj;
	}
	
}
