package com.bonree.brfs.resourceschedule.commons.impl;

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

import com.bonree.brfs.resourceschedule.commons.Cache;
import com.bonree.brfs.resourceschedule.commons.CommonMapCalcInterface;
import com.bonree.brfs.resourceschedule.model.BaseNetModel;
import com.bonree.brfs.resourceschedule.model.BasePatitionModel;
import com.bonree.brfs.resourceschedule.model.BaseServerModel;
import com.bonree.brfs.resourceschedule.model.CpuStatModel;
import com.bonree.brfs.resourceschedule.model.MemoryStatModel;
import com.bonree.brfs.resourceschedule.model.NetStatModel;
import com.bonree.brfs.resourceschedule.model.PatitionStatModel;
import com.bonree.brfs.resourceschedule.model.ResourceModel;
import com.bonree.brfs.resourceschedule.model.ServerStatModel;
import com.bonree.brfs.resourceschedule.model.enums.MemoryEnum;
import com.bonree.brfs.resourceschedule.utils.DiskUtils;
import com.bonree.brfs.resourceschedule.utils.OSCheckUtils;
import com.bonree.brfs.resourceschedule.utils.SigarUtils;
import com.bonree.brfs.resourceschedule.utils.StringUtils;
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
     * 概述：采集服务基本信息
     * @param serverId
     * @param dataPath
     * @return
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public static void gatherBaseServerInfo(Cache cache){
		BaseServerModel obj = new BaseServerModel();
		Map<String, BaseNetModel> netMap = null;
		Map<String, BasePatitionModel> patitionMap = null;
		Map<String, String> snToPatitionMap = null;
		try {
			netMap = SigarUtils.instance.gatherBaseNetInfos(cache);
			patitionMap = SigarUtils.instance.gatherBasePatitionInfos(cache);
			obj.setCpuCoreCount(SigarUtils.instance.gatherCpuCoreCount());
			obj.setMemorySize(SigarUtils.instance.gatherMemSize());
			obj.setServerId(cache.SERVER_ID);
			if(netMap != null){
				obj.setNetInfoMap(netMap);
			}
			if(patitionMap != null){
				obj.setPatitionInfoMap(patitionMap);
				//确定sn与patition的对应关系
				 cache.snWithDisk = matchSnToPatition(cache.snList, patitionMap.keySet());
				
				//统计磁盘空间大小
				obj.setTotalPatitionSize(calcTotalPatitionSize(patitionMap.values()));
			}
			
		} catch (SigarException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		cache.baseInfo = obj;
	}
    /**
     * 概述：采集状态信息
     * @param dataPath
     * @return
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
	public static void gatherServerStatInfo(Cache cache){
		ServerStatModel obj = new ServerStatModel();
		Map<String,NetStatModel> netMap = null;
		Map<String, PatitionStatModel> patitionMap = null;
		Map<String, String> snToPatitionMap = new ConcurrentHashMap<String,String>();
		try {
			netMap = SigarUtils.instance.gatherNetStatInfos();
			patitionMap = SigarUtils.instance.gatherPatitionStatInfos(cache);
			obj.setCpuStatInfo(SigarUtils.instance.gatherCpuStatInfo());
			obj.setMemoryStatInfo(SigarUtils.instance.gatherMemoryStatInfo());
			if(netMap != null){
				obj.setNetStatInfoMap(netMap);
			}
			if(patitionMap != null){
				obj.setPatitionStatInfoMap(patitionMap);
				if(cache.snWithDisk !=null && !cache.snWithDisk.isEmpty()){
					cache.snRemainSizeMap = matchSnRemainSizeMap(cache.snWithDisk,patitionMap);
				}
			}
		} catch (SigarException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		cache.statInfoQueue.add(obj);
	}
	 /**
     * 概述：汇总状态信息
     * @param cache
     * @return
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public static ServerStatModel sumServerStatus(Cache cache){
    	Queue<ServerStatModel> tmpqueue = cache.statInfoQueue;
		if(tmpqueue.size() <2){
			return null;
		}
		// 汇总状态信息
		int size = tmpqueue.size();
		ServerStatModel prex = null;
		ServerStatModel current = null;
		CpuStatModel cpuObj = new CpuStatModel();
		MemoryStatModel memObj = new MemoryStatModel();
		// 磁盘IO率集合
		Map<String,List<PatitionStatModel>> patMap = null;
		// 网卡IO率集合
		Map<String,List<NetStatModel>> netMap = null;
		CommonMapCalcInterface<String, NetStatModel> netInstance = new MapModelCalc<String, NetStatModel>();
		CommonMapCalcInterface<String, PatitionStatModel> patInstance = new MapModelCalc<String, PatitionStatModel>();
		for(int i = 0; i < size; i++){
			current = tmpqueue.poll();
			// 1.统计cpu状态
			cpuObj = cpuObj.calc(current.getCpuStatInfo());
			// 2.统计内存状态
			memObj = memObj.calc(current.getMemoryStatInfo());
			if(i == 0){
				prex = current;
				continue;
			}
			// 3.统计网卡IO
			netMap = netInstance.collectModels(netMap, netInstance.calcMapData(current.getNetStatInfoMap(), prex.getNetStatInfoMap()));
			// 4.统计磁盘IO
			patMap = patInstance.collectModels(patMap, patInstance.calcMapData(current.getPatitionStatInfoMap(), prex.getPatitionStatInfoMap()));
		}
		Map<String,NetStatModel> netResult = netInstance.sumMapData(netMap);
		Map<String,PatitionStatModel> patResult = patInstance.sumMapData(patMap);
		ServerStatModel obj = new ServerStatModel();
		obj.setCpuStatInfo(cpuObj);
		obj.setMemoryStatInfo(memObj);
		obj.setNetStatInfoMap(netResult);
		obj.setPatitionStatInfoMap(patResult);
		obj.setSnRemainSizeMap(cache.snRemainSizeMap);
		return obj;
    }
    /**
     * 概述：统计服务节点磁盘空间大小
     * @param collection
     * @return
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    private static long calcTotalPatitionSize(Collection<BasePatitionModel> collection){
    	BasePatitionModel obj = null;
    	for(BasePatitionModel tmp : collection){
    		obj = tmp.calc(obj);
    	}
    	return obj.getPatitionSize();
    }
    /**
     * 概述：匹配sn与分区
     * @param snList sn目录信息
     * @param mountPoints 挂载点目录信息
     * @return
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    private static Map<String,String> matchSnToPatition(List<String> snList, Set<String> mountPoints){
    	Map<String, String> objMap = new ConcurrentHashMap<String,String>();
    	if(snList == null || mountPoints == null){
    		return objMap;
    	}
    	// 获取每个sn对应的空间大小
		String mountPoint = null;
		// 匹配sn与挂载点
		for(String sn : snList){
			mountPoint = DiskUtils.selectPartOfDisk(sn, mountPoints);
			if(StringUtils.isEmpty(mountPoint)){
				continue;
			}
			if(!objMap.containsKey(sn)){
				objMap.put(sn, mountPoint);
			}
		}
		return objMap;
    }
    /**
     * 概述：获取sn对应磁盘的剩余空间
     * @param snMap
     * @param pMap
     * @return
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    private static Map<String,Long> matchSnRemainSizeMap(Map<String,String> snMap, Map<String, PatitionStatModel> pMap){
    	Map<String, Long> objMap = new ConcurrentHashMap<String, Long>();
    	if(snMap == null||pMap==null || snMap.isEmpty()){
    		return objMap;
    	}
    	String sn = null;
    	String mountPoint = null;
    	for(Map.Entry<String, String> entry : snMap.entrySet()){
    		sn = entry.getKey();
    		mountPoint = entry.getValue();
    		// 过滤无效的sn对应关系
    		if(StringUtils.isEmpty(sn) || StringUtils.isEmpty(mountPoint)){
    			continue;
    		}
    		// 过滤不存在的挂载点
    		if(!pMap.containsKey(mountPoint) || pMap.get(mountPoint) == null){
    			continue;
    		}
    		if(!objMap.containsKey(sn)){
    			objMap.put(sn, pMap.get(mountPoint).getRemainSize());
    		}
    		
    	}
    	
    	return objMap;
    }
    /**
     * 概述：汇总集群基础信息
     * @param clusterList
     * @return
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
	public static BaseServerModel sumBaseClusterModel(int localServerId,List<BaseServerModel> clusterList){
		
		//汇总基础信息
		int cpuCoreCount = 0;
		long memorySize = 0;
		long totalDiskSize = 0;		
		BaseServerModel baseCluster = new BaseServerModel();
		baseCluster.setServerId(localServerId);
		for(BaseServerModel base : clusterList){
			cpuCoreCount += base.getCpuCoreCount();
			memorySize += base.getMemorySize();
			totalDiskSize += base.getTotalPatitionSize();
		}
		baseCluster.setCpuCoreCount(cpuCoreCount);
		baseCluster.setMemorySize(memorySize);
		baseCluster.setTotalPatitionSize(totalDiskSize);
		return baseCluster;
	}
	/***
	 * 概述：计算本机可用指标
	 * @param base
	 * @param stat
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public ResourceModel calcResource(BaseServerModel base, ServerStatModel stat) {
		ResourceModel obj = new ResourceModel();
		obj.setServerId(base.getServerId());
		
		Map<String,PatitionStatModel>  patMap = stat.getPatitionStatInfoMap();
		Map<String,String> snToPat = base.getSnToDiskMap();
		long totalDisk = base.getTotalPatitionSize();

		long remain = 0;
		String sn = null;
		String mount = null;
		PatitionStatModel pat = null;
		NetStatModel net = null;
		for(Map.Entry<String, Long> entry : stat.getSnRemainSizeMap().entrySet()){	
			sn = entry.getKey();
			remain = entry.getValue();
			mount = snToPat.get(sn);
			// 没有挂载点的数据为最低
			if(mount == null){
				obj.putClientSNRead(sn, 0.0);
				obj.putClientSNWrite(sn, 0.0);
				continue;
			}
			pat = patMap.get(mount);
			//无磁盘状态信息的数据为最低
			if(pat == null){
				obj.putClientSNRead(sn, 0.0);
				obj.putClientSNWrite(sn, 0.0);
				continue;
			}
			// 1.计算写请求 磁盘剩余率 +I/O请求
			double wValue = (double) remain/totalDisk + (pat.getWriteMaxSpeed() - pat.getWriteSpeed()/pat.getCount())/pat.getWriteMaxSpeed();
			double rValue = (double) (pat.getReadMaxSpeed() - pat.getReadSpeed()/pat.getCount())/pat.getReadMaxSpeed();
			double remainValue = (double) remain/(pat.getRemainSize() + pat.getUsedSize());
			obj.putClientSNRead(sn, rValue);
			obj.putClientSNWrite(sn, wValue);
			obj.putSnRemainRate(sn, remainValue);
		}
		
		return null;
	}
}
