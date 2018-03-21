package com.bonree.brfs.resourceschedule.commons;

import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.hyperic.sigar.SigarException;

import com.bonree.brfs.resourceschedule.model.BaseNetModel;
import com.bonree.brfs.resourceschedule.model.BasePatitionModel;
import com.bonree.brfs.resourceschedule.model.BaseServerModel;
import com.bonree.brfs.resourceschedule.model.NetStatModel;
import com.bonree.brfs.resourceschedule.model.PatitionStatModel;
import com.bonree.brfs.resourceschedule.model.ResourceModel;
import com.bonree.brfs.resourceschedule.model.ServerModel;
import com.bonree.brfs.resourceschedule.model.ServerStatModel;
import com.bonree.brfs.resourceschedule.utils.DiskUtils;
import com.bonree.brfs.resourceschedule.utils.OSCheckUtils;
import com.bonree.brfs.resourceschedule.utils.SigarUtils;
import com.bonree.brfs.resourceschedule.utils.StringUtils;

public class GatherResource {
	
    /**
     * 概述：采集服务基本信息
     * @param serverId
     * @param dataPath
     * @return
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public static BaseServerModel gatherBaseServerInfo(Cache cache){
		BaseServerModel obj = new BaseServerModel();
		Map<String, BaseNetModel> netMap = null;
		Map<String, BasePatitionModel> patitionMap = null;
		Map<String, String> snToPatitionMap = new ConcurrentHashMap<String,String>();
		Map<String, Long> snSizeMap = new ConcurrentHashMap<String, Long>();
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
				// 获取每个sn对应的空间大小
				String mountPoint = null;
				// 获取分区所有的挂载点
				Set<String> mountPath = patitionMap.keySet();
				
				for(String sn : cache.snList){
					mountPoint = DiskUtils.selectPartOfDisk(sn, mountPath);
					if(StringUtils.isEmpty(mountPoint)){
						continue;
					}
					if(!snToPatitionMap.containsKey(sn)){
						snToPatitionMap.put(sn, mountPoint);
					}
					if(!snSizeMap.containsKey(sn) && patitionMap.get(mountPoint) != null){
						snSizeMap.put(sn, patitionMap.get(mountPoint).getPatitionSize());
					}
				}
				obj.setSnSizeMap(snSizeMap);
				obj.setSnToDiskMap(snToPatitionMap);
			}
			
		}
		catch (SigarException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return obj;
	}
    /**
     * 概述：采集状态信息
     * @param dataPath
     * @return
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
	public static ServerStatModel gatherServerStatInfo(Cache cache){
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
				// 获取每个sn对应的空间大小
				String mountPoint = null;
				// 获取分区所有的挂载点
				Set<String> mountPath = patitionMap.keySet();
				
				for(String sn : cache.snList){
					mountPoint = DiskUtils.selectPartOfDisk(sn, mountPath);
					if(StringUtils.isEmpty(mountPoint)){
						continue;
					}
					if(!snToPatitionMap.containsKey(sn)){
						snToPatitionMap.put(sn, mountPoint);
					}
				}
				obj.setSnToDiskMap(snToPatitionMap);
			}
		}
		catch (SigarException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return obj;
	}
	/**
	 * 概述：计算资源
	 * @param server
	 * @param clusterList
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static ResourceModel calcResourceModel(ServerModel server, List<BaseServerModel> clusterList){
		Queue<ServerStatModel> tmpqueue = server.getStatInfoQueue();
		if(tmpqueue.size() <2){
			return null;
		}
		// 
		ResourceModel resource = new ResourceModel();
		Map<String,Double> snWrite = new ConcurrentHashMap<String,Double>();
		//汇总基础信息
		int cpuCoreCount = 0;
		long memorySize = 0;
		Map<String,Long> snSizeMap = new ConcurrentHashMap<String,Long>();
		Map<String,Long> tmpSnSizeMap = null;
		for(BaseServerModel base : clusterList){
			cpuCoreCount += base.getCpuCoreCount();
			memorySize += base.getMemorySize();
			tmpSnSizeMap = sumMapData(tmpSnSizeMap, base.getSnSizeMap());
		}
		
		return null;
		
	}
	/**
	 * 概述：汇总map数据
	 * @param source
	 * @param dent
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static Map<String,Long> sumMapData(Map<String,Long> source, Map<String,Long> dent){
		Map<String,Long> resource = new ConcurrentHashMap<String, Long>();
		if(source == null && dent == null){
			return null;
		}
		if(source == null){
			resource.putAll(dent);
			return resource;
		}
		if(dent == null){
			resource.putAll(source);
			return resource;
		}
		Set<String> keys = new HashSet<String>();
		keys.addAll(source.keySet());
		keys.addAll(dent.keySet());
		for(String key : keys){
			if(source.containsKey(key) && dent.containsKey(key)){
				resource.put(key, source.get(key) + dent.get(key));
			}
			if(source.containsKey(key) ){
				resource.put(key, source.get(key));
			}
			if(dent.containsKey(key)){
				resource.put(key, dent.get(key));
			}
		}
		return resource;
	}
	/**
	 * 概述：减去map source - dent
	 * @param source
	 * @param dent
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static Map<String,Long> diffMapData(Map<String,Long> source, Map<String,Long> dent){
		Map<String,Long> resource = new ConcurrentHashMap<String, Long>();
		if(source == null && dent == null){
			return null;
		}
		if(source == null){
			resource.putAll(dent);
			return resource;
		}
		if(dent == null){
			resource.putAll(source);
			return resource;
		}
		Set<String> keys = new HashSet<String>();
		keys.addAll(source.keySet());
		keys.addAll(dent.keySet());
		for(String key : keys){
			if(source.containsKey(key) && dent.containsKey(key)){
				resource.put(key, source.get(key) - dent.get(key));
			}
			if(source.containsKey(key) ){
				resource.put(key, source.get(key));
			}
			if(dent.containsKey(key)){
				resource.put(key, 0 - dent.get(key));
			}
		}
		return resource;
	}
	/**
	 * 概述：汇总集群
	 * @param clusterList
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static BaseServerModel sumClusterBase(List<BaseServerModel> clusterList){
		BaseServerModel obj = new BaseServerModel();
		for(BaseServerModel tmp : clusterList){
			obj.setCpuCoreCount(tmp.getCpuCoreCount() + obj.getCpuCoreCount());
			obj.setMemorySize(tmp.getMemorySize() + obj.getMemorySize());
		}
		return obj;
	}
}
