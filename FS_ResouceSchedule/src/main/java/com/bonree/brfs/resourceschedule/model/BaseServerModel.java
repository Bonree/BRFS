package com.bonree.brfs.resourceschedule.model;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.alibaba.fastjson.JSONObject;
import com.bonree.brfs.resourceschedule.model.enums.CpuEnum;
import com.bonree.brfs.resourceschedule.model.enums.MemoryEnum;
import com.bonree.brfs.resourceschedule.model.enums.PatitionEnum;
import com.bonree.brfs.resourceschedule.model.enums.ServerCommonEnum;

/*******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 *
 * @date 2018-3-7
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * Description: 
 * Version: 机器基本信息
 ******************************************************************************/
public class BaseServerModel extends AbstractResourceModel{
    /**
     * 服务Id
     */
    private int serverId;
    /**
     * cpu内核数
     */
    private int cpuCoreCount;
    /**
     * 内存大小 单位byte
     */
    private long memorySize;
    /**
     * 网卡信息 key：ip地址，value：网卡基本信息
     */
    private Map<String, BaseNetModel> netInfoMap = new ConcurrentHashMap<String, BaseNetModel>();
    /**
     * 文件系统信息 key：挂载点，value：文件系统信息
     */
    private Map<String, BasePatitionModel> patitionInfoMap = new ConcurrentHashMap<String, BasePatitionModel>();
    /**
     * SN与文件系统的映射关系 key：SN名称
     */
    private Map<String,String> snToDiskMap = new ConcurrentHashMap<String, String>();
    /**
     * 总分区大小
     */
    private long totalPatitionSize;
    
    public BaseServerModel(int serverId, int cpuCoreCount, int memorySize) {
        this.serverId = serverId;
        this.cpuCoreCount = cpuCoreCount;
        this.memorySize = memorySize;
    }

    public BaseServerModel() {
    }

    public int getServerId() {
        return serverId;
    }

    public void setServerId(int serverId) {
        this.serverId = serverId;
    }
    public JSONObject toJSONObject(){
    	JSONObject obj = new JSONObject();
    	obj.put(ServerCommonEnum.SERVER_ID.name(), this.serverId);
    	obj.put(CpuEnum.CPU_CORE_COUNT.name(), this.cpuCoreCount);
    	
    	JSONObject netObj = new JSONObject();
    	for(Map.Entry<String, BaseNetModel> netEntry : this.netInfoMap.entrySet()){
    		netObj.put(netEntry.getKey(), netEntry.getValue().toJSONObject());
    	}
    	obj.put(ServerCommonEnum.NET_BASE_INFO.name(), netObj);
    	
    	JSONObject diskObj = new JSONObject();
    	for(Map.Entry<String, BasePatitionModel> diskEntry : this.patitionInfoMap.entrySet()){
    		diskObj.put(diskEntry.getKey(), diskEntry.getValue().toJSONObject());
    	}
    	obj.put(ServerCommonEnum.PATITION_BASE_INFO.name(), diskObj);
    	obj.put(MemoryEnum.MEMORY_SIZE.name(), this.memorySize);
    	return obj;
    }
    public int getCpuCoreCount() {
        return cpuCoreCount;
    }

    public void setCpuCoreCount(int cpuCoreCount) {
        this.cpuCoreCount = cpuCoreCount;
    }

    public long getMemorySize() {
        return memorySize;
    }

    public void setMemorySize(long memorySize) {
        this.memorySize = memorySize;
    }

    public Map<String, BaseNetModel> getNetInfoMap() {
        return netInfoMap;
    }

    public void setNetInfoMap(Map<String, BaseNetModel> netInfoMap) {
        this.netInfoMap = netInfoMap;
    }

    public Map<String, BasePatitionModel> getPatitionInfoMap() {
        return patitionInfoMap;
    }

    public void setPatitionInfoMap(Map<String, BasePatitionModel> patitionInfoMap) {
        this.patitionInfoMap = patitionInfoMap;
    }
    public void putNetInfo(String ipAddress, BaseNetModel netInfo){
        this.netInfoMap.put(ipAddress, netInfo);
    }
    public void putPatitionInfo(String mountePoint, BasePatitionModel patitionInfo){
        this.patitionInfoMap.put(mountePoint, patitionInfo);
    }

	public Map<String, String> getSnToDiskMap() {
		return snToDiskMap;
	}

	public void setSnToDiskMap(Map<String, String> snToDiskMap) {
		this.snToDiskMap = snToDiskMap;
	}

	public long getTotalPatitionSize() {
		return totalPatitionSize;
	}

	public void setTotalPatitionSize(long totalPatitionSize) {
		this.totalPatitionSize = totalPatitionSize;
	}
	
}
