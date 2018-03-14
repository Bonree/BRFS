package com.bonree.brfs.resouceschedule.vo;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.alibaba.fastjson.JSONObject;
import com.bonree.brfs.resouceschedule.vo.ServerEnum.CPU_ENUM;
import com.bonree.brfs.resouceschedule.vo.ServerEnum.MEMORY_ENUM;
import com.bonree.brfs.resouceschedule.vo.ServerEnum.PATITION_ENUM;
import com.bonree.brfs.resouceschedule.vo.ServerEnum.SERVER_COMMON_ENUM;

/*******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 *
 * @date 2018-3-7
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * Description: 
 * Version: 机器基本信息
 ******************************************************************************/
public class BaseServerInfo {
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
    private Map<String, BaseNetInfo> netInfoMap = new ConcurrentHashMap<String, BaseNetInfo>();
    /**
     * 文件系统信息 key：挂载点，value：文件系统信息
     */
    private Map<String, BasePatitionInfo> patitionInfoMap = new ConcurrentHashMap<String, BasePatitionInfo>();

    public BaseServerInfo(int serverId, int cpuCoreCount, int memorySize) {
        this.serverId = serverId;
        this.cpuCoreCount = cpuCoreCount;
        this.memorySize = memorySize;
    }

    public BaseServerInfo() {
    }

    public int getServerId() {
        return serverId;
    }

    public void setServerId(int serverId) {
        this.serverId = serverId;
    }
    public JSONObject toJSONObject(){
    	JSONObject obj = new JSONObject();
    	obj.put(SERVER_COMMON_ENUM.SERVER_ID.name(), this.serverId);
    	obj.put(CPU_ENUM.CPU_CORE_COUNT.name(), this.cpuCoreCount);
    	
    	JSONObject netObj = new JSONObject();
    	for(Map.Entry<String, BaseNetInfo> netEntry : this.netInfoMap.entrySet()){
    		netObj.put(netEntry.getKey(), netEntry.getValue().toJSONObject());
    	}
    	obj.put(SERVER_COMMON_ENUM.NET_BASE_INFO.name(), netObj);
    	
    	JSONObject diskObj = new JSONObject();
    	for(Map.Entry<String, BasePatitionInfo> diskEntry : this.patitionInfoMap.entrySet()){
    		diskObj.put(diskEntry.getKey(), diskEntry.getValue().toJSONObject());
    	}
    	obj.put(SERVER_COMMON_ENUM.PATITION_BASE_INFO.name(), diskObj);
    	obj.put(MEMORY_ENUM.MEMORY_SIZE.name(), this.memorySize);
    	return obj;
    }
    public String toString(){
    	return toJSONObject().toString();
    }
    public String toJSONString(){
    	return toJSONObject().toJSONString();
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

    public Map<String, BaseNetInfo> getNetInfoMap() {
        return netInfoMap;
    }

    public void setNetInfoMap(Map<String, BaseNetInfo> netInfoMap) {
        this.netInfoMap = netInfoMap;
    }

    public Map<String, BasePatitionInfo> getPatitionInfoMap() {
        return patitionInfoMap;
    }

    public void setPatitionInfoMap(Map<String, BasePatitionInfo> patitionInfoMap) {
        this.patitionInfoMap = patitionInfoMap;
    }
    public void putNetInfo(String ipAddress, BaseNetInfo netInfo){
        this.netInfoMap.put(ipAddress, netInfo);
    }
    public void putPatitionInfo(String mountePoint, BasePatitionInfo patitionInfo){
        this.patitionInfoMap.put(mountePoint, patitionInfo);
    }
}
