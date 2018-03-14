package com.bonree.brfs.resouceschedule.vo;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.alibaba.fastjson.JSONObject;
import com.bonree.brfs.resouceschedule.vo.ServerEnum.MEMORY_ENUM;
import com.bonree.brfs.resouceschedule.vo.ServerEnum.SERVER_COMMON_ENUM;

/*******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 *
 * @date 2018-3-7
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * Description: 
 * Version: 
 ******************************************************************************/
public class ServerStatInfo {
    /**
     * cpu状态信息
     */
    private CpuStatInfo cpuStatInfo;
    /**
     * 内存状态信息
     */
    private MemoryStatInfo memoryStatInfo;
    /**
     * 网卡状态信息 key：ip地址，value：网卡状态
     */
    private Map<String,NetStatInfo> netStatInfoMap = new ConcurrentHashMap<String,NetStatInfo>();
    /**
     * 文件系统状态信息 key：挂载点，value：文件系统状态
     */
    private Map<String,PatitionStatInfo> patitionStatInfoMap = new ConcurrentHashMap<String,PatitionStatInfo>();

    public ServerStatInfo() {
    }
    public JSONObject toJSONObject(){
    	JSONObject obj = new JSONObject();
    	obj.put(SERVER_COMMON_ENUM.CPU_STAT_INFO.name(), this.cpuStatInfo.toJSONObject());
    	obj.put(SERVER_COMMON_ENUM.MEMORY_STAT_INFO.name(), this.memoryStatInfo.toJSONObject());
    	
    	JSONObject netObj = new JSONObject();
    	for(Map.Entry<String, NetStatInfo> netEntry : this.netStatInfoMap.entrySet()){
    		netObj.put(netEntry.getKey(), netEntry.getValue().toJSONObject());
    	}
    	obj.put(SERVER_COMMON_ENUM.NET_STAT_INFO.name(), netObj);
    	
    	JSONObject diskObj = new JSONObject();
    	for(Map.Entry<String, PatitionStatInfo> diskEntry : this.patitionStatInfoMap.entrySet()){
    		diskObj.put(diskEntry.getKey(), diskEntry.getValue().toJSONObject());
    	}
    	obj.put(SERVER_COMMON_ENUM.PATITION_STAT_INFO.name(), diskObj);
    	
    	return obj;
    }
    public String toString(){
    	return toJSONObject().toString();
    }
    public String toJSONString(){
    	return toJSONObject().toJSONString();
    }

    public CpuStatInfo getCpuStatInfo() {
        return cpuStatInfo;
    }

    public void setCpuStatInfo(CpuStatInfo cpuStatInfo) {
        this.cpuStatInfo = cpuStatInfo;
    }

    public MemoryStatInfo getMemoryStatInfo() {
        return memoryStatInfo;
    }

    public void setMemoryStatInfo(MemoryStatInfo memoryStatInfo) {
        this.memoryStatInfo = memoryStatInfo;
    }

    public Map<String, NetStatInfo> getNetStatInfoMap() {
        return netStatInfoMap;
    }

    public void setNetStatInfoMap(Map<String, NetStatInfo> netStatInfoMap) {
        this.netStatInfoMap = netStatInfoMap;
    }

    public Map<String, PatitionStatInfo> getPatitionStatInfoMap() {
        return patitionStatInfoMap;
    }

    public void setPatitionStatInfoMap(Map<String, PatitionStatInfo> patitionStatInfoMap) {
        this.patitionStatInfoMap = patitionStatInfoMap;
    }
    public void putNetStatInfo(String ipAddress, NetStatInfo netStatInfo){
        this.netStatInfoMap.put(ipAddress, netStatInfo);
    }
    public void putPatitionStatInfo(String mountPoint, PatitionStatInfo patitionStatInfo){
        this.patitionStatInfoMap.put(mountPoint, patitionStatInfo);
    }
}
