package com.bonree.brfs.resourceschedule.model;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.alibaba.fastjson.JSONObject;
import com.bonree.brfs.resourceschedule.model.ServerEnum.MEMORY_ENUM;
import com.bonree.brfs.resourceschedule.model.ServerEnum.SERVER_COMMON_ENUM;

/*******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 *
 * @date 2018-3-7
 * @author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * Description: 
 * Version: 
 ******************************************************************************/
public class ServerStatModel {
    /**
     * cpu状态信息
     */
    private CpuStatModel cpuStatInfo;
    /**
     * 内存状态信息
     */
    private MemoryStatModel memoryStatInfo;
    /**
     * 网卡状态信息 key：ip地址，value：网卡状态
     */
    private Map<String,NetStatModel> netStatInfoMap = new ConcurrentHashMap<String,NetStatModel>();
    /**
     * 文件系统状态信息 key：挂载点，value：文件系统状态
     */
    private Map<String,PatitionStatModel> patitionStatInfoMap = new ConcurrentHashMap<String,PatitionStatModel>();

    public ServerStatModel() {
    }
    public JSONObject toJSONObject(){
    	JSONObject obj = new JSONObject();
    	obj.put(SERVER_COMMON_ENUM.CPU_STAT_INFO.name(), this.cpuStatInfo.toJSONObject());
    	obj.put(SERVER_COMMON_ENUM.MEMORY_STAT_INFO.name(), this.memoryStatInfo.toJSONObject());
    	
    	JSONObject netObj = new JSONObject();
    	for(Map.Entry<String, NetStatModel> netEntry : this.netStatInfoMap.entrySet()){
    		netObj.put(netEntry.getKey(), netEntry.getValue().toJSONObject());
    	}
    	obj.put(SERVER_COMMON_ENUM.NET_STAT_INFO.name(), netObj);
    	
    	JSONObject diskObj = new JSONObject();
    	for(Map.Entry<String, PatitionStatModel> diskEntry : this.patitionStatInfoMap.entrySet()){
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

    public CpuStatModel getCpuStatInfo() {
        return cpuStatInfo;
    }

    public void setCpuStatInfo(CpuStatModel cpuStatInfo) {
        this.cpuStatInfo = cpuStatInfo;
    }

    public MemoryStatModel getMemoryStatInfo() {
        return memoryStatInfo;
    }

    public void setMemoryStatInfo(MemoryStatModel memoryStatInfo) {
        this.memoryStatInfo = memoryStatInfo;
    }

    public Map<String, NetStatModel> getNetStatInfoMap() {
        return netStatInfoMap;
    }

    public void setNetStatInfoMap(Map<String, NetStatModel> netStatInfoMap) {
        this.netStatInfoMap = netStatInfoMap;
    }

    public Map<String, PatitionStatModel> getPatitionStatInfoMap() {
        return patitionStatInfoMap;
    }

    public void setPatitionStatInfoMap(Map<String, PatitionStatModel> patitionStatInfoMap) {
        this.patitionStatInfoMap = patitionStatInfoMap;
    }
    public void putNetStatInfo(String ipAddress, NetStatModel netStatInfo){
        this.netStatInfoMap.put(ipAddress, netStatInfo);
    }
    public void putPatitionStatInfo(String mountPoint, PatitionStatModel patitionStatInfo){
        this.patitionStatInfoMap.put(mountPoint, patitionStatInfo);
    }
}
