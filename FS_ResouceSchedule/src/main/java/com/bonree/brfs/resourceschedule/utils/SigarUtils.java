package com.bonree.brfs.resourceschedule.utils;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.hyperic.sigar.Cpu;
import org.hyperic.sigar.FileSystem;
import org.hyperic.sigar.FileSystemUsage;
import org.hyperic.sigar.Mem;
import org.hyperic.sigar.NetInterfaceConfig;
import org.hyperic.sigar.NetInterfaceStat;
import org.hyperic.sigar.Sigar;
import org.hyperic.sigar.SigarException;

import com.bonree.brfs.common.utils.BrStringUtils;

/*****************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年3月8日 下午3:00:31
 * @Author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @Description: 资源采集工具
 *****************************************************************************
 */
public enum SigarUtils {
	instance;
    private Sigar sigar = new Sigar();
    /**
     * 概述：获取cpu核心数
     * @return 
     * @throws SigarException
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public int gatherCpuCoreCount()throws SigarException{
        return new Sigar().getCpuInfoList().length;
    }
    /**
     * 概述：获取内存大小
     * @return
     * @throws SigarException
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public long gatherMemSize() throws SigarException {
        Mem mem = sigar.getMem();
        return mem.getTotal();
    }
    
    /**
     * 概述：采集cpus使用率
     * 比较特殊的是CPU总使用率的计算(util),目前的算法是: util = 1 - idle - iowait - steal
     * @return
     * @throws SigarException
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public double gatherCpuRate() throws SigarException {
        Cpu cpu = sigar.getCpu();
        long userTime = cpu.getUser();
        long sysTime = cpu.getSys();
        long niceTime = cpu.getNice();
        long idleTime = cpu.getIdle();
        long iowaitTime = cpu.getWait();
        long irqTime = cpu.getIrq();
        long softirqTime = cpu.getSoftIrq();
        long stlTime = cpu.getStolen();
        long cpuTotalTime = userTime + sysTime+niceTime+idleTime+iowaitTime+irqTime+softirqTime+stlTime;
        double idleRate = (double) idleTime/cpuTotalTime;
        double iowaitRate = (double) iowaitTime /cpuTotalTime;
        double stlRate = (double) stlTime/cpuTotalTime;
        return (1 - (idleRate+iowaitRate+stlRate));
    }
    
    /**
     * 概述：采集内存使用率
     * util = (total - free - buff - cache) / total
     * @return
     * @throws SigarException
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public double gatherMemoryRate() throws SigarException {
        Mem mem = sigar.getMem();
        long  actUse = mem.getActualUsed();
        long total = mem.getTotal();
        double usageRate = (double) actUse / total;
        return usageRate;
    }
    
    /**
     * 概述：获取配置ip的信息
     * @param ipSet
     * @return 设备名称
     * @throws SigarException
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public Set<String> gatherBaseNetDevSet(Collection<String> ipSet) throws SigarException{
    	Set<String> DevSet = new HashSet<String>();
    	if(ipSet == null || ipSet.isEmpty()){
    		return DevSet;
    	}
    	String[] netInfos = sigar.getNetInterfaceList();
    	NetInterfaceConfig netConfig = null;
    	String tmpIp = null;
    	for(String netInfo : netInfos){
    		netConfig = sigar.getNetInterfaceConfig(netInfo);
    		tmpIp = netConfig.getAddress();    	
    		// 1.过滤非法的ip
    		if(NetUtils.filterIp(tmpIp)){
    			continue;
    		}
    		// 2.过滤网卡不存在的
    		if(((netConfig.getFlags() & 1L) <= 0L)){
    			continue;
    		}
    		if(ipSet.contains(tmpIp)){
    			ipSet.add(netInfo);
    		}
    		break;
    	}
    	return DevSet;
    }
    
    /**
     * 概述：采集网卡状态信息
     * @return key 0-发送字节数，1-接收字节数
     * @throws SigarException
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public Map<Integer,Map<String, Long>> gatherNetStatInfos(Collection<String> ipDevSet) throws SigarException{
    	Map<Integer,Map<String, Long>> objMap = new ConcurrentHashMap<Integer,Map<String, Long>>();
    	if(ipDevSet == null || ipDevSet.isEmpty()){
    		return objMap;
    	}
    	NetInterfaceConfig netConfig = null;
    	NetInterfaceStat  netStat = null;
    	String tmpIp = null;
    	String devName = null;
    	for(String netInfo : ipDevSet){
    		netConfig = sigar.getNetInterfaceConfig(netInfo);
    		tmpIp = netConfig.getAddress();
    		devName = netConfig.getName();
    		// 1.过滤网卡不存在的
    		if(((netConfig.getFlags() & 1L) <= 0L)){
    			continue;
    		}
    		
    		netStat = sigar.getNetInterfaceStat(netInfo);
    		 addDataToMap(objMap,0,tmpIp,netStat.getTxBytes());
             addDataToMap(objMap,1,tmpIp,netStat.getRxBytes());
    	}
    	return objMap;
    }
    
    /**
     * 概述：采集分区信息
     * @param rootPath
     * @return key：0-分区大小，1-分区可用大小，2-硬盘读取kb数, 3-硬盘写入kb数
     * @throws SigarException
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public Map<Integer,Map<String,Long>> gatherPartitionInfo(String rootPath) throws SigarException {
        Map<Integer,Map<String,Long>> objMap = new ConcurrentHashMap<Integer, Map<String, Long>>();
        if(BrStringUtils.isEmpty(rootPath)){
            return objMap;
        }
        File file = new File(rootPath);
        if(!file.exists()||file.isFile()){
            return objMap;
        }
        FileSystem[] fileSystems = sigar.getFileSystemList();
        String mountedPoint = null;
        FileSystemUsage usage = null;
        int type = -1;
        for(FileSystem fileSystem : fileSystems){
            mountedPoint = fileSystem.getDirName();
            type = fileSystem.getType();
            if(BrStringUtils.isEmpty(mountedPoint)){
                continue;
            }
            //目录无关的分区
            if(!mountedPoint.contains(file.getAbsolutePath()) && !file.getAbsolutePath().contains(mountedPoint)){
                continue;
            }
            if(type == 2||type == 3){
            	usage = sigar.getFileSystemUsage(fileSystem.getDirName());
            	addDataToMap(objMap,0,mountedPoint,usage.getTotal());
            	addDataToMap(objMap,1,mountedPoint,usage.getAvail());
            	addDataToMap(objMap,2,mountedPoint,usage.getDiskReadBytes());
            	addDataToMap(objMap,3,mountedPoint,usage.getDiskWriteBytes());
            }
        }
        return objMap;
    }
    /**
     * 概述：汇总信息
     * @param objMap
     * @param type
     * @param key
     * @param value
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    private void addDataToMap(Map<Integer,Map<String,Long>> objMap, int type, String key, Long value){
        Map<String,Long> cMap = null;
        if(!objMap.containsKey(type)){
        	objMap.put(type,new ConcurrentHashMap<String, Long>());
        }
        cMap = objMap.get(type);
        cMap.put(key, value);
    }
}
