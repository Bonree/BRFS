package com.bonree.brfs.resouceschedule.commons;

import org.hyperic.sigar.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.resouceschedule.utils.StringUtils;
import com.bonree.brfs.resouceschedule.vo.BaseNetInfo;
import com.bonree.brfs.resouceschedule.vo.BasePatitionInfo;
import com.bonree.brfs.resouceschedule.vo.CpuStatInfo;
import com.bonree.brfs.resouceschedule.vo.MemoryStatInfo;
import com.bonree.brfs.resouceschedule.vo.NetStatInfo;
import com.bonree.brfs.resouceschedule.vo.PatitionStatInfo;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
     * 概述：采集指定ip地址的网卡信息
     * @param ipAddress 网卡ip地址
     * @param maxTSpeed 预设网卡最大发送速度
     * @param maxRSpeed 预设网卡最大接收速度
     * @return
     * @throws SigarException
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public BaseNetInfo gatherBaseNetInfo(String ipAddress, long maxTSpeed, long maxRSpeed) throws SigarException{
    	// 1.过滤非法的ip地址
    	if(Commons.filterIp(ipAddress)){
    		return null;
    	}
    	BaseNetInfo obj = new BaseNetInfo();
    	//Sigar sigar = new Sigar();
    	String[] netInfos = sigar.getNetInterfaceList();
    	NetInterfaceConfig netConfig = null;
    	String tmpIp = null;
    	for(String netInfo : netInfos){
    		netConfig = sigar.getNetInterfaceConfig(netInfo);
    		tmpIp = netConfig.getAddress();    	
    		// 1.过滤非法的ip
    		if(Commons.filterIp(tmpIp)){
    			continue;
    		}
    		// 2.过滤网卡不存在的
    		if(((netConfig.getFlags() & 1L) <= 0L)){
    			continue;
    		}
    		// 3.过滤不符合ip的网卡
    		if(!tmpIp.equals(ipAddress)){
    			continue;
    		}
    		obj.setIpAddress(tmpIp);
    		obj.setDevName(netConfig.getName());
    		obj.setMacAddress(netConfig.getHwaddr());
    		obj.setMaxRSpeed(maxRSpeed);
    		obj.setMaxTSpeed(maxTSpeed);
    		break;
    	}
    	return obj;
    }
    /**
     * 概述：采集所有网卡信息
     * @param maxRSpeed 预设网卡最大接收速度
     * @param maxTSpeed 预设网卡最大发送速度
     * @return Map<String,BaseNetInfo>   key：ip地址，value：网卡基本信息
     * @throws SigarException
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public Map<String, BaseNetInfo> gatherBaseNetInfos(long maxRSpeed, long maxTSpeed) throws SigarException{
    	Map<String, BaseNetInfo> objMap = new ConcurrentHashMap<String, BaseNetInfo>();
    	//Sigar sigar = new Sigar();
    	String[] netInfos = sigar.getNetInterfaceList();
    	NetInterfaceConfig netConfig = null;
    	String tmpIp = null;
    	BaseNetInfo obj = null;
    	for(String netInfo : netInfos){
    		netConfig = sigar.getNetInterfaceConfig(netInfo);
    		tmpIp = netConfig.getAddress();
    		// 1.过滤非法的IP
    		if(Commons.filterIp(tmpIp)){
    			continue;
    		}
    		// 2.过滤网卡不存在的
    		if(((netConfig.getFlags() & 1L) <= 0L)){
    			continue;
    		}
    		// 3.存在的ip将不再采集
    		if(objMap.containsKey(tmpIp)){
    			continue;
    		}
    		obj = new BaseNetInfo();
    		obj.setIpAddress(tmpIp);
    		obj.setDevName(netConfig.getName());
    		obj.setMacAddress(netConfig.getHwaddr());
    		obj.setMaxRSpeed(maxRSpeed);
    		obj.setMaxTSpeed(maxTSpeed);
    		objMap.put(tmpIp, obj);
    		
    	}
    	return objMap;
    }
    /**
     * 概述：采集网卡状态信息
     * @param ipAddress 
     * @return
     * @throws SigarException
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public NetStatInfo gatherNetStatInfo(String ipAddress) throws SigarException{
    	// 1.过滤非法的ip地址
    	if(Commons.filterIp(ipAddress)){
    		return null;
    	}
    	NetStatInfo obj = new NetStatInfo();
    	//Sigar sigar = new Sigar();
    	String[] netInfos = sigar.getNetInterfaceList();
    	NetInterfaceConfig netConfig = null;
    	NetInterfaceStat  netStat = null;
    	String tmpIp = null;
    	String devName = null;
    	for(String netInfo : netInfos){
    		netConfig = sigar.getNetInterfaceConfig(netInfo);
    		tmpIp = netConfig.getAddress();
    		devName = netConfig.getName();
    		// 1.过滤非法的ip
    		if(Commons.filterIp(tmpIp)){
    			continue;
    		}
    		// 2.过滤网卡不存在的
    		if(((netConfig.getFlags() & 1L) <= 0L)){
    			continue;
    		}
    		// 3.过滤不符合ip的网卡
    		if(!tmpIp.equals(ipAddress)){
    			continue;
    		}
    		obj.setIpAddress(tmpIp);
    		obj.setrDataSize(netStat.getRxBytes());
    		obj.settDataSize(netStat.getTxBytes());
    		break;
    	}
    	return obj;
    }
    /**
     * 概述：采集网卡状态信息
     * @return
     * @throws SigarException
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public Map<String, NetStatInfo> gatherNetStatInfos() throws SigarException{
    	Map<String, NetStatInfo> objMap = new ConcurrentHashMap<String, NetStatInfo>();
    	//Sigar sigar = new Sigar();
    	String[] netInfos = sigar.getNetInterfaceList();
    	NetInterfaceConfig netConfig = null;
    	NetInterfaceStat  netStat = null;
    	NetStatInfo obj = null;
    	String tmpIp = null;
    	String devName = null;
    	for(String netInfo : netInfos){
    		netConfig = sigar.getNetInterfaceConfig(netInfo);
    		tmpIp = netConfig.getAddress();
    		devName = netConfig.getName();
    		
    		// 1.过滤非法的IP
    		if(Commons.filterIp(tmpIp)){
    			continue;
    		}
    		// 2.过滤网卡不存在的
    		if(((netConfig.getFlags() & 1L) <= 0L)){
    			continue;
    		}
    		// 3.存在的ip将不再采集
    		if(objMap.containsKey(tmpIp)){
    			continue;
    		}
    		if(StringUtils.isEmpty(devName)){
    			continue;
    		}
    		netStat = sigar.getNetInterfaceStat(devName);
    		obj = new NetStatInfo();
    		obj.setIpAddress(tmpIp);
    		obj.setrDataSize(netStat.getRxBytes());
    		obj.settDataSize(netStat.getTxBytes());
    		objMap.put(tmpIp, obj);
    		
    	}
    	return objMap;
    }
    /**
     * 概述：获取指定分区的基本信息
     * @param mountPoint 磁盘分区挂载点
     * @param maxWriteSpeed 预设磁盘最大写入速度
     * @param maxReadSpeed 预设磁盘最大读取速度
     * @return
     * @throws SigarException
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public BasePatitionInfo gatherBasePatitionInfo(String mountPoint, long maxWriteSpeed, long maxReadSpeed) throws SigarException{
    	// 1.过滤非法的挂载点
    	if(Commons.filterMountPoint(mountPoint)){
    		return null;
    	}
    	BasePatitionInfo obj = new BasePatitionInfo();
    	//Sigar sigar = new Sigar();
    	FileSystem[] fileSystems = sigar.getFileSystemList();
    	String tmpMountPoint = null;
    	FileSystemUsage usage = null;
    	int devType = 0;
    	for(FileSystem fileSystem : fileSystems){
    		tmpMountPoint = fileSystem.getDirName();
    		// 1.过滤挂载点为空的分区
    		if(StringUtils.isEmpty(tmpMountPoint)){
    			continue;
    		}
    		if(!mountPoint.equals(tmpMountPoint)){
    			continue;
    		}
    		devType = fileSystem.getType();
    		obj.setMountedPoint(tmpMountPoint);
    		obj.setDiskType(fileSystem.getType());
    		obj.setMaxReadSpeed(maxReadSpeed);
    		obj.setMaxWriteSpeed(maxWriteSpeed);
    		obj.setPatitionFormateName(fileSystem.getSysTypeName());
    		// 3.非本地磁盘及NFS的不统计分区大小
    		if(devType == 2 || devType == 3){
    			usage = sigar.getFileSystemUsage(tmpMountPoint);
    			obj.setPatitionSize(usage.getTotal());
    		}
    		break;
    	}
    	return obj;
    }
   /**
    * 概述：
    * @param dir
    * @param maxWriteSpeed
    * @param maxReadSpeed
    * @return
    * @throws SigarException
    * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
    */
    public Map<String, BasePatitionInfo> gatherBasePatitionInfos(String dir, long maxWriteSpeed, long maxReadSpeed) throws SigarException{
    	Map<String, BasePatitionInfo> basePatitionMap = new ConcurrentHashMap<String, BasePatitionInfo>();
    	// 1.过滤非法的挂载点
    	if(Commons.filterMountPoint(dir)){
    		return basePatitionMap;
    	}
    	String dirPath = new File(dir).getAbsolutePath();
    	//Sigar sigar = new Sigar();
    	FileSystem[] fileSystems = sigar.getFileSystemList();
    	BasePatitionInfo obj = null;
    	String tmpMountPoint = null;
    	FileSystemUsage usage = null;
    	int devType = 0;
    	for(FileSystem fileSystem : fileSystems){
    		tmpMountPoint = fileSystem.getDirName();
    		// 1.过滤挂载点为空的分区
    		if(StringUtils.isEmpty(tmpMountPoint)){
    			continue;
    		}
    		// 2.过滤与目录无关的分区
            if(!tmpMountPoint.contains(dirPath) && !dirPath.contains(tmpMountPoint)){
                continue;
            }
            // 3.过滤已经存在的分区
            if(basePatitionMap.containsKey(tmpMountPoint)){
            	continue;
            }
            
            obj = new BasePatitionInfo();
            devType = fileSystem.getType();
    		obj.setMountedPoint(tmpMountPoint);
    		obj.setDiskType(fileSystem.getType());
    		obj.setMaxReadSpeed(maxReadSpeed);
    		obj.setMaxWriteSpeed(maxWriteSpeed);
    		obj.setPatitionFormateName(fileSystem.getSysTypeName());
    		// 4.非本地磁盘及NFS的不统计分区大小
    		if(devType == 2 || devType == 3){
    			usage = sigar.getFileSystemUsage(tmpMountPoint);
    			obj.setPatitionSize(usage.getTotal());
    		}
    		basePatitionMap.put(tmpMountPoint, obj);
    	}
    	return basePatitionMap;
    }
   
    /**
     * 概述：采集挂载点分区使用状态
     * @param mountPoint
     * @return
     * @throws SigarException
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public PatitionStatInfo gatherPatitionStatInfo(String mountPoint) throws SigarException{
    	// 1.过滤非法挂载点
    	if(Commons.filterMountPoint(mountPoint)){
    		return null;
    	}
    	PatitionStatInfo obj = new PatitionStatInfo();
    	//Sigar sigar = new Sigar();
    	FileSystem[] fileSystems = sigar.getFileSystemList();
    	String tmpMountPoint = null;
    	FileSystemUsage usage = null;
    	int devType = 0;
    	for(FileSystem fileSystem : fileSystems){
    		tmpMountPoint = fileSystem.getDirName();
    		// 1.过滤挂载点为空的分区
    		if(StringUtils.isEmpty(tmpMountPoint)){
    			continue;
    		}
    		if(!mountPoint.equals(tmpMountPoint)){
    			continue;
    		}
    		obj.setMountPoint(tmpMountPoint);
    		// 3.非本地磁盘及NFS的不统计分区大小
    		if(devType == 2 || devType == 3){
    			usage = sigar.getFileSystemUsage(tmpMountPoint);
    			obj.setReadDataSize(usage.getDiskReadBytes());
    			obj.setWriteDataSize(usage.getDiskWriteBytes());
    			obj.setRemainSize(usage.getAvail());
    			obj.setUsedSize(usage.getUsed());
    		}
    		break;
    	}
    	return obj;
    }
    /**
     * 概述：采集data目录下的分区状态信息
     * @param dir
     * @return
     * @throws SigarException
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public Map<String, PatitionStatInfo> gatherPatitionStatInfos(String dir) throws SigarException{
    	Map<String, PatitionStatInfo> patitionStatMap = new ConcurrentHashMap<String, PatitionStatInfo>();
    	// 1.过滤非法的挂载点
    	if(Commons.filterMountPoint(dir)){
    		return patitionStatMap;
    	}
    	String dirPath = new File(dir).getAbsolutePath();
    	System.out.println("gatherPatitionStatInfos :"+sigar.getFileSystemMap().keySet().toString());
    	//Sigar sigar = new Sigar();
    	FileSystem[] fileSystems = sigar.getFileSystemList();
    	PatitionStatInfo obj = null;
    	String tmpMountPoint = null;
    	FileSystemUsage usage = null;
    	int devType = 0;
    	for(FileSystem fileSystem : fileSystems){
    		tmpMountPoint = fileSystem.getDirName();
    		devType = fileSystem.getType();
    		// 1.过滤挂载点为空的分区
    		if(StringUtils.isEmpty(tmpMountPoint)){
    			continue;
    		}
    		// 2.过滤与目录无关的分区
            if(!tmpMountPoint.contains(dirPath) && !dirPath.contains(tmpMountPoint)){
                continue;
            }
            // 3.过滤已经存在的分区
            if(patitionStatMap.containsKey(tmpMountPoint)){
            	continue;
            }
            obj = new PatitionStatInfo();
            obj.setMountPoint(tmpMountPoint);
    		// 4.非本地磁盘及NFS的不统计分区大小
    		if(devType == 2 || devType == 3){
    			usage = sigar.getFileSystemUsage(tmpMountPoint);
    			obj.setReadDataSize(usage.getDiskReadBytes());
    			obj.setWriteDataSize(usage.getDiskWriteBytes());
    			obj.setRemainSize(usage.getAvail());
    			obj.setUsedSize(usage.getUsed());
    		}
    		patitionStatMap.put(tmpMountPoint, obj);
    	}
    	return patitionStatMap;
    }
    /**
     * 概述：采集data目录下的分区状态信息
     * @param dir
     * @return
     * @throws SigarException
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public Map<String, PatitionStatInfo> gatherPatitionStatInfos2(String dir) throws SigarException{
    	Map<String, PatitionStatInfo> patitionStatMap = new ConcurrentHashMap<String, PatitionStatInfo>();
    	// 1.过滤非法的挂载点
    	if(Commons.filterMountPoint(dir)){
    		return patitionStatMap;
    	}
    	String dirPath = new File(dir).getAbsolutePath();
    	Map<String,FileSystem> fileSystemMap = sigar.getFileSystemMap();
    	Set<String> mountPoints = fileSystemMap.keySet();
    	List<String> avaliablePoints = new ArrayList<String>();
    	PatitionStatInfo obj = null;
    	FileSystemUsage usage = null;
    	int devType = 0;
    	for(String tmpPoint : mountPoints){
    		// 1.过滤挂载点为空的分区
    		if(StringUtils.isEmpty(tmpPoint)){
    			continue;
    		}
    		// 2.过滤与目录无关的分区
            if(!tmpPoint.contains(dirPath) && !dirPath.contains(tmpPoint)){
                continue;
            }
            // 3.过滤已经存在的分区
            if(patitionStatMap.containsKey(tmpPoint)){
            	continue;
            }
            devType = fileSystemMap.get(tmpPoint).getType();
            obj = new PatitionStatInfo();
            obj.setMountPoint(tmpPoint);
    		// 4.非本地磁盘及NFS的不统计分区大小
    		if(devType == 2 || devType == 3){
    			usage = sigar.getFileSystemUsage(tmpPoint);
    			obj.setReadDataSize(usage.getDiskReadBytes());
    			obj.setWriteDataSize(usage.getDiskWriteBytes());
    			obj.setRemainSize(usage.getAvail());
    			obj.setUsedSize(usage.getUsed());
    		}
    		patitionStatMap.put(tmpPoint, obj);
    	}
    	
    	return patitionStatMap;
    }
    
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
    	//Sigar sigar = new Sigar();
        Mem mem = sigar.getMem();
        return mem.getTotal();
    }
    /**
     * 概述：采集cpu状态
     * 比较特殊的是CPU总使用率的计算(util),目前的算法是: util = 1 - idle - iowait - steal
     * @return
     * @throws SigarException
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public CpuStatInfo gatherCpuStatInfo() throws SigarException {
    	//Sigar sigar = new Sigar();
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
        CpuStatInfo obj = new CpuStatInfo();
        obj.setCpuRate(1 - (idleRate+iowaitRate+stlRate));
        obj.setCpuRemainRate(idleRate+iowaitRate+stlRate);
        return obj;
    }
    /**
     * 概述：采集内存状态信息
     * util = (total - free - buff - cache) / total
     * @return
     * @throws SigarException
     * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
     */
    public MemoryStatInfo gatherMemoryStatInfo() throws SigarException {
        Mem mem = sigar.getMem();
        long  actUse = mem.getActualUsed();
        long total = mem.getTotal();
        MemoryStatInfo obj = new MemoryStatInfo();
        double usageRate = (double) actUse / total;
        double remainRate = 1 - usageRate;
        obj.setMemoryRate(usageRate);
        obj.setMemoryRemainRate(remainRate);
        return obj;

    }
}
