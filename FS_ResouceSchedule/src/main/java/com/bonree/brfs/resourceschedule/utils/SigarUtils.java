package com.bonree.brfs.resourceschedule.utils;

import java.io.File;
import java.util.ArrayList;
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

import com.bonree.brfs.resourceschedule.commons.Cache;
import com.bonree.brfs.resourceschedule.commons.impl.GatherResource;
import com.bonree.brfs.resourceschedule.model.BaseNetModel;
import com.bonree.brfs.resourceschedule.model.BasePatitionModel;
import com.bonree.brfs.resourceschedule.model.CpuStatModel;
import com.bonree.brfs.resourceschedule.model.MemoryStatModel;
import com.bonree.brfs.resourceschedule.model.NetStatModel;
import com.bonree.brfs.resourceschedule.model.PatitionStatModel;
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
    public BaseNetModel gatherBaseNetInfo(String ipAddress, long maxTSpeed, long maxRSpeed) throws SigarException{
    	// 1.过滤非法的ip地址
    	if(NetUtils.filterIp(ipAddress)){
    		return null;
    	}
    	BaseNetModel obj = new BaseNetModel();
    	//Sigar sigar = new Sigar();
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
    public Map<String, BaseNetModel> gatherBaseNetInfos(Cache cache) throws SigarException{
    	Map<String, BaseNetModel> objMap = new ConcurrentHashMap<String, BaseNetModel>();
    	//Sigar sigar = new Sigar();
    	String[] netInfos = sigar.getNetInterfaceList();
    	NetInterfaceConfig netConfig = null;
    	String tmpIp = null;
    	BaseNetModel obj = null;
    	for(String netInfo : netInfos){
    		netConfig = sigar.getNetInterfaceConfig(netInfo);
    		tmpIp = netConfig.getAddress();
    		// 1.过滤非法的IP
    		if(NetUtils.filterIp(tmpIp)){
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
    		obj = new BaseNetModel();
    		obj.setIpAddress(tmpIp);
    		obj.setDevName(netConfig.getName());
    		obj.setMacAddress(netConfig.getHwaddr());
    		obj.setMaxRSpeed(cache.NET_MAX_R_SPEED);
    		obj.setMaxTSpeed(cache.NET_MAX_T_SPEED);
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
    public NetStatModel gatherNetStatInfo(String ipAddress) throws SigarException{
    	// 1.过滤非法的ip地址
    	if(NetUtils.filterIp(ipAddress)){
    		return null;
    	}
    	NetStatModel obj = new NetStatModel();
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
    		if(NetUtils.filterIp(tmpIp)){
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
    public Map<String, NetStatModel> gatherNetStatInfos() throws SigarException{
    	Map<String, NetStatModel> objMap = new ConcurrentHashMap<String, NetStatModel>();
    	//Sigar sigar = new Sigar();
    	String[] netInfos = sigar.getNetInterfaceList();
    	NetInterfaceConfig netConfig = null;
    	NetInterfaceStat  netStat = null;
    	NetStatModel obj = null;
    	String tmpIp = null;
    	String devName = null;
    	for(String netInfo : netInfos){
    		netConfig = sigar.getNetInterfaceConfig(netInfo);
    		tmpIp = netConfig.getAddress();
    		devName = netConfig.getName();
    		
    		// 1.过滤非法的IP
    		if(NetUtils.filterIp(tmpIp)){
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
    		obj = new NetStatModel();
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
    public BasePatitionModel gatherBasePatitionInfo(String mountPoint, long maxWriteSpeed, long maxReadSpeed) throws SigarException{
    	// 1.过滤非法的挂载点
    	if(DiskUtils.filterMountPoint(mountPoint)){
    		return null;
    	}
    	BasePatitionModel obj = new BasePatitionModel();
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
    public Map<String, BasePatitionModel> gatherBasePatitionInfos(Cache cache) throws SigarException{
    	Map<String, BasePatitionModel> basePatitionMap = new ConcurrentHashMap<String, BasePatitionModel>();
    	String dir = cache.DATA_DIRECTORY;
    	// 1.过滤非法的挂载点
    	if(DiskUtils.filterMountPoint(dir)){
    		throw new NullPointerException("mount point is valid !!! " + dir);
    	}
    	String dirPath = new File(dir).getAbsolutePath();
    	//Sigar sigar = new Sigar();
    	FileSystem[] fileSystems = sigar.getFileSystemList();
    	BasePatitionModel obj = null;
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
            
            obj = new BasePatitionModel();
            devType = fileSystem.getType();
    		obj.setMountedPoint(tmpMountPoint);
    		obj.setDiskType(fileSystem.getType());
    		obj.setMaxReadSpeed(cache.DISK_MAX_READ_SPEED);
    		obj.setMaxWriteSpeed(cache.DISK_MAX_WRITE_SPEED);
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
    public PatitionStatModel gatherPatitionStatInfo(String mountPoint) throws SigarException{
    	// 1.过滤非法挂载点
    	if(DiskUtils.filterMountPoint(mountPoint)){
    		return null;
    	}
    	PatitionStatModel obj = new PatitionStatModel();
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
    public Map<String, PatitionStatModel> gatherPatitionStatInfos(Cache cache) throws SigarException{
    	Map<String, PatitionStatModel> patitionStatMap = new ConcurrentHashMap<String, PatitionStatModel>();
    	String dir = cache.DATA_DIRECTORY;
    	// 1.过滤非法的挂载点
    	if(DiskUtils.filterMountPoint(dir)){
    		return patitionStatMap;
    	}
    	String dirPath = new File(dir).getAbsolutePath();
    	//Sigar sigar = new Sigar();
    	FileSystem[] fileSystems = sigar.getFileSystemList();
    	PatitionStatModel obj = null;
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
            obj = new PatitionStatModel();
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
    public Map<String, PatitionStatModel> gatherPatitionStatInfos2(Cache cache) throws SigarException{
    	Map<String, PatitionStatModel> patitionStatMap = new ConcurrentHashMap<String, PatitionStatModel>();
    	String dir = cache.DATA_DIRECTORY;
    	// 1.过滤非法的挂载点
    	if(DiskUtils.filterMountPoint(dir)){
    		return patitionStatMap;
    	}
    	String dirPath = new File(dir).getAbsolutePath();
    	Map<String,FileSystem> fileSystemMap = sigar.getFileSystemMap();
    	Set<String> mountPoints = fileSystemMap.keySet();
    	List<String> avaliablePoints = new ArrayList<String>();
    	PatitionStatModel obj = null;
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
            obj = new PatitionStatModel();
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
    public CpuStatModel gatherCpuStatInfo() throws SigarException {
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
        CpuStatModel obj = new CpuStatModel();
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
    public MemoryStatModel gatherMemoryStatInfo() throws SigarException {
        Mem mem = sigar.getMem();
        long  actUse = mem.getActualUsed();
        long total = mem.getTotal();
        MemoryStatModel obj = new MemoryStatModel();
        double usageRate = (double) actUse / total;
        double remainRate = 1 - usageRate;
        obj.setMemoryRate(usageRate);
        obj.setMemoryRemainRate(remainRate);
        return obj;

    }
}
