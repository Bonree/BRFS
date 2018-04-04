package com.bonree.brfs.resourceschedule.model;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.resourceschedule.utils.CalcUtils;
import com.bonree.brfs.resourceschedule.utils.DiskUtils;

public class StatServerModel{
	/**
	 * 统计计数器
	 */
	private int calcCount = 1;
	/**
	 * cpu内核数
	 */
	private int cpuCoreCount;
	
	/**
	 * 内存大小
	 */
	private long memorySize;
	
	/**
	 * cpu使用率
	 */
	private double cpuRate;
	
	/**
	 * 内存使用率
	 */
	private double memoryRate;
	/**
	 * 硬盘大小
	 */
	private long totalDiskSize;
	/**
	 * 硬盘剩余空间
	 */
	private long remainDiskSize;
	
	/**
	 * 分区大小kb
	 */
	private Map<String,Long> partitionTotalSizeMap = new ConcurrentHashMap<String,Long>();		
	
	/**
	 * 分区剩余kb
	 */
	private Map<String,Long> partitionRemainSizeMap = new ConcurrentHashMap<String,Long>();
	
	/**
	 * 分区写入speedkb
	 */
	private Map<String,Long> partitionWriteSpeedMap = new ConcurrentHashMap<String,Long>();
	
	/**
	 * 分区读取kb
	 */
	private Map<String,Long> partitionReadSpeedMap = new ConcurrentHashMap<String,Long>();
	
	/**
	 * 网卡发送字节数
	 */
	private Map<String,Long> netTSpeedMap = new ConcurrentHashMap<String,Long>();
	
	/**
	 * 网卡发送字节数
	 */
	private Map<String,Long> netRSpeedMap = new ConcurrentHashMap<String,Long>();
	
	/**
	 * storagename与分区的映射关系
	 */
	private Map<String,String> storageNameOnPartitionMap = new ConcurrentHashMap<String,String>();
	
	public void calc(Collection<String> snSet, long time) {
		long count = time < 0 ? 1 : time;
		
		/**
		 * 硬盘大小
		 */
		this.totalDiskSize = CalcUtils.collectDataMap(this.partitionTotalSizeMap);
		/**
		 * 硬盘剩余大小
		 */
		this.remainDiskSize = CalcUtils.collectDataMap(this.partitionRemainSizeMap);
		/**
		 * 转换sn与partition的关系
		 */
		this.storageNameOnPartitionMap = matchSnToPatition(snSet, this.partitionTotalSizeMap.keySet());
		
		if(this.calcCount == 0 || this.calcCount ==1){
			return;
		}
		// 内存使用率
		this.memoryRate = this.memoryRate / this.calcCount / count;
		// cpu使用率
		this.cpuRate = this.cpuRate / this.calcCount/ count;
		count = count * this.calcCount;
		// 分区读取速度
		this.partitionReadSpeedMap = CalcUtils.divDataMap(this.partitionReadSpeedMap, count);
		// 分区写入速度
		this.partitionWriteSpeedMap = CalcUtils.divDataMap(this.partitionWriteSpeedMap, count);
		
		// 网卡发送速度
		this.netTSpeedMap = CalcUtils.divDataMap(this.netTSpeedMap, count);
		// 网卡接收速度
		this.netRSpeedMap = CalcUtils.divDataMap(this.netRSpeedMap, count);
		//重置计数器
		this.calcCount = 1;
		
	}
	private Map<String,String> matchSnToPatition(Collection<String> snList, Set<String> mountPoints){
    	Map<String, String> objMap = new ConcurrentHashMap<String,String>();
    	if(snList == null || mountPoints == null){
    		return objMap;
    	}
    	// 获取每个sn对应的空间大小
		String mountPoint = null;
		// 匹配sn与挂载点
		for(String sn : snList){
			mountPoint = DiskUtils.selectPartOfDisk(sn, mountPoints);
			if(BrStringUtils.isEmpty(mountPoint)){
				continue;
			}
			if(!objMap.containsKey(sn)){
				objMap.put(sn, mountPoint);
			}
		}
		return objMap;
    }

	public StatServerModel sum(StatServerModel t1) {
		StatServerModel obj = new StatServerModel();
		int count = this.calcCount + t1.getCalcCount();
		// 1.不变的参数
		// cpu核心数
		obj.setCpuCoreCount(this.cpuCoreCount);
		// 内存大小
		obj.setMemorySize(this.memorySize);
		// 分区大小
		obj.setPartitionTotalSizeMap(this.partitionTotalSizeMap);
		
		// 2.替换数据
		obj.setPartitionRemainSizeMap(this.partitionRemainSizeMap);
		
		// 3.汇总数据
		// 内存使用率
		obj.setMemoryRate(this.memoryRate + t1.getMemoryRate());
		// cpu使用率
		obj.setCpuRate(this.cpuRate + t1.cpuRate);
		// 分区读取速度
		obj.setPartitionReadSpeedMap(CalcUtils.sumDataMap(this.partitionReadSpeedMap, t1.getPartitionReadSpeedMap()));
		// 分区写入速度
		obj.setPartitionWriteSpeedMap(CalcUtils.sumDataMap(this.partitionWriteSpeedMap, t1.getPartitionWriteSpeedMap()));
		// 网卡发送速度
		obj.setNetTSpeedMap(CalcUtils.sumDataMap(this.netTSpeedMap, t1.getNetTSpeedMap()));
		// 网卡接收速度
		obj.setNetRSpeedMap(CalcUtils.sumDataMap(this.netRSpeedMap, t1.getNetRSpeedMap()));
		obj.setCalcCount(count);
		return obj;
	}
	public double getCpuRate() {
		return cpuRate;
	}

	public void setCpuRate(double cpuRate) {
		this.cpuRate = cpuRate;
	}

	public double getMemoryRate() {
		return memoryRate;
	}

	public void setMemoryRate(double memoryRate) {
		this.memoryRate = memoryRate;
	}

	public long getRemainDiskSize() {
		return remainDiskSize;
	}

	public void setRemainDiskSize(long remainDiskSize) {
		this.remainDiskSize = remainDiskSize;
	}

	public Map<String, Long> getPartitionRemainSizeMap() {
		return partitionRemainSizeMap;
	}

	public void setPartitionRemainSizeMap(Map<String, Long> partitionRemainSizeMap) {
		this.partitionRemainSizeMap = partitionRemainSizeMap;
	}

	public Map<String, Long> getPartitionWriteSpeedMap() {
		return partitionWriteSpeedMap;
	}

	public void setPartitionWriteSpeedMap(Map<String, Long> partitionWriteSpeedMap) {
		this.partitionWriteSpeedMap = partitionWriteSpeedMap;
	}

	public Map<String, Long> getPartitionReadSpeedMap() {
		return partitionReadSpeedMap;
	}

	public void setPartitionReadSpeedMap(Map<String, Long> partitionReadSpeedMap) {
		this.partitionReadSpeedMap = partitionReadSpeedMap;
	}

	public Map<String, Long> getNetTSpeedMap() {
		return netTSpeedMap;
	}

	public void setNetTSpeedMap(Map<String, Long> netTSpeedMap) {
		this.netTSpeedMap = netTSpeedMap;
	}

	public Map<String, Long> getNetRSpeedMap() {
		return netRSpeedMap;
	}

	public void setNetRSpeedMap(Map<String, Long> netRSpeedMap) {
		this.netRSpeedMap = netRSpeedMap;
	}

	public Map<String, String> getStorageNameOnPartitionMap() {
		return storageNameOnPartitionMap;
	}

	public void setStorageNameOnPartitionMap(Map<String, String> storageNameOnPartitionMap) {
		this.storageNameOnPartitionMap = storageNameOnPartitionMap;
	}

	public int getCalcCount() {
		return calcCount;
	}

	public void setCalcCount(int calcCount) {
		this.calcCount = calcCount;
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

	public Map<String, Long> getPartitionTotalSizeMap() {
		return partitionTotalSizeMap;
	}

	public void setPartitionTotalSizeMap(Map<String, Long> partitionTotalSizeMap) {
		this.partitionTotalSizeMap = partitionTotalSizeMap;
	}
	public long getTotalDiskSize() {
		return totalDiskSize;
	}
	public void setTotalDiskSize(long totalDiskSize) {
		this.totalDiskSize = totalDiskSize;
	}
	
}
