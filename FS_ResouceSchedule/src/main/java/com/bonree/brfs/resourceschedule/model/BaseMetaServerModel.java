package com.bonree.brfs.resourceschedule.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
@JsonIgnoreProperties(ignoreUnknown = true) 
public class BaseMetaServerModel {
		
	/**
	 * cpu核心数
	 */
	private int cpuCoreCount = 0;
	
	/**
	 * 内存大小kb
	 */
	private long memoryTotalSize = 0;
	
	/**
	 * 总的硬盘空间大小kb
	 */
	private long diskTotalSize = 0;
	/**
	 * 网卡最大接收速度 单位byte（按10000Mbit/s网卡计算）
	 */
	private long netRxMaxSpeed = 1310720000L;
	
	/**
	 * 网卡最大发送速度 单位byte（按10000Mbit/s网卡计算）
	 */
	private long netTxMaxSpeed = 1310720000L;
	
	/**
	 * 磁盘最大写入速度 单位byte（按500MB/s的ssd硬盘计算）
	 */
	private long diskWriteMaxSpeed = 512000L;
	
	/**
	 * 磁盘最大读取速度 单位byte（按500MB/s的ssd硬盘计算）
	 */
	private long diskReadMaxSpeed = 512000L;
	
	public BaseMetaServerModel sum(BaseMetaServerModel base){
		BaseMetaServerModel obj = new BaseMetaServerModel();
		obj.setCpuCoreCount(this.cpuCoreCount + base.cpuCoreCount);
		obj.setDiskTotalSize(this.diskTotalSize + base.getDiskTotalSize());
		obj.setMemoryTotalSize(this.memoryTotalSize + base.getMemoryTotalSize());
		long netMaxR = this.netRxMaxSpeed > base.getNetRxMaxSpeed() ? this.netRxMaxSpeed : base.getNetRxMaxSpeed();
		long netMaxT = this.netTxMaxSpeed > base.getNetTxMaxSpeed() ? this.netTxMaxSpeed : base.getNetTxMaxSpeed();
		long diskW = this.diskWriteMaxSpeed > base.getDiskWriteMaxSpeed() ? this.diskWriteMaxSpeed : base.getDiskWriteMaxSpeed();
		long diskR = this.diskReadMaxSpeed > base.getDiskReadMaxSpeed() ? this.diskReadMaxSpeed : base.getDiskReadMaxSpeed();
		obj.setNetRxMaxSpeed(netMaxR);
		obj.setNetTxMaxSpeed(netMaxT);
		obj.setDiskWriteMaxSpeed(diskW);
		obj.setDiskReadMaxSpeed(diskR);
		return obj;
	}
	
	public int getCpuCoreCount() {
		return cpuCoreCount;
	}

	public void setCpuCoreCount(int cpuCoreCount) {
		this.cpuCoreCount = cpuCoreCount;
	}

	public long getMemoryTotalSize() {
		return memoryTotalSize;
	}

	public void setMemoryTotalSize(long memoryTotalSize) {
		this.memoryTotalSize = memoryTotalSize;
	}

	public long getDiskTotalSize() {
		return diskTotalSize;
	}

	public void setDiskTotalSize(long diskTotalSize) {
		this.diskTotalSize = diskTotalSize;
	}

	public long getNetRxMaxSpeed() {
		return netRxMaxSpeed;
	}

	public void setNetRxMaxSpeed(long netRxMaxSpeed) {
		this.netRxMaxSpeed = netRxMaxSpeed;
	}

	public long getNetTxMaxSpeed() {
		return netTxMaxSpeed;
	}

	public void setNetTxMaxSpeed(long netTxMaxSpeed) {
		this.netTxMaxSpeed = netTxMaxSpeed;
	}

	public long getDiskWriteMaxSpeed() {
		return diskWriteMaxSpeed;
	}

	public void setDiskWriteMaxSpeed(long diskWriteMaxSpeed) {
		this.diskWriteMaxSpeed = diskWriteMaxSpeed;
	}

	public long getDiskReadMaxSpeed() {
		return diskReadMaxSpeed;
	}

	public void setDiskReadMaxSpeed(long diskReadMaxSpeed) {
		this.diskReadMaxSpeed = diskReadMaxSpeed;
	}
	
	
}
