package com.bonree.brfs.schedulers.task.model;

import com.bonree.brfs.configuration.ResourceTaskConfig;

public class TaskExecutablePattern {
	private double memoryRate;
	private double cpuRate;
	private double diskRemainRate;
	private double diskWriteRate;
	private double diskReadRate;
	private double netRxRate;
	private double netTxRate;
	
	/***
	 * 概述：创建限制资源对象
	 * @param conf
	 * @return
	 * @user <a href=mailto:zhucg@bonree.com>朱成岗</a>
	 */
	public static TaskExecutablePattern parse(ResourceTaskConfig conf){
		TaskExecutablePattern limit = new TaskExecutablePattern();
		limit.setCpuRate(conf.getLimitCpuRate());
		limit.setMemoryRate(conf.getLimitMemoryRate());
		limit.setDiskRemainRate(conf.getLimitDiskRemaintRate());
		limit.setDiskReadRate(conf.getLimitDiskReadRate());
		limit.setDiskWriteRate(conf.getLimitDiskWriteRate());
		limit.setNetRxRate(conf.getLimitNetRxRate());
		limit.setNetTxRate(conf.getLimitNetTxRate());
		return limit;
	}
	public double getMemoryRate() {
		return memoryRate;
	}
	public void setMemoryRate(double memoryRate) {
		this.memoryRate = memoryRate;
	}
	public double getCpuRate() {
		return cpuRate;
	}
	public void setCpuRate(double cpuRate) {
		this.cpuRate = cpuRate;
	}
	public double getDiskRemainRate() {
		return diskRemainRate;
	}
	public void setDiskRemainRate(double diskRemainRate) {
		this.diskRemainRate = diskRemainRate;
	}
	public double getDiskWriteRate() {
		return diskWriteRate;
	}
	public void setDiskWriteRate(double diskWriteRate) {
		this.diskWriteRate = diskWriteRate;
	}
	public double getDiskReadRate() {
		return diskReadRate;
	}
	public void setDiskReadRate(double diskReadRate) {
		this.diskReadRate = diskReadRate;
	}
	public double getNetRxRate() {
		return netRxRate;
	}
	public void setNetRxRate(double netRxRate) {
		this.netRxRate = netRxRate;
	}
	public double getNetTxRate() {
		return netTxRate;
	}
	public void setNetTxRate(double netTxRate) {
		this.netTxRate = netTxRate;
	}	
}
