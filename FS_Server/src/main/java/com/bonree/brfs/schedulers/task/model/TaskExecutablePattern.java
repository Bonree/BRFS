package com.bonree.brfs.schedulers.task.model;

public class TaskExecutablePattern {
	private double memoryRate;
	private double cpuRate;
	private double diskRemainRate;
	private double diskWriteRate;
	private double diskReadRate;
	private double netRxRate;
	private double netTxRate;
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
