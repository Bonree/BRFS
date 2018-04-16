package com.bonree.brfs.schedulers.task.model;

public class TaskExecutablePattern {
	private double memoryRate;
	private double cpuRate;
	private double diskRemainRate;
	private double diskRemainWriteRate;
	private double diskRemainReadRate;
	private double netRemainRxRate;
	private double netRemainTxRate;
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
	public double getDiskRemainWriteRate() {
		return diskRemainWriteRate;
	}
	public void setDiskRemainWriteRate(double diskRemainWriteRate) {
		this.diskRemainWriteRate = diskRemainWriteRate;
	}
	public double getDiskRemainReadRate() {
		return diskRemainReadRate;
	}
	public void setDiskRemainReadRate(double diskRemainReadRate) {
		this.diskRemainReadRate = diskRemainReadRate;
	}
	public double getNetRemainRxRate() {
		return netRemainRxRate;
	}
	public void setNetRemainRxRate(double netRemainRxRate) {
		this.netRemainRxRate = netRemainRxRate;
	}
	public double getNetRemainTxRate() {
		return netRemainTxRate;
	}
	public void setNetRemainTxRate(double netRemainTxRate) {
		this.netRemainTxRate = netRemainTxRate;
	}
	
}
