package com.bonree.brfs.resourceschedule.model;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ResourceModel {
	private String serverId;
	private double diskRemainRate;
	private double cpuValue;
	private double memoryValue;
	private Map<String,Double> diskWriteValue = new ConcurrentHashMap<String, Double>();
	private Map<String,Double> diskReadValue = new ConcurrentHashMap<String, Double>();
	private Map<String,Double> diskRemainValue = new ConcurrentHashMap<String, Double>();
	private Map<String,Double> netRxValue = new ConcurrentHashMap<String, Double>();
	private Map<String,Double> netTxValue = new ConcurrentHashMap<String, Double>();
	public String getServerId() {
		return serverId;
	}
	public void setServerId(String serverId) {
		this.serverId = serverId;
	}
	public double getDiskRemainRate() {
		return diskRemainRate;
	}
	public void setDiskRemainRate(double diskRemainRate) {
		this.diskRemainRate = diskRemainRate;
	}
	public double getCpuValue() {
		return cpuValue;
	}
	public void setCpuValue(double cpuValue) {
		this.cpuValue = cpuValue;
	}
	public double getMemoryValue() {
		return memoryValue;
	}
	public void setMemoryValue(double memoryValue) {
		this.memoryValue = memoryValue;
	}
	public Map<String, Double> getDiskWriteValue() {
		return diskWriteValue;
	}
	public void setDiskWriteValue(Map<String, Double> diskWriteValue) {
		this.diskWriteValue = diskWriteValue;
	}
	public Map<String, Double> getDiskReadValue() {
		return diskReadValue;
	}
	public void setDiskReadValue(Map<String, Double> diskReadValue) {
		this.diskReadValue = diskReadValue;
	}
	public Map<String, Double> getDiskRemainValue() {
		return diskRemainValue;
	}
	public void setDiskRemainValue(Map<String, Double> diskRemainValue) {
		this.diskRemainValue = diskRemainValue;
	}
	public Map<String, Double> getNetRxValue() {
		return netRxValue;
	}
	public void setNetRxValue(Map<String, Double> netRxValue) {
		this.netRxValue = netRxValue;
	}
	public Map<String, Double> getNetTxValue() {
		return netTxValue;
	}
	public void setNetTxValue(Map<String, Double> netTxValue) {
		this.netTxValue = netTxValue;
	}
	
}
