package com.bonree.brfs.resourceschedule.model;
/******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007北京博睿宏远数据科技股份有限公司,Inc.All Rights Reserved.
 * 
 * @date 2018年4月7日 下午9:42:54
 * @Author: <a href=mailto:zhucg@bonree.com>朱成岗</a>
 * @Description: 可用资源管理策略
 *****************************************************************************
 */
public class ServerPattern {
	private double diskRemainRatio = 1.0;
	private double diskReadRatio = 1.0;
	private double diskWriteRatio = 1.0;
	private double netTxRatio = 1.0;
	private double netRxRatio = 1.0;
	private double cpuRatio = 1.0;
	private double memoryRatio = 1.0;
	public double getDiskRemainRatio() {
		return diskRemainRatio;
	}
	public void setDiskRemainRatio(double diskRemainRatio) {
		this.diskRemainRatio = diskRemainRatio;
	}
	public double getDiskReadRatio() {
		return diskReadRatio;
	}
	public void setDiskReadRatio(double diskReadRatio) {
		this.diskReadRatio = diskReadRatio;
	}
	public double getDiskWriteRatio() {
		return diskWriteRatio;
	}
	public void setDiskWriteRatio(double diskWriteRatio) {
		this.diskWriteRatio = diskWriteRatio;
	}
	public double getNetTxRatio() {
		return netTxRatio;
	}
	public void setNetTxRatio(double netTxRatio) {
		this.netTxRatio = netTxRatio;
	}
	public double getNetRxRatio() {
		return netRxRatio;
	}
	public void setNetRxRatio(double netRxRatio) {
		this.netRxRatio = netRxRatio;
	}
	public double getCpuRatio() {
		return cpuRatio;
	}
	public void setCpuRatio(double cpuRatio) {
		this.cpuRatio = cpuRatio;
	}
	public double getMemoryRatio() {
		return memoryRatio;
	}
	public void setMemoryRatio(double memoryRatio) {
		this.memoryRatio = memoryRatio;
	}
}
