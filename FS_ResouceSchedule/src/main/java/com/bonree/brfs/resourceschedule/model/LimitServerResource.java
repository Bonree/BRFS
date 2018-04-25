package com.bonree.brfs.resourceschedule.model;

public class LimitServerResource {
	
	/**
	 * 本机最大硬盘剩余率
	 */
	private double remainValue = 0.0;
	public double getRemainValue() {
		return remainValue;
	}
	public void setRemainValue(double remainValue) {
		this.remainValue = remainValue;
	}
}
