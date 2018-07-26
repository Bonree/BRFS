package com.bonree.brfs.resourceschedule.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true) 
public class LimitServerResource {
	
	/**
	 * 本机最大硬盘剩余率
	 */
	private double remainValue = 0.01;
	public double getRemainValue() {
		return remainValue;
	}
	public void setRemainValue(double remainValue) {
		this.remainValue = remainValue;
	}
}
