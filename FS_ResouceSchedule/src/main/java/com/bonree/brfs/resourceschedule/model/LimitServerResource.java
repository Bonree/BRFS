package com.bonree.brfs.resourceschedule.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class LimitServerResource {
	private double diskRemainRate = 0.05;
	private double forceDiskRemainRate = 0.01;

	public double getDiskRemainRate() {
		return diskRemainRate;
	}
	public void setDiskRemainRate(double remainValue) {
		this.diskRemainRate = remainValue;
	}

    public double getForceDiskRemainRate(){
        return forceDiskRemainRate;
    }

    public void setForceDiskRemainRate(double forceDiskRemainRate){
        this.forceDiskRemainRate = forceDiskRemainRate;
    }
}
