package com.bonree.brfs.resourceschedule.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class LimitServerResource {
	private double diskRemainRate = 0.05;
	private double forceDiskRemainRate = 0.01;
	private double diskWriteValue = 0.9;
	private double forceWriteValue = 0.99;

    public double getForceWriteValue(){
        return forceWriteValue;
    }

    public void setForceWriteValue(double forceWriteValue){
        this.forceWriteValue = forceWriteValue;
    }

    public double getDiskWriteValue(){
        return diskWriteValue;
    }

    public void setDiskWriteValue(double diskWriteValue){
        this.diskWriteValue = diskWriteValue;
    }

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
