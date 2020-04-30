package com.bonree.brfs.disknode;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ResourceConfig {
    @JsonProperty("switch")
    private boolean runFlag = false;
    @JsonProperty("interval.seconds")
    private int intervalTime = 60;

    public boolean isRunFlag() {
        return runFlag;
    }

    public void setRunFlag(boolean runFlag) {
        this.runFlag = runFlag;
    }

    public int getIntervalTime() {
        return intervalTime;
    }

    public void setIntervalTime(int intervalTime) {
        this.intervalTime = intervalTime;
    }
}
