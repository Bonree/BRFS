package com.bonree.brfs.disknode;

import com.fasterxml.jackson.annotation.JsonProperty;

public class GuiResourceConfig {
    @JsonProperty("switch")
    private boolean runFlag = false;
    @JsonProperty("dir")
    private String guiDir = "gui";
    @JsonProperty("interval.seconds")
    private int intervalTime = 60;
    @JsonProperty("ttl.seconds")
    private int ttlTime = 7 * 24 * 60 * 60;
    @JsonProperty("maintain.interval.seconds")
    private int scanIntervalTime = 60;

    public String getGuiDir() {
        return guiDir;
    }

    public void setGuiDir(String guiDir) {
        this.guiDir = guiDir;
    }

    public int getIntervalTime() {
        return intervalTime;
    }

    public void setIntervalTime(int intervalTime) {
        this.intervalTime = intervalTime;
    }

    public int getTtlTime() {
        return ttlTime;
    }

    public void setTtlTime(int ttlTime) {
        this.ttlTime = ttlTime;
    }

    public int getScanIntervalTime() {
        return scanIntervalTime;
    }

    public void setScanIntervalTime(int scanIntervalTime) {
        this.scanIntervalTime = scanIntervalTime;
    }

    public boolean isRunFlag() {
        return runFlag;
    }

    public void setRunFlag(boolean runFlag) {
        this.runFlag = runFlag;
    }
}
