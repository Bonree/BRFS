package com.bonree.brfs.gui.server.stats;

import com.fasterxml.jackson.annotation.JsonProperty;

public class StatConfigs {
    @JsonProperty("seed.uri")
    private String seedUri;
    @JsonProperty("base.dir")
    private String baseDir = "stat";
    @JsonProperty("interval.seconds")
    private int intervalTime = 60;
    @JsonProperty("ttl.seconds")
    private int ttlTime = 7 * 24 * 60 * 60;
    @JsonProperty("maintain.interval.seconds")
    private int scanIntervalTime = 60;

    public String getBaseDir() {
        return baseDir;
    }

    public void setBaseDir(String baseDir) {
        this.baseDir = baseDir;
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

    public String getSeedUri() {
        return seedUri;
    }

    public void setSeedUri(String seedUri) {
        this.seedUri = seedUri;
    }
}
