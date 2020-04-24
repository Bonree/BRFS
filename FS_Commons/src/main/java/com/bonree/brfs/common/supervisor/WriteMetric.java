package com.bonree.brfs.common.supervisor;

import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.common.utils.JsonUtils.JsonException;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.HashMap;
import java.util.Map;

public class WriteMetric {
    @JsonProperty("monitor_time")
    private long monitorTime;
    @JsonProperty("storage_name")
    private String storageName;
    @JsonProperty("region_node_id")
    private String regionNodeID;
    @JsonProperty("data_node_id")
    private String dataNodeID;
    @JsonProperty("data_count")
    private int dataCount;
    @JsonProperty("data_size")
    private int dataSize;
    @JsonProperty("elapsed_time")
    private int elapsedTime;
    @JsonProperty("data_max_size")
    private int dataMaxSize;
    @JsonProperty("elapsed_max_time")
    private int avgElapsedTime;

    public long getMonitorTime() {
        return monitorTime;
    }

    public void setMonitorTime(long monitorTime) {
        this.monitorTime = monitorTime;
    }

    public String getStorageName() {
        return storageName;
    }

    public void setStorageName(String storageName) {
        this.storageName = storageName;
    }

    public String getRegionNodeID() {
        return regionNodeID;
    }

    public void setRegionNodeID(String regionNodeID) {
        this.regionNodeID = regionNodeID;
    }

    public String getDataNodeID() {
        return dataNodeID;
    }

    public void setDataNodeID(String dataNodeID) {
        this.dataNodeID = dataNodeID;
    }

    public int getDataCount() {
        return dataCount;
    }

    public void setDataCount(int dataCount) {
        this.dataCount = dataCount;
    }

    public void incrementDataCount(int count) {
        this.dataCount += count;
    }

    public int getDataSize() {
        return dataSize;
    }

    public void setDataSize(int dataSize) {
        this.dataSize = dataSize;
    }

    public void incrementDataSize(int size) {
        this.dataSize += size;
    }

    public int getElapsedTime() {
        return elapsedTime;
    }

    public void setElapsedTime(int elapsedTime) {
        this.elapsedTime = elapsedTime;
    }

    public int getDataMaxSize() {
        return dataMaxSize;
    }

    public void setDataMaxSize(int dataMaxSize) {
        this.dataMaxSize = dataMaxSize;
    }

    public void updateDataMaxSize(int dataMaxSize) {
        this.dataMaxSize = dataMaxSize > this.dataMaxSize ? dataMaxSize : this.dataMaxSize;
    }

    public int getAvgElapsedTime() {
        return avgElapsedTime;
    }

    public void setAvgElapsedTime(int avgElapsedTime) {
        this.avgElapsedTime = avgElapsedTime;
    }

    public String toJsonString() throws JsonException {
        return JsonUtils.toJsonString(this);
    }

    public Map<String, Object> toMap() {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("monitor_time", monitorTime);
        map.put("storage_name", storageName);
        map.put("region_node_id", regionNodeID);
        map.put("data_node_id", dataNodeID);
        map.put("data_count", dataCount);
        map.put("data_size", dataSize);
        map.put("elapsed_time", elapsedTime);
        map.put("data_max_size", dataMaxSize);
        map.put("elapsed_max_time", avgElapsedTime);

        return map;
    }
}
