package com.bonree.brfs.common.supervisor;

import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.common.utils.JsonUtils.JsonException;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.HashMap;
import java.util.Map;

public class ReadMetric {
    @JsonProperty("monitor_time")
    private long monitorTime;
    @JsonProperty("storage_name")
    private String storageName;
    @JsonProperty("data_node_id")
    private String dataNodeId;
    @JsonProperty("data_count")
    private int dataCount;
    @JsonProperty("data_size")
    private int dataSize;
    @JsonProperty("elapsed_time")
    private int elapsedTime;

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

    public String getDataNodeId() {
        return dataNodeId;
    }

    public void setDataNodeId(String dataNodeId) {
        this.dataNodeId = dataNodeId;
    }

    public int getDataCount() {
        return dataCount;
    }

    public void setDataCount(int dataCount) {
        this.dataCount = dataCount;
    }

    public int getDataSize() {
        return dataSize;
    }

    public void setDataSize(int dataSize) {
        this.dataSize = dataSize;
    }

    public int getElapsedTime() {
        return elapsedTime;
    }

    public void setElapsedTime(int elapsedTime) {
        this.elapsedTime = elapsedTime;
    }

    public String toJsonString() throws JsonException {
        return JsonUtils.toJsonString(this);
    }

    public Map<String, Object> toMap() {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("monitor_time", monitorTime);
        map.put("storage_name", storageName);
        map.put("data_node_id", dataNodeId);
        map.put("data_count", dataCount);
        map.put("data_size", dataSize);
        map.put("elapsed_time", elapsedTime);

        return map;
    }
}
