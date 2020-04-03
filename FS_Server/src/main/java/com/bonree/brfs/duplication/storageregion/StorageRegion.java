package com.bonree.brfs.duplication.storageregion;

import static com.google.common.base.MoreObjects.toStringHelper;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class StorageRegion {
    private final String name;
    private final int id;
    private final long createTime;
    private final StorageRegionProperties properties;
    
    public StorageRegion(
            String name, 
            int id,
            long createTime,
            StorageRegionProperties props) {
        this.name = name;
        this.id = id;
        this.createTime = createTime;
        this.properties = props;
    }

    /**
     * for compatibility of BRFS v1.0 
     * 
     * @param name
     * @param id
     * @param createTime
     * @param enable
     * @param replicateNum
     * @param dataTtl
     * @param fileCapacity
     * @param filePartitionDuration
     */
    @JsonCreator
    public StorageRegion(
            @JsonProperty("name") String name, 
            @JsonProperty("id") int id,
            @JsonProperty("create_time") long createTime,
            @JsonProperty("enable") boolean enable,
            @JsonProperty("replicate_num") int replicateNum,
            @JsonProperty("data_ttl") String dataTtl,
            @JsonProperty("file_capacity") long fileCapacity,
            @JsonProperty("patition_duration") String filePartitionDuration) {
        this.name = name;
        this.id = id;
        this.createTime = createTime;
        this.properties = new StorageRegionProperties(
                enable,
                replicateNum,
                dataTtl,
                fileCapacity,
                filePartitionDuration);
    }

    @JsonProperty("name")
    public String getName() {
        return name;
    }

    @JsonProperty("id")
    public int getId() {
        return id;
    }

    @JsonProperty("create_time")
    public long getCreateTime() {
        return createTime;
    }
    
    @JsonProperty("enable")
    public boolean isEnable() {
        return properties.isEnable();
    }

    @JsonProperty("replicate_num")
    public int getReplicateNum() {
        return properties.getReplicateNum();
    }

    @JsonProperty("data_ttl")
    public String getDataTtl() {
        return properties.getDataTtl();
    }

    @JsonProperty("file_capacity")
    public long getFileCapacity() {
        return properties.getFileCapacity();
    }

    @JsonProperty("patition_duration")
    public String getFilePartitionDuration() {
        return properties.getFilePartitionDuration();
    }

    public StorageRegionProperties getProperties() {
        return properties;
    }

    @Override
    public String toString() {
        return toStringHelper(getClass())
                .add("name", name)
                .add("id", id)
                .add("createTime", createTime)
                .add("props", properties)
                .toString();
    }
}
