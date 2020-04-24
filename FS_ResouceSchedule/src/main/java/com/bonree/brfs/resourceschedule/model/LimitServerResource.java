package com.bonree.brfs.resourceschedule.model;

import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.CommonConfigs;
import com.bonree.brfs.configuration.units.DataNodeConfigs;
import com.bonree.brfs.configuration.units.ResourceConfigs;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class LimitServerResource {
    private double diskRemainRate = Configs.getConfiguration().GetConfig(ResourceConfigs.CONFIG_LIMIT_DISK_AVAILABLE_RATE);
    private double forceDiskRemainRate =
        Configs.getConfiguration().GetConfig(ResourceConfigs.CONFIG_LIMIT_FORCE_DISK_AVAILABLE_RATE);
    private double diskWriteValue = Configs.getConfiguration().GetConfig(ResourceConfigs.CONFIG_LIMIT_FORCE_DISK_WRITE_SPEED);
    private double forceWriteValue = Configs.getConfiguration().GetConfig(ResourceConfigs.CONFIG_LIMIT_DISK_WRITE_SPEED);
    private long remainWarnSize = Configs.getConfiguration().GetConfig(ResourceConfigs.CONFIG_LIMIT_DISK_REMAIN_SIZE);
    private long remainForceSize = Configs.getConfiguration().GetConfig(ResourceConfigs.CONFIG_LIMIT_FORCE_DISK_REMAIN_SIZE);
    private int centSize = Configs.getConfiguration().GetConfig(ResourceConfigs.CONFIG_RESOURCE_CENT_SIZE);
    private long fileSize = Configs.getConfiguration().GetConfig(DataNodeConfigs.CONFIG_FILE_MAX_CAPACITY) / 1024;
    private String diskGroup = Configs.getConfiguration().GetConfig(CommonConfigs.CONFIG_DATA_SERVICE_GROUP_NAME);

    public double getForceWriteValue() {
        return forceWriteValue;
    }

    public void setForceWriteValue(double forceWriteValue) {
        this.forceWriteValue = forceWriteValue;
    }

    public double getDiskWriteValue() {
        return diskWriteValue;
    }

    public void setDiskWriteValue(double diskWriteValue) {
        this.diskWriteValue = diskWriteValue;
    }

    public double getDiskRemainRate() {
        return diskRemainRate;
    }

    public void setDiskRemainRate(double remainValue) {
        this.diskRemainRate = remainValue;
    }

    public double getForceDiskRemainRate() {
        return forceDiskRemainRate;
    }

    public void setForceDiskRemainRate(double forceDiskRemainRate) {
        this.forceDiskRemainRate = forceDiskRemainRate;
    }

    public long getRemainWarnSize() {
        return remainWarnSize;
    }

    public void setRemainWarnSize(long remainWarnSize) {
        this.remainWarnSize = remainWarnSize;
    }

    public long getRemainForceSize() {
        return remainForceSize;
    }

    public void setRemainForceSize(long remainForceSize) {
        this.remainForceSize = remainForceSize;
    }

    public int getCentSize() {
        return centSize;
    }

    public void setCentSize(int centSize) {
        this.centSize = centSize;
    }

    public long getFileSize() {
        return fileSize;
    }

    public void setFileSize(long fileSize) {
        this.fileSize = fileSize;
    }

    public String getDiskGroup() {
        return diskGroup;
    }

    public void setDiskGroup(String diskGroup) {
        this.diskGroup = diskGroup;
    }
}
