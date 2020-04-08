package com.bonree.brfs.resourceschedule.model;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.bonree.brfs.common.utils.BrStringUtils;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ResourceModel{
    /**
     * cpu使用率，本机
     */
    private double cpuRate = 0.0;
    /**
     * 内存使用率 本机
     */
    private double memoryRate = 0.0;
    /**
     *
     */
    private String serverId = "";
    /**
     * 硬盘大小
     */
    private long diskSize = 0;
    /**
     * 本机硬盘剩余率
     */
    private double diskRemainRate = 0.0;
    /**
     * cpu剩余值
     */
    private double cpuValue = 0.0;
    /**
     * 内存剩余值
     */
    private double memoryValue = 0.0;
    /**
     * 硬盘写剩余值
     */
    private double netRxValue = 0.0;
    private double netTxValue = 0.0;
    private String host = null;

    public String getHost(){
        return host;
    }

    public void setHost(String host){
        this.host = host;
    }

    private Map<String, Double> diskWriteValue = new ConcurrentHashMap<String, Double>();
    private Map<String, Double> diskReadValue = new ConcurrentHashMap<String, Double>();
    private Map<String, Double> diskRemainValue = new ConcurrentHashMap<String, Double>();
    private Map<String,Long> localSizeValue = new ConcurrentHashMap<>();

    private Map<String,Long> localRemainSizeValue = new ConcurrentHashMap<>();

    private Map<String, Double> localDiskRemainRate = new ConcurrentHashMap<>();


    /**
     * storagename与分区的映射关系
     */
    private Map<String, String> storageNameOnPartitionMap = new ConcurrentHashMap<String, String>();

    private Map<Integer, String> snIds = new ConcurrentHashMap<Integer, String>();

    public String getServerId(){
        return serverId;
    }

    public void setServerId(String serverId){
        this.serverId = serverId;
    }

    public double getDiskRemainRate(){
        return diskRemainRate;
    }

    public void setDiskRemainRate(double diskRemainRate){
        this.diskRemainRate = diskRemainRate;
    }

    public double getCpuValue(){
        return cpuValue;
    }

    public void setCpuValue(double cpuValue){
        this.cpuValue = cpuValue;
    }

    public double getMemoryValue(){
        return memoryValue;
    }

    public void setMemoryValue(double memoryValue){
        this.memoryValue = memoryValue;
    }

    public Map<String, Double> getDiskWriteValue(){
        return diskWriteValue;
    }

    public void setDiskWriteValue(Map<String, Double> diskWriteValue){
        this.diskWriteValue = diskWriteValue;
    }

    public Map<String, Double> getDiskReadValue(){
        return diskReadValue;
    }

    public void setDiskReadValue(Map<String, Double> diskReadValue){
        this.diskReadValue = diskReadValue;
    }

    public Map<String, Double> getDiskRemainValue(){
        return diskRemainValue;
    }

    public void setDiskRemainValue(Map<String, Double> diskRemainValue){
        this.diskRemainValue = diskRemainValue;
    }

    public double getCpuRate(){
        return cpuRate;
    }

    public void setCpuRate(double cpuRate){
        this.cpuRate = cpuRate;
    }

    public double getMemoryRate(){
        return memoryRate;
    }

    public void setMemoryRate(double memoryRate){
        this.memoryRate = memoryRate;
    }

    public long getDiskSize(){
        return diskSize;
    }

    public void setDiskSize(long diskSize){
        this.diskSize = diskSize;
    }

    public Map<String, String> getStorageNameOnPartitionMap(){
        return storageNameOnPartitionMap;
    }

    public void setStorageNameOnPartitionMap(Map<String, String> storageNameOnPartitionMap){
        this.storageNameOnPartitionMap = storageNameOnPartitionMap;
    }

    public double getDiskWriteValue(String storage){
        if(BrStringUtils.isEmpty(storage)) {
            return 0.0;
        }
        String mount = getMountedPoint(storage);
        if(BrStringUtils.isEmpty(mount)) {
            return 0.0;
        }
        if(!this.diskWriteValue.containsKey(mount)) {
            return 0.0;
        }
        return this.diskWriteValue.get(mount);

    }

    public Map<String, Long> getLocalSizeValue(){
        return localSizeValue;
    }

    public void setLocalSizeValue(Map<String, Long> localSizeValue){
        this.localSizeValue = localSizeValue;
    }

    public Map<String, Long> getLocalRemainSizeValue(){
        return localRemainSizeValue;
    }
    public long getLocalRemainSizeValue(String sn){
        if(BrStringUtils.isEmpty(sn)) {
            return diskSize;
        }
        String mount = getMountedPoint(sn);
        if(BrStringUtils.isEmpty(mount)){
            return diskSize;
        }
        if(!localRemainSizeValue.containsKey(mount)){
            return diskSize;
        }
        return this.getLocalRemainSizeValue().get(mount);
    }
    public void setLocalRemainSizeValue(Map<String, Long> localRemainSizeValue){
        this.localRemainSizeValue = localRemainSizeValue;
    }

    public double getDiskReadValue(String storage){
        if(BrStringUtils.isEmpty(storage)) {
            return 0.0;
        }
        String mount = getMountedPoint(storage);
        if(BrStringUtils.isEmpty(mount)) {
            return 0.0;
        }
        if(!this.diskReadValue.containsKey(mount)) {
            return 0.0;
        }
        return this.diskReadValue.get(mount);

    }

    public double getDiskRemainValue(String storage){
        if(BrStringUtils.isEmpty(storage)) {
            return 0.0;
        }
        String mount = getMountedPoint(storage);
        if(BrStringUtils.isEmpty(mount)) {
            return this.diskRemainRate;
        }
        if(!this.diskRemainValue.containsKey(mount)) {
            return this.diskRemainRate;
        }
        return this.diskRemainValue.get(mount);

    }


    public String getMountedPoint(String storage){
        if(BrStringUtils.isEmpty(storage)) {
            return null;
        }
        if(!this.storageNameOnPartitionMap.containsKey(storage)) {
            return null;
        }
        return this.storageNameOnPartitionMap.get(storage);
    }

    public Map<String, Double> getLocalDiskRemainRate(){
        return localDiskRemainRate;
    }

    public void setLocalDiskRemainRate(Map<String, Double> localDiskRemainRate){
        this.localDiskRemainRate = localDiskRemainRate;
    }
    public double getLocalDiskRemainRate(String path){
        if(!this.storageNameOnPartitionMap.containsKey(path)){
            return diskSize;
        }
        String mountPoint = this.storageNameOnPartitionMap.get(path);
        if(BrStringUtils.isEmpty(mountPoint)){
            return diskSize;
        }
        return this.localDiskRemainRate.get(mountPoint);
    }
    public double getNetRxValue(){
        return netRxValue;
    }

    public void setNetRxValue(double netRxValue){
        this.netRxValue = netRxValue;
    }

    public double getNetTxValue(){
        return netTxValue;
    }

    public void setNetTxValue(double netTxValue){
        this.netTxValue = netTxValue;
    }

    public Map<Integer, String> getSnIds(){
        return snIds;
    }

    public void setSnIds(Map<Integer, String> snIds){
        this.snIds = snIds;
    }
}
