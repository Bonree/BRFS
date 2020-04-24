package com.bonree.brfs.resourceschedule.model;

import com.bonree.brfs.resourceschedule.utils.CalcUtils;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@JsonIgnoreProperties(ignoreUnknown = true)
public class StateMetaServerModel {
    /**
     * 统计计数器
     */
    private int calcCount = 1;
    /**
     * cpu内核数
     */
    private int cpuCoreCount = 1;
    /**
     * cpu使用率
     */
    private double cpuRate = 0.0;

    /**
     * 内存大小
     */
    private long memorySize = 0;

    /**
     * 内存使用率
     */
    private double memoryRate = 0.0;

    /**
     * 分区大小kb
     */
    private Map<String, Long> partitionTotalSizeMap = new ConcurrentHashMap<String, Long>();

    /**
     * 分区剩余kb
     */
    private Map<String, Long> partitionRemainSizeMap = new ConcurrentHashMap<String, Long>();

    /**
     * 分区写入kb
     */
    private Map<String, Long> partitionWriteByteMap = new ConcurrentHashMap<String, Long>();

    /**
     * 分区读取kb
     */
    private Map<String, Long> partitionReadByteMap = new ConcurrentHashMap<String, Long>();

    /**
     * 网卡发送字节数
     */
    private Map<String, Long> netTByteMap = new ConcurrentHashMap<String, Long>();

    /**
     * 网卡接收字节数
     */
    private Map<String, Long> netRByteMap = new ConcurrentHashMap<String, Long>();
    /**
     * 网卡发送字节数
     */
    private long netTByte;

    /**
     * 网卡接收字节数
     */
    private long netRByte;

    public StatServerModel converObject(StateMetaServerModel t1) {
        StatServerModel obj = new StatServerModel();
        int count = this.calcCount + t1.getCalcCount();
        // 1.不变的参数
        // cpu核心数
        obj.setCpuCoreCount(this.cpuCoreCount);
        // 内存大小
        obj.setMemorySize(this.memorySize);
        // 分区大小
        obj.setPartitionTotalSizeMap(this.partitionTotalSizeMap);

        // 2.分区剩余数据
        obj.setPartitionRemainSizeMap(this.partitionRemainSizeMap);

        // 3.汇总数据
        // 内存使用率
        double mrate = (this.memoryRate + t1.getMemoryRate()) / count;
        obj.setMemoryRate(mrate);
        // cpu使用率
        double crate = (this.cpuRate + t1.cpuRate) / count;
        obj.setCpuRate(crate);

        // 4.转换数据
        // 分区读取速度
        Map<String, Long> diskReadSpeedMap = CalcUtils.diffDataMap(this.partitionReadByteMap, t1.getPartitionReadByteMap());
        obj.setPartitionReadSpeedMap(diskReadSpeedMap);
        // 分区写入速度
        Map<String, Long> diskWriteSpeedMap = CalcUtils.diffDataMap(this.partitionWriteByteMap, t1.getPartitionWriteByteMap());
        obj.setPartitionWriteSpeedMap(diskWriteSpeedMap);

        // 网卡发送速度
        obj.setNetTSpeed(this.netTByte - t1.getNetTByte());
        // 网卡接收速度
        obj.setNetRSpeed(this.netRByte - t1.getNetRByte());
        obj.setCalcCount(count);
        return obj;
    }

    public int getCpuCoreCount() {
        return cpuCoreCount;
    }

    public void setCpuCoreCount(int cpuCoreCount) {
        this.cpuCoreCount = cpuCoreCount;
    }

    public double getCpuRate() {
        return cpuRate;
    }

    public void setCpuRate(double cpuRate) {
        this.cpuRate = cpuRate;
    }

    public long getMemorySize() {
        return memorySize;
    }

    public void setMemorySize(long memorySize) {
        this.memorySize = memorySize;
    }

    public double getMemoryRate() {
        return memoryRate;
    }

    public void setMemoryRate(double memoryRate) {
        this.memoryRate = memoryRate;
    }

    public Map<String, Long> getPartitionTotalSizeMap() {
        return partitionTotalSizeMap;
    }

    public void setPartitionTotalSizeMap(Map<String, Long> partitionTotalSizeMap) {
        this.partitionTotalSizeMap = partitionTotalSizeMap;
    }

    public Map<String, Long> getPartitionRemainSizeMap() {
        return partitionRemainSizeMap;
    }

    public void setPartitionRemainSizeMap(Map<String, Long> partitionRemainSizeMap) {
        this.partitionRemainSizeMap = partitionRemainSizeMap;
    }

    public Map<String, Long> getPartitionWriteByteMap() {
        return partitionWriteByteMap;
    }

    public void setPartitionWriteByteMap(Map<String, Long> partitionWriteByteMap) {
        this.partitionWriteByteMap = partitionWriteByteMap;
    }

    public Map<String, Long> getPartitionReadByteMap() {
        return partitionReadByteMap;
    }

    public void setPartitionReadByteMap(Map<String, Long> partitionReadByteMap) {
        this.partitionReadByteMap = partitionReadByteMap;
    }

    public Map<String, Long> getNetTByteMap() {
        return netTByteMap;
    }

    public void setNetTByteMap(Map<String, Long> netTByteMap) {
        this.netTByteMap = netTByteMap;
    }

    public Map<String, Long> getNetRByteMap() {
        return netRByteMap;
    }

    public void setNetRByteMap(Map<String, Long> netRByteMap) {
        this.netRByteMap = netRByteMap;
    }

    public int getCalcCount() {
        return calcCount;
    }

    public void setCalcCount(int calcCount) {
        this.calcCount = calcCount;
    }

    public long getNetTByte() {
        return netTByte;
    }

    public void setNetTByte(long netTByte) {
        this.netTByte = netTByte;
    }

    public long getNetRByte() {
        return netRByte;
    }

    public void setNetRByte(long netRByte) {
        this.netRByte = netRByte;
    }
}
