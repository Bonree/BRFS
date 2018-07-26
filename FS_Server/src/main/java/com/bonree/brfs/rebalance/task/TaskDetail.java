package com.bonree.brfs.rebalance.task;

import com.bonree.brfs.rebalance.DataRecover;
import com.bonree.brfs.rebalance.DataRecover.ExecutionStatus;
import com.fasterxml.jackson.annotation.JsonProperty;

public class TaskDetail {
    @JsonProperty("selfServerId")
    private String selfServerId;
    
    @JsonProperty("status")
    private DataRecover.ExecutionStatus status;
    
    @JsonProperty("totalDirectories")
    private int totalDirectories;
    
    @JsonProperty("curentCount")
    private int curentCount;
    
    @JsonProperty("process")
    private double process;

    @SuppressWarnings("unused")
    private TaskDetail() {
    }

    public TaskDetail(String selfServerId, ExecutionStatus status, int totalDirectories, int curentCount, double process) {
        this.selfServerId = selfServerId;
        this.status = status;
        this.totalDirectories = totalDirectories;
        this.curentCount = curentCount;
        this.process = process;
    }

    public String getSelfServerId() {
        return selfServerId;
    }

    public void setSelfServerId(String selfServerId) {
        this.selfServerId = selfServerId;
    }

    public DataRecover.ExecutionStatus getStatus() {
        return status;
    }

    public void setStatus(DataRecover.ExecutionStatus status) {
        this.status = status;
    }

    public int getTotalDirectories() {
        return totalDirectories;
    }

    public void setTotalDirectories(int totalDirectories) {
        this.totalDirectories = totalDirectories;
    }

    public int getCurentCount() {
        return curentCount;
    }

    public void setCurentCount(int curentCount) {
        this.curentCount = curentCount;
    }

    public double getProcess() {
        return process;
    }

    public void setProcess(double process) {
        this.process = process;
    }

    @Override
    public String toString() {
        return "TaskDetail [selfServerId=" + selfServerId + ", status=" + status + ", totalDirectories=" + totalDirectories + ", curentCount=" + curentCount + ", process=" + process + "]";
    }
    
    

}
