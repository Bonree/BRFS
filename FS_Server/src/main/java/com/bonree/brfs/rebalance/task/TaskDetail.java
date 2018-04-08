package com.bonree.brfs.rebalance.task;


public class TaskDetail {
    private String selfServerId;
    private TaskStatus status;
    private int totalDirectories;
    private int curentCount;
    private double process;

    public String getSelfServerId() {
        return selfServerId;
    }

    public void setSelfServerId(String selfServerId) {
        this.selfServerId = selfServerId;
    }

    public TaskStatus getStatus() {
        return status;
    }

    public void setStatus(TaskStatus status) {
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
    

}
