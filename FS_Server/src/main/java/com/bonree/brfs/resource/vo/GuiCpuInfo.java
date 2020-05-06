package com.bonree.brfs.resource.vo;

public class GuiCpuInfo {
    private long time;
    private double total;
    private double system;
    private double user;
    private double steal;
    private double iowait;

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public double getTotal() {
        return total;
    }

    public void setTotal(double total) {
        this.total = total;
    }

    public double getSystem() {
        return system;
    }

    public void setSystem(double system) {
        this.system = system;
    }

    public double getUser() {
        return user;
    }

    public void setUser(double user) {
        this.user = user;
    }

    public double getSteal() {
        return steal;
    }

    public void setSteal(double steal) {
        this.steal = steal;
    }

    public double getIowait() {
        return iowait;
    }

    public void setIowait(double iowait) {
        this.iowait = iowait;
    }
}
