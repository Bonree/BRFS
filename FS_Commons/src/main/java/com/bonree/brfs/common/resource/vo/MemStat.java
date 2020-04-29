package com.bonree.brfs.common.resource.vo;

public class MemStat {
    private long total = 0L;
    private long ram = 0L;
    private long used = 0L;
    private long free = 0L;
    private long actualUsed = 0L;
    private long actualFree = 0L;
    private double usedPercent = 0.0D;
    private double freePercent = 0.0D;

    public void setTotal(long total) {
        this.total = total;
    }

    public void setRam(long ram) {
        this.ram = ram;
    }

    public void setUsed(long used) {
        this.used = used;
    }

    public void setFree(long free) {
        this.free = free;
    }

    public void setActualUsed(long actualUsed) {
        this.actualUsed = actualUsed;
    }

    public void setActualFree(long actualFree) {
        this.actualFree = actualFree;
    }

    public void setUsedPercent(double usedPercent) {
        this.usedPercent = usedPercent;
    }

    public void setFreePercent(double freePercent) {
        this.freePercent = freePercent;
    }

    public long getTotal() {
        return total;
    }

    public long getRam() {
        return ram;
    }

    public long getUsed() {
        return used;
    }

    public long getFree() {
        return free;
    }

    public long getActualUsed() {
        return actualUsed;
    }

    public long getActualFree() {
        return actualFree;
    }

    public double getUsedPercent() {
        return usedPercent;
    }

    public double getFreePercent() {
        return freePercent;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("MemStat{");
        sb.append("total=").append(total);
        sb.append(", ram=").append(ram);
        sb.append(", used=").append(used);
        sb.append(", free=").append(free);
        sb.append(", actualUsed=").append(actualUsed);
        sb.append(", actualFree=").append(actualFree);
        sb.append(", usedPercent=").append(usedPercent);
        sb.append(", freePercent=").append(freePercent);
        sb.append('}');
        return sb.toString();
    }
}
