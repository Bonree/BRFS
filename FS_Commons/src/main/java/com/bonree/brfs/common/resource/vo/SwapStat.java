package com.bonree.brfs.common.resource.vo;

public class SwapStat {
    long total = 0L;
    long used = 0L;
    long free = 0L;
    long pageIn = 0L;
    long pageOut = 0L;

    public void setTotal(long total) {
        this.total = total;
    }

    public void setUsed(long used) {
        this.used = used;
    }

    public void setFree(long free) {
        this.free = free;
    }

    public void setPageIn(long pageIn) {
        this.pageIn = pageIn;
    }

    public void setPageOut(long pageOut) {
        this.pageOut = pageOut;
    }

    public long getTotal() {
        return total;
    }

    public long getUsed() {
        return used;
    }

    public long getFree() {
        return free;
    }

    public long getPageIn() {
        return pageIn;
    }

    public long getPageOut() {
        return pageOut;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("{");
        sb.append("total=").append(total);
        sb.append(", used=").append(used);
        sb.append(", free=").append(free);
        sb.append(", pageIn=").append(pageIn);
        sb.append(", pageOut=").append(pageOut);
        sb.append('}');
        return sb.toString();
    }
}
