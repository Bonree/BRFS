package com.bonree.brfs.common.resource.vo;


public class MemorySwapInfo {
    private long totalMemorySize = 0L;
    private long totalSwapSize = 0L;

    public long getTotalMemorySize() {
        return totalMemorySize;
    }

    public void setTotalMemorySize(long totalMemorySize) {
        this.totalMemorySize = totalMemorySize;
    }

    public long getTotalSwapSize() {
        return totalSwapSize;
    }

    public void setTotalSwapSize(long totalSwapSize) {
        this.totalSwapSize = totalSwapSize;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("MemorySwapInfo{");
        sb.append("totalMemorySize=").append(totalMemorySize);
        sb.append(", totalSwapSize=").append(totalSwapSize);
        sb.append('}');
        return sb.toString();
    }
}
