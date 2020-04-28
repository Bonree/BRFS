package com.bonree.brfs.common.resource.vo;

public class CPUInfo {
    String vendor = null;
    String model = null;
    int mhz = 0;
    long cacheSize = 0L;
    int totalCores = 0;
    int totalSockets = 0;
    int coresPerSocket = 0;
    /**
     *
     */
    private int physicalCpuNum;
    /**
     * 核心数
     */
    private int coresNum;

    public String getVendor() {
        return vendor;
    }

    public void setVendor(String vendor) {
        this.vendor = vendor;
    }

    public String getModel() {
        return model;
    }

    public void setModel(String model) {
        this.model = model;
    }

    public int getMhz() {
        return mhz;
    }

    public void setMhz(int mhz) {
        this.mhz = mhz;
    }

    public long getCacheSize() {
        return cacheSize;
    }

    public void setCacheSize(long cacheSize) {
        this.cacheSize = cacheSize;
    }

    public int getTotalCores() {
        return totalCores;
    }

    public void setTotalCores(int totalCores) {
        this.totalCores = totalCores;
    }

    public int getTotalSockets() {
        return totalSockets;
    }

    public void setTotalSockets(int totalSockets) {
        this.totalSockets = totalSockets;
    }

    public int getCoresPerSocket() {
        return coresPerSocket;
    }

    public void setCoresPerSocket(int coresPerSocket) {
        this.coresPerSocket = coresPerSocket;
    }

    public int getPhysicalCpuNum() {
        return physicalCpuNum;
    }

    public void setPhysicalCpuNum(int physicalCpuNum) {
        this.physicalCpuNum = physicalCpuNum;
    }

    public int getCoresNum() {
        return coresNum;
    }

    public void setCoresNum(int coresNum) {
        this.coresNum = coresNum;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("{");
        sb.append("vendor='").append(vendor).append('\'');
        sb.append(", model='").append(model).append('\'');
        sb.append(", mhz=").append(mhz);
        sb.append(", cacheSize=").append(cacheSize);
        sb.append(", totalCores=").append(totalCores);
        sb.append(", totalSockets=").append(totalSockets);
        sb.append(", coresPerSocket=").append(coresPerSocket);
        sb.append(", physicalCpuNum=").append(physicalCpuNum);
        sb.append(", coresNum=").append(coresNum);
        sb.append('}');
        return sb.toString();
    }
}
