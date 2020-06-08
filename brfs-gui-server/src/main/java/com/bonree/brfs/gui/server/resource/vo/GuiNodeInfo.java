package com.bonree.brfs.gui.server.resource.vo;

public class GuiNodeInfo {
    private String id;
    private String host;
    private int cpuCores;
    private String cpuBrand;
    private long totalMemSize;
    private String os;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public int getCpuCores() {
        return cpuCores;
    }

    public void setCpuCores(int cpuCores) {
        this.cpuCores = cpuCores;
    }

    public String getCpuBrand() {
        return cpuBrand;
    }

    public void setCpuBrand(String cpuBrand) {
        this.cpuBrand = cpuBrand;
    }

    public long getTotalMemSize() {
        return totalMemSize;
    }

    public void setTotalMemSize(long totalMemSize) {
        this.totalMemSize = totalMemSize;
    }

    public String getOs() {
        return os;
    }

    public void setOs(String os) {
        this.os = os;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }
}
