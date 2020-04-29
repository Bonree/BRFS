package com.bonree.brfs.common.resource.vo;

/**
 * 系统信息
 */
public class OSInfo {
    private String hostName;
    /**
     * 操作系统描述信息
     */
    private String osDescription;
    /**
     * 操作系统名字
     */
    private String osName;
    /**
     * 操作系统架构
     */
    private String osArch;
    /**
     * 操作系统版本
     */
    private String osVersion;
    /**
     * 操作系统厂商
     */
    private String osVendor;
    /**
     * 操作系统厂商版本
     */
    private String osVendorVersion;
    /**
     * 操作系统代号
     */
    private String osCodeName;
    /**
     * 操作系统位数
     */
    private String osDataModel;
    /**
     * 操作系统cpu字节序列
     */
    private String osCpuEndian;

    public String getHostName() {
        return hostName;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    public String getOsDescription() {
        return osDescription;
    }

    public void setOsDescription(String osDescription) {
        this.osDescription = osDescription;
    }

    public String getOsName() {
        return osName;
    }

    public void setOsName(String osName) {
        this.osName = osName;
    }

    public String getOsArch() {
        return osArch;
    }

    public void setOsArch(String osArch) {
        this.osArch = osArch;
    }

    public String getOsVersion() {
        return osVersion;
    }

    public void setOsVersion(String osVersion) {
        this.osVersion = osVersion;
    }

    public String getOsVendor() {
        return osVendor;
    }

    public void setOsVendor(String osVendor) {
        this.osVendor = osVendor;
    }

    public String getOsVendorVersion() {
        return osVendorVersion;
    }

    public void setOsVendorVersion(String osVendorVersion) {
        this.osVendorVersion = osVendorVersion;
    }

    public String getOsCodeName() {
        return osCodeName;
    }

    public void setOsCodeName(String osCodeName) {
        this.osCodeName = osCodeName;
    }

    public String getOsDataModel() {
        return osDataModel;
    }

    public void setOsDataModel(String osDataModel) {
        this.osDataModel = osDataModel;
    }

    public String getOsCpuEndian() {
        return osCpuEndian;
    }

    public void setOsCpuEndian(String osCpuEndian) {
        this.osCpuEndian = osCpuEndian;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("OSInfo{");
        sb.append("hostName='").append(hostName).append('\'');
        sb.append(", osDescription='").append(osDescription).append('\'');
        sb.append(", osName='").append(osName).append('\'');
        sb.append(", osArch='").append(osArch).append('\'');
        sb.append(", osVersion='").append(osVersion).append('\'');
        sb.append(", osVendor='").append(osVendor).append('\'');
        sb.append(", osVendorVersion='").append(osVendorVersion).append('\'');
        sb.append(", osCodeName='").append(osCodeName).append('\'');
        sb.append(", osDataModel='").append(osDataModel).append('\'');
        sb.append(", osCpuEndian='").append(osCpuEndian).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
