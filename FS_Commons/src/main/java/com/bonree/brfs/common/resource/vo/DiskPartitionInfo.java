package com.bonree.brfs.common.resource.vo;

public class DiskPartitionInfo {
    String dirName = null;
    String devName = null;
    String typeName = null;
    String sysTypeName = null;
    String options = null;
    int type = 0;
    long flags = 0L;
    public static final int TYPE_UNKNOWN = 0;
    public static final int TYPE_NONE = 1;
    public static final int TYPE_LOCAL_DISK = 2;
    public static final int TYPE_NETWORK = 3;
    public static final int TYPE_RAM_DISK = 4;
    public static final int TYPE_CDROM = 5;
    public static final int TYPE_SWAP = 6;

    public String getDirName() {
        return dirName;
    }

    public void setDirName(String dirName) {
        this.dirName = dirName;
    }

    public String getDevName() {
        return devName;
    }

    public void setDevName(String devName) {
        this.devName = devName;
    }

    public String getTypeName() {
        return typeName;
    }

    public void setTypeName(String typeName) {
        this.typeName = typeName;
    }

    public String getSysTypeName() {
        return sysTypeName;
    }

    public void setSysTypeName(String sysTypeName) {
        this.sysTypeName = sysTypeName;
    }

    public String getOptions() {
        return options;
    }

    public void setOptions(String options) {
        this.options = options;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public long getFlags() {
        return flags;
    }

    public void setFlags(long flags) {
        this.flags = flags;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("DiskPartitionInfo{");
        sb.append("dirName='").append(dirName).append('\'');
        sb.append(", devName='").append(devName).append('\'');
        sb.append(", typeName='").append(typeName).append('\'');
        sb.append(", sysTypeName='").append(sysTypeName).append('\'');
        sb.append(", options='").append(options).append('\'');
        sb.append(", type=").append(type);
        sb.append(", flags=").append(flags);
        sb.append('}');
        return sb.toString();
    }
}
