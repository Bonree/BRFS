package com.bonree.brfs.common.resource.vo;

public class NetInfo {
    private String name = null;
    private String hwaddr = null;
    private String type = null;
    private String description = null;
    private String address = null;
    private String destination = null;
    private String broadcast = null;
    private String netmask = null;
    private long flags = 0L;
    private long mtu = 0L;
    private long metric = 0L;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getHwaddr() {
        return hwaddr;
    }

    public void setHwaddr(String hwaddr) {
        this.hwaddr = hwaddr;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getDestination() {
        return destination;
    }

    public void setDestination(String destination) {
        this.destination = destination;
    }

    public String getBroadcast() {
        return broadcast;
    }

    public void setBroadcast(String broadcast) {
        this.broadcast = broadcast;
    }

    public String getNetmask() {
        return netmask;
    }

    public void setNetmask(String netmask) {
        this.netmask = netmask;
    }

    public long getFlags() {
        return flags;
    }

    public void setFlags(long flags) {
        this.flags = flags;
    }

    public long getMtu() {
        return mtu;
    }

    public void setMtu(long mtu) {
        this.mtu = mtu;
    }

    public long getMetric() {
        return metric;
    }

    public void setMetric(long metric) {
        this.metric = metric;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("NetInfo{");
        sb.append("name='").append(name).append('\'');
        sb.append(", hwaddr='").append(hwaddr).append('\'');
        sb.append(", type='").append(type).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", address='").append(address).append('\'');
        sb.append(", destination='").append(destination).append('\'');
        sb.append(", broadcast='").append(broadcast).append('\'');
        sb.append(", netmask='").append(netmask).append('\'');
        sb.append(", flags=").append(flags);
        sb.append(", mtu=").append(mtu);
        sb.append(", metric=").append(metric);
        sb.append('}');
        return sb.toString();
    }
}
