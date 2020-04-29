package com.bonree.brfs.resource.vo;

public class GuiNetInfo {
    private long time;
    private String netDev;
    private long txBytesPs;
    private long rxBytesPs;

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public String getNetDev() {
        return netDev;
    }

    public void setNetDev(String netDev) {
        this.netDev = netDev;
    }

    public long getTxBytesPs() {
        return txBytesPs;
    }

    public void setTxBytesPs(long txBytesPs) {
        this.txBytesPs = txBytesPs;
    }

    public long getRxBytesPs() {
        return rxBytesPs;
    }

    public void setRxBytesPs(long rxBytesPs) {
        this.rxBytesPs = rxBytesPs;
    }
}
