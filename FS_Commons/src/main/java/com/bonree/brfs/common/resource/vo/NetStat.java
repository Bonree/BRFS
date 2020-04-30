package com.bonree.brfs.common.resource.vo;

import com.google.common.base.MoreObjects;

public class NetStat {
    private String devName;
    private String ipAddress;
    private long rxBytes = 0L;
    private long rxPackets = 0L;
    private long rxErrors = 0L;
    private long rxDropped = 0L;
    private long rxOverruns = 0L;
    private long rxFrame = 0L;
    private long txBytes = 0L;
    private long txPackets = 0L;
    private long txErrors = 0L;
    private long txDropped = 0L;
    private long txOverruns = 0L;
    private long txCollisions = 0L;
    private long txCarrier = 0L;
    private long speed = 0L;

    public String getDevName() {
        return devName;
    }

    public void setDevName(String devName) {
        this.devName = devName;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public void setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
    }

    public long getRxBytes() {
        return rxBytes;
    }

    public void setRxBytes(long rxBytes) {
        this.rxBytes = rxBytes;
    }

    public long getRxPackets() {
        return rxPackets;
    }

    public void setRxPackets(long rxPackets) {
        this.rxPackets = rxPackets;
    }

    public long getRxErrors() {
        return rxErrors;
    }

    public void setRxErrors(long rxErrors) {
        this.rxErrors = rxErrors;
    }

    public long getRxDropped() {
        return rxDropped;
    }

    public void setRxDropped(long rxDropped) {
        this.rxDropped = rxDropped;
    }

    public long getRxOverruns() {
        return rxOverruns;
    }

    public void setRxOverruns(long rxOverruns) {
        this.rxOverruns = rxOverruns;
    }

    public long getRxFrame() {
        return rxFrame;
    }

    public void setRxFrame(long rxFrame) {
        this.rxFrame = rxFrame;
    }

    public long getTxBytes() {
        return txBytes;
    }

    public void setTxBytes(long txBytes) {
        this.txBytes = txBytes;
    }

    public long getTxPackets() {
        return txPackets;
    }

    public void setTxPackets(long txPackets) {
        this.txPackets = txPackets;
    }

    public long getTxErrors() {
        return txErrors;
    }

    public void setTxErrors(long txErrors) {
        this.txErrors = txErrors;
    }

    public long getTxDropped() {
        return txDropped;
    }

    public void setTxDropped(long txDropped) {
        this.txDropped = txDropped;
    }

    public long getTxOverruns() {
        return txOverruns;
    }

    public void setTxOverruns(long txOverruns) {
        this.txOverruns = txOverruns;
    }

    public long getTxCollisions() {
        return txCollisions;
    }

    public void setTxCollisions(long txCollisions) {
        this.txCollisions = txCollisions;
    }

    public long getTxCarrier() {
        return txCarrier;
    }

    public void setTxCarrier(long txCarrier) {
        this.txCarrier = txCarrier;
    }

    public long getSpeed() {
        return speed;
    }

    public void setSpeed(long speed) {
        this.speed = speed;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                          .add("devName", devName)
                          .add("ipAddress", ipAddress)
                          .add("rxBytes", rxBytes)
                          .add("rxPackets", rxPackets)
                          .add("rxErrors", rxErrors)
                          .add("rxDropped", rxDropped)
                          .add("rxOverruns", rxOverruns)
                          .add("rxFrame", rxFrame)
                          .add("txBytes", txBytes)
                          .add("txPackets", txPackets)
                          .add("txErrors", txErrors)
                          .add("txDropped", txDropped)
                          .add("txOverruns", txOverruns)
                          .add("txCollisions", txCollisions)
                          .add("txCarrier", txCarrier)
                          .add("speed", speed)
                          .toString();
    }
}
