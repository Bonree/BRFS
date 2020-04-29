package com.bonree.brfs.common.resource.vo;

public class NetStat {
    long rxBytes = 0L;
    long rxPackets = 0L;
    long rxErrors = 0L;
    long rxDropped = 0L;
    long rxOverruns = 0L;
    long rxFrame = 0L;
    long txBytes = 0L;
    long txPackets = 0L;
    long txErrors = 0L;
    long txDropped = 0L;
    long txOverruns = 0L;
    long txCollisions = 0L;
    long txCarrier = 0L;
    long speed = 0L;

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
        final StringBuilder sb = new StringBuilder("{");
        sb.append("rxBytes=").append(rxBytes);
        sb.append(", rxPackets=").append(rxPackets);
        sb.append(", rxErrors=").append(rxErrors);
        sb.append(", rxDropped=").append(rxDropped);
        sb.append(", rxOverruns=").append(rxOverruns);
        sb.append(", rxFrame=").append(rxFrame);
        sb.append(", txBytes=").append(txBytes);
        sb.append(", txPackets=").append(txPackets);
        sb.append(", txErrors=").append(txErrors);
        sb.append(", txDropped=").append(txDropped);
        sb.append(", txOverruns=").append(txOverruns);
        sb.append(", txCollisions=").append(txCollisions);
        sb.append(", txCarrier=").append(txCarrier);
        sb.append(", speed=").append(speed);
        sb.append('}');
        return sb.toString();
    }
}
