package com.bonree.brfs.gui.server;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;

public class AlertConfig {
    @JsonProperty("cpu.usage.percent")
    private double alertLineCpuPercent = 70;
    @JsonProperty("memory.usage.percent")
    private double alertLineMemPercent = 70;
    @JsonProperty("data.disk.usage.percent")
    private double alertLineDataDiskPercent = 80;

    public AlertConfig() {
    }

    public double getAlertLineCpuPercent() {
        return alertLineCpuPercent;
    }

    public double getAlertLineMemPercent() {
        return alertLineMemPercent;
    }

    public double getAlertLineDataDiskPercent() {
        return alertLineDataDiskPercent;
    }

    public void setAlertLineCpuPercent(double alertLineCpuPercent) {
        this.alertLineCpuPercent = alertLineCpuPercent;
    }

    public void setAlertLineMemPercent(double alertLineMemPercent) {
        this.alertLineMemPercent = alertLineMemPercent;
    }

    public void setAlertLineDataDiskPercent(double alertLineDataDiskPercent) {
        this.alertLineDataDiskPercent = alertLineDataDiskPercent;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                          .add("alertLineCpuPercent", alertLineCpuPercent)
                          .add("alertLineMemPercent", alertLineMemPercent)
                          .add("alertLineDataDiskPercent", alertLineDataDiskPercent)
                          .toString();
    }
}
