package com.bonree.brfs.gui.server;

import com.fasterxml.jackson.annotation.JsonProperty;

public class AlertConfig {
    @JsonProperty("cpu.usage.percent")
    private double alertLineCpuPercent = 70;
    @JsonProperty("memory.usage.percent")
    private double alertLineMemPercent = 60;
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
}
