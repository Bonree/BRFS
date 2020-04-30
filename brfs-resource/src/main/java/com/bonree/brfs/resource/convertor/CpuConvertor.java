package com.bonree.brfs.resource.convertor;

import com.bonree.brfs.common.resource.vo.CPUInfo;
import com.bonree.brfs.common.resource.vo.CpuStat;
import org.hyperic.sigar.CpuInfo;
import org.hyperic.sigar.CpuPerc;

public class CpuConvertor {
    public CpuStat convertCpuStat(CpuPerc perc) {
        CpuStat stat = new CpuStat();
        stat.setCombined(perc.getCombined());
        stat.setIdle(perc.getIdle());
        stat.setIrq(perc.getIrq());
        stat.setNice(perc.getNice());
        stat.setSoftIrq(perc.getSoftIrq());
        stat.setStolen(perc.getStolen());
        stat.setSys(perc.getSys());
        stat.setTotal(1 - perc.getIdle() - perc.getWait() - perc.getStolen());
        stat.setUser(perc.getUser());
        stat.setWait(perc.getWait());
        return stat;
    }

    public CPUInfo convertCpuInfo(CpuInfo net) {
        CPUInfo stat = new CPUInfo();
        stat.setVendor(net.getVendor());
        stat.setModel(net.getModel());
        stat.setMhz(net.getMhz());
        stat.setCacheSize(net.getCacheSize());
        stat.setTotalCores(net.getTotalCores());
        stat.setTotalSockets(net.getTotalSockets());
        stat.setCoresPerSocket(net.getCoresPerSocket());
        if (net.getTotalCores() != net.getTotalSockets() || net.getCoresPerSocket() > net.getTotalCores()) {
            stat.setPhysicalCpuNum(net.getTotalSockets());
            stat.setCoresNum(net.getCoresPerSocket());
        }
        return stat;

    }
}
