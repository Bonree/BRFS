package com.bonree.brfs.resource.gather.impl;

import com.bonree.brfs.common.resource.vo.CPUInfo;
import com.bonree.brfs.common.resource.vo.CpuStat;
import com.bonree.brfs.resource.convertor.CpuConvertor;
import com.bonree.brfs.resource.gather.CPUGather;
import com.bonree.brfs.resource.utils.SigarUtil;
import org.hyperic.sigar.CpuInfo;
import org.hyperic.sigar.CpuPerc;
import org.hyperic.sigar.Sigar;

public class SigarCpuGather implements CPUGather {
    private Sigar sigar = null;
    private CpuConvertor cpuConvertor = null;

    public SigarCpuGather() {

    }

    @Override
    public CPUInfo gatherCpuInfo() throws Exception {
        CpuInfo info = (sigar.getCpuInfoList())[0];
        return this.cpuConvertor.convertCpuInfo(info);
    }

    @Override
    public CpuStat gatherCpuStat() throws Exception {
        CpuPerc perc = sigar.getCpuPerc();
        return this.cpuConvertor.convertCpuStat(perc);
    }

    @Override
    public void start() throws Exception {
        this.sigar = SigarUtil.getSigar();
        this.cpuConvertor = new CpuConvertor();
    }

    @Override
    public void stop() throws Exception {
        if (this.sigar != null) {
            this.sigar.close();
        }
    }
}
