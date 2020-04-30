package com.bonree.brfs.resource.gather.impl;

import com.bonree.brfs.common.resource.vo.SwapStat;
import com.bonree.brfs.resource.convertor.SwapConvertor;
import com.bonree.brfs.resource.gather.SwapGather;
import org.hyperic.sigar.Sigar;
import org.hyperic.sigar.Swap;

public class SigarSwapGather implements SwapGather {
    private Sigar sigar;
    private SwapConvertor convertor;

    public SigarSwapGather() {
    }

    @Override
    public SwapStat gatherSwap() throws Exception {
        Swap swap = sigar.getSwap();
        return this.convertor.convert(swap);
    }

    @Override
    public void start() throws Exception {
        this.sigar = new Sigar();
        this.convertor = new SwapConvertor();
    }

    @Override
    public void stop() throws Exception {
        if (this.sigar != null) {
            this.sigar.close();
        }
    }
}
