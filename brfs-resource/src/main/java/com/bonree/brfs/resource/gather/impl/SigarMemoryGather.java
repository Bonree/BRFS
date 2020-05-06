package com.bonree.brfs.resource.gather.impl;

import com.bonree.brfs.common.resource.vo.MemStat;
import com.bonree.brfs.resource.convertor.MemoryConvertor;
import com.bonree.brfs.resource.gather.MemoryGather;
import com.bonree.brfs.resource.utils.SigarUtil;
import org.hyperic.sigar.Mem;
import org.hyperic.sigar.Sigar;

public class SigarMemoryGather implements MemoryGather {
    private Sigar sigar = null;
    private MemoryConvertor convertor = null;

    public SigarMemoryGather() {
    }

    @Override
    public MemStat gatherMemStat() throws Exception {
        Mem mem = sigar.getMem();
        return this.convertor.convertoMemStat(mem);
    }

    @Override
    public void start() throws Exception {
        this.sigar = SigarUtil.getSigar();
        this.convertor = new MemoryConvertor();
    }

    @Override
    public void stop() throws Exception {
        if (this.sigar != null) {
            this.sigar.close();
        }
    }
}
