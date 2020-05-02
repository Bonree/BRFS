package com.bonree.brfs.resource.gather.impl;

import com.bonree.brfs.common.resource.vo.Load;
import com.bonree.brfs.resource.convertor.LoadConvertor;
import com.bonree.brfs.resource.gather.LoadGather;
import com.bonree.brfs.resource.utils.SigarUtil;
import org.hyperic.sigar.Sigar;

public class SigarLoadGather implements LoadGather {
    private Sigar sigar;
    private LoadConvertor convertor;

    @Override
    public Load gather() throws Exception {
        return this.convertor.convetoLoad(this.sigar.getLoadAverage());
    }

    @Override
    public void start() throws Exception {
        this.sigar = SigarUtil.getSigar();
        this.convertor = new LoadConvertor();
    }

    @Override
    public void stop() throws Exception {
        if (this.sigar != null) {
            this.sigar.close();
        }
    }
}
