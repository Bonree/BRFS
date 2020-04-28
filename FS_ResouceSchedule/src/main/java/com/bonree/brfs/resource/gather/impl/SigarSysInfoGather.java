package com.bonree.brfs.resource.gather.impl;

import com.bonree.brfs.common.resource.vo.OSInfo;
import com.bonree.brfs.resource.convertor.SwapConvertor;
import com.bonree.brfs.resource.convertor.SysInfoConvertor;
import com.bonree.brfs.resource.gather.SysInfoGather;
import org.hyperic.sigar.OperatingSystem;
import org.hyperic.sigar.Sigar;

public class SigarSysInfoGather implements SysInfoGather {
    private SysInfoConvertor convertor;

    public SigarSysInfoGather() {

    }

    @Override
    public OSInfo gatherOSInfo() throws Exception {
        OperatingSystem opSys = OperatingSystem.getInstance();
        return this.convertor.convertSysInfo(opSys);
    }
    @Override
    public void start() throws Exception {

        this.convertor = new SysInfoConvertor();
    }

    @Override
    public void stop() throws Exception {

    }
}
