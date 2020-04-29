package com.bonree.brfs.resource.gather;

import com.bonree.brfs.common.process.LifeCycle;
import com.bonree.brfs.common.resource.vo.CPUInfo;
import com.bonree.brfs.common.resource.vo.CpuStat;

public interface CPUGather extends LifeCycle {
    CPUInfo gatherCpuInfo() throws Exception;

    CpuStat gatherCpuStat() throws Exception;
}
