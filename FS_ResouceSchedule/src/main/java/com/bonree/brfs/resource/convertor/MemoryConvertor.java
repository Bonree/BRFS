package com.bonree.brfs.resource.convertor;

import com.bonree.brfs.common.resource.vo.MemStat;
import org.hyperic.sigar.Mem;

public class MemoryConvertor {
    public MemStat convertoMemStat(Mem mem) {
        MemStat stat = new MemStat();
        stat.setTotal(mem.getTotal());
        stat.setRam(mem.getRam());
        stat.setUsed(mem.getUsed());
        stat.setFree(mem.getFree());
        stat.setActualUsed(mem.getActualUsed());
        stat.setActualFree(mem.getActualFree());
        stat.setUsedPercent(mem.getUsedPercent());
        stat.setFreePercent(mem.getFreePercent());
        return stat;
    }
}
