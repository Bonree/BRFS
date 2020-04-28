package com.bonree.brfs.resource.convertor;

import com.bonree.brfs.common.resource.vo.SwapStat;
import org.hyperic.sigar.Swap;

public class SwapConvertor {
    public SwapStat convert(Swap swap) {
        SwapStat stat = new SwapStat();
        stat.setTotal(swap.getTotal());
        stat.setUsed(swap.getUsed());
        stat.setFree(swap.getFree());
        stat.setPageIn(swap.getPageIn());
        stat.setPageOut(swap.getPageOut());
        return stat;
    }
}
