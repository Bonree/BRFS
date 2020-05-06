package com.bonree.brfs.resource.gather;

import com.bonree.brfs.common.process.LifeCycle;
import com.bonree.brfs.common.resource.vo.SwapStat;

public interface SwapGather extends LifeCycle {
    SwapStat gatherSwap() throws Exception;
}
