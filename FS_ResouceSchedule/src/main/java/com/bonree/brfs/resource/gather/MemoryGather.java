package com.bonree.brfs.resource.gather;

import com.bonree.brfs.common.process.LifeCycle;
import com.bonree.brfs.common.resource.vo.MemStat;

public interface MemoryGather extends LifeCycle {
    MemStat gatherMemStat() throws Exception;
}
