package com.bonree.brfs.resource.gather;

import com.bonree.brfs.common.process.LifeCycle;
import com.bonree.brfs.common.resource.vo.Load;

public interface LoadGather extends LifeCycle {
    Load gather() throws Exception;
}
