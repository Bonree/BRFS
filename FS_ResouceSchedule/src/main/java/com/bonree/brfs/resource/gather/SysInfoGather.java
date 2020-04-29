package com.bonree.brfs.resource.gather;

import com.bonree.brfs.common.process.LifeCycle;
import com.bonree.brfs.common.resource.vo.OSInfo;

public interface SysInfoGather extends LifeCycle {
    OSInfo gatherOSInfo() throws Exception;
}
