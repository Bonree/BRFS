package com.bonree.brfs.resource.gather;

import com.bonree.brfs.common.process.LifeCycle;
import com.bonree.brfs.common.resource.vo.NetInfo;
import com.bonree.brfs.common.resource.vo.NetStat;
import java.util.Collection;

public interface NetGather  extends LifeCycle {
    NetInfo gatherNetInfo(String ip) throws Exception;

    Collection<NetInfo> gatherNetInfos() throws Exception;

    NetStat gatherNetStat(String dev) throws Exception;

    Collection<NetStat> gatherNetStats() throws Exception;
}
