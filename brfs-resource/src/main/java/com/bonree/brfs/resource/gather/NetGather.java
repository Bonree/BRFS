package com.bonree.brfs.resource.gather;

import com.bonree.brfs.common.process.LifeCycle;
import com.bonree.brfs.common.resource.vo.NetInfo;
import com.bonree.brfs.common.resource.vo.NetStat;
import java.util.Collection;

public interface NetGather extends LifeCycle {
    /**
     * 从所有网卡设备中查找具有某一ip的网卡
     * @param ip
     * @return
     * @throws Exception
     */
    NetInfo gatherNetInfo(String ip) throws Exception;

    Collection<NetInfo> gatherNetInfos() throws Exception;

    NetStat gatherNetStat(String dev) throws Exception;

    Collection<NetStat> gatherNetStats() throws Exception;
}
