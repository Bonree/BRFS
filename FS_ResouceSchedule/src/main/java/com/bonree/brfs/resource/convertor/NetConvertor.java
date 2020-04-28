package com.bonree.brfs.resource.convertor;

import com.bonree.brfs.common.resource.vo.NetInfo;
import com.bonree.brfs.common.resource.vo.NetStat;
import org.hyperic.sigar.NetInterfaceConfig;
import org.hyperic.sigar.NetInterfaceStat;

public class NetConvertor {
    public NetStat convertNetStat(NetInterfaceStat net, NetInterfaceConfig config) {
        NetStat stat = new NetStat();
        stat.setRxBytes(net.getRxBytes());
        stat.setRxPackets(net.getRxPackets());
        stat.setRxErrors(net.getRxErrors());
        stat.setRxDropped(net.getRxDropped());
        stat.setRxOverruns(net.getRxOverruns());
        stat.setRxFrame(net.getRxFrame());
        stat.setTxBytes(net.getTxBytes());
        stat.setTxPackets(net.getTxPackets());
        stat.setTxErrors(net.getTxErrors());
        stat.setTxDropped(net.getTxDropped());
        stat.setTxOverruns(net.getTxOverruns());
        stat.setTxCollisions(net.getTxCollisions());
        stat.setTxCarrier(net.getTxCarrier());
        stat.setSpeed(net.getSpeed());
        return stat;
    }

    public NetInfo convertNetInfo(NetInterfaceConfig config) {
        NetInfo info = new NetInfo();
        info.setName(config.getName());
        info.setHwaddr(config.getHwaddr());
        info.setType(config.getType());
        info.setDescription(config.getDescription());
        info.setAddress(config.getAddress());
        info.setDestination(config.getDestination());
        info.setBroadcast(config.getBroadcast());
        info.setNetmask(config.getNetmask());
        info.setFlags(config.getFlags());
        info.setMtu(config.getMtu());
        info.setMetric(config.getMetric());
        return info;
    }
}
