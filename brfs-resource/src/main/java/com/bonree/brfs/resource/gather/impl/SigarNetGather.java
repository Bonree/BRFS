package com.bonree.brfs.resource.gather.impl;

import com.bonree.brfs.common.resource.vo.NetInfo;
import com.bonree.brfs.common.resource.vo.NetStat;
import com.bonree.brfs.resource.convertor.NetConvertor;
import com.bonree.brfs.resource.gather.NetGather;
import java.util.Collection;
import java.util.HashSet;
import org.hyperic.sigar.NetFlags;
import org.hyperic.sigar.NetInterfaceConfig;
import org.hyperic.sigar.NetInterfaceStat;
import org.hyperic.sigar.Sigar;

public class SigarNetGather implements NetGather {
    private Sigar sigar;
    private NetConvertor convertor;

    @Override
    public NetInfo gatherNetInfo(String ip) throws Exception {
        String[] netDevs = this.sigar.getNetInterfaceList();
        for (String net : netDevs) {
            NetInterfaceConfig config = this.sigar.getNetInterfaceConfig(net);
            String address = config.getAddress();
            if (NetFlags.isAnyAddress(address) || NetFlags.isLoopback(address) || !address.equals(ip)) {
                continue;
            }

            return this.convertor.convertNetInfo(config);
        }
        return null;
    }

    @Override
    public Collection<NetInfo> gatherNetInfos() throws Exception {
        String[] netDevs = this.sigar.getNetInterfaceList();
        Collection<NetInfo> netInfos = new HashSet<>();
        for (String net : netDevs) {
            NetInterfaceConfig config = this.sigar.getNetInterfaceConfig(net);
            String address = config.getAddress();
            if (NetFlags.isAnyAddress(address) || NetFlags.isLoopback(address)) {
                continue;
            }
            netInfos.add(this.convertor.convertNetInfo(config));
        }
        return netInfos;
    }

    @Override
    public NetStat gatherNetStat(String ip) throws Exception {
        String[] netDevs = this.sigar.getNetInterfaceList();
        for (String net : netDevs) {
            NetInterfaceConfig config = this.sigar.getNetInterfaceConfig(net);
            String address = config.getAddress();
            if (NetFlags.isAnyAddress(address) || NetFlags.isLoopback(address) || !address.equals(ip)) {
                continue;
            }
            NetInterfaceStat stat = sigar.getNetInterfaceStat(config.getName());

            return this.convertor.convertNetStat(stat, config);
        }
        return null;
    }

    @Override
    public Collection<NetStat> gatherNetStats() throws Exception {
        String[] netDevs = this.sigar.getNetInterfaceList();
        Collection<NetStat> netStats = new HashSet<>();
        for (String net : netDevs) {
            NetInterfaceConfig config = this.sigar.getNetInterfaceConfig(net);
            String address = config.getAddress();
            if (NetFlags.isAnyAddress(address) || NetFlags.isLoopback(address)) {
                continue;
            }
            NetInterfaceStat stat = sigar.getNetInterfaceStat(config.getName());

            netStats.add(this.convertor.convertNetStat(stat, config));
        }
        return netStats;
    }

    @Override
    public void start() throws Exception {
        this.sigar = new Sigar();
        this.convertor = new NetConvertor();
    }

    @Override
    public void stop() throws Exception {
        if (this.sigar != null) {
            this.sigar.close();
        }
    }
}
