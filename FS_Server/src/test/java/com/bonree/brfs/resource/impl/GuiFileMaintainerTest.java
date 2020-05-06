package com.bonree.brfs.resource.impl;

import com.bonree.brfs.disknode.GuiResourceConfig;
import com.bonree.brfs.resource.vo.GuiCpuInfo;
import com.bonree.brfs.resource.vo.GuiNetInfo;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import org.junit.Test;

public class GuiFileMaintainerTest {
    private String path = "/home/wellgeek/test/factory/brfs/guifactory";

    @Test
    public void testSaveCpuInfo() {
        GuiFileMaintainer maintainer = new GuiFileMaintainer(new GuiResourceConfig());
        GuiCpuInfo cpuInfo = new GuiCpuInfo();
        cpuInfo.setTime(System.currentTimeMillis());
        maintainer.setCpuInfo(cpuInfo);
        long time = System.currentTimeMillis() - 7 * 24 * 60 * 60 * 1000;
        Collection<GuiCpuInfo> cpuInfos = maintainer.getCpuInfos(time);
        System.out.println(time);
        System.out.println("-------------");
        cpuInfos.stream().forEach(x -> {
            System.out.println(x.getTime());
        });
    }

    @Test
    public void testArrayNetInfo() {
        long time1 = System.currentTimeMillis();
        GuiNetInfo net1 = new GuiNetInfo();
        net1.setNetDev("net1");
        net1.setTime(time1);
        GuiNetInfo net2 = new GuiNetInfo();
        net2.setNetDev("net2");
        net2.setTime(time1);

        Collection<GuiNetInfo> nets = new ArrayList<>();
        nets.add(net1);
        nets.add(net2);
        GuiFileMaintainer maintainer = new GuiFileMaintainer(new GuiResourceConfig());
        maintainer.setNetInfos(nets);
        long time = System.currentTimeMillis() - 7 * 24 * 60 * 60 * 1000;
        Map<String, Collection<GuiNetInfo>> map = maintainer.getNetInfos(time);
        for (Map.Entry<String, Collection<GuiNetInfo>> netEntry : map.entrySet()) {
            System.out.println(netEntry.getKey());
        }
    }

}
