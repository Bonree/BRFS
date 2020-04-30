package com.bonree.brfs.tasks.resource.impl;

import com.bonree.brfs.resource.GuiResourceMaintainer;
import com.bonree.brfs.resource.ResourceGatherInterface;
import com.bonree.brfs.resource.vo.GuiCpuInfo;
import com.bonree.brfs.resource.vo.GuiDiskIOInfo;
import com.bonree.brfs.resource.vo.GuiDiskUsageInfo;
import com.bonree.brfs.resource.vo.GuiLoadInfo;
import com.bonree.brfs.resource.vo.GuiMemInfo;
import com.bonree.brfs.resource.vo.GuiNetInfo;
import com.bonree.brfs.resource.vo.GuiNodeInfo;
import java.util.Collection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * GUI 采集资源信息,文件个数的维护，
 */
public class GuiResourcTask extends SuperResourceTask {
    private static final Logger LOG = LoggerFactory.getLogger(GuiResourcTask.class);
    private ResourceGatherInterface gather;
    private GuiResourceMaintainer guiMaintainer;
    private String storagePath;
    private int ttl;

    public GuiResourcTask(ResourceGatherInterface gather, GuiResourceMaintainer guiMaintainer, String storagePath,
                          int intervalTime, int ttl) {
        super(LOG, intervalTime);
        this.gather = gather;
        this.guiMaintainer = guiMaintainer;
        this.storagePath = storagePath;
        this.ttl = ttl;
    }

    @Override
    protected void atomRun() {
        long time = getGatherGranuleTime();
        try {
            GuiNodeInfo nodeInfo = gather.gatherNodeInfo();
            GuiCpuInfo cpuInfo = gather.gatherCpuInfo();
            GuiMemInfo memInfo = gather.gahterMemInfo();
            GuiLoadInfo loadInfo = gather.gatherLoadInfo();
            Collection<GuiNetInfo> netInfos = gather.gatherNetInfos();
            Collection<GuiDiskIOInfo> diskIOInfos = gather.gatherDiskIOInfos();
            Collection<GuiDiskUsageInfo> diskUsageInfos = gather.gatherDiskUsageInfos();
            cpuInfo.setTime(time);
            memInfo.setTime(time);
            loadInfo.setTime(time);
            if (netInfos != null) {
                netInfos.stream().forEach(x -> {
                    x.setTime(time);
                });
            }
            if (diskIOInfos != null) {
                diskIOInfos.stream().forEach(x -> x.setTime(time));
            }
            if (diskUsageInfos != null) {
                diskIOInfos.stream().forEach(x -> x.setTime(time));
            }
            guiMaintainer.setNodeInfo(nodeInfo);
            guiMaintainer.setCpuInfo(cpuInfo);
            guiMaintainer.setMemInfo(memInfo);
            guiMaintainer.setLoadInfo(loadInfo);
            guiMaintainer.setNetInfos(netInfos);
            guiMaintainer.setDiskIOs(diskIOInfos);
            guiMaintainer.setDiskUsages(diskUsageInfos);
        } catch (Exception e) {
            LOG.error("gui task happen error ", e);
        }
    }

    private long getGatherGranuleTime() {
        long time = System.currentTimeMillis();
        return time - time % (getIntervalSecond() * 1000);
    }
}
