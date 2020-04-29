package com.bonree.brfs.resource;

import com.bonree.brfs.resource.vo.GuiCpuInfo;
import com.bonree.brfs.resource.vo.GuiDiskIOInfo;
import com.bonree.brfs.resource.vo.GuiDiskUsageInfo;
import com.bonree.brfs.resource.vo.GuiLoadInfo;
import com.bonree.brfs.resource.vo.GuiMemInfo;
import com.bonree.brfs.resource.vo.GuiNetInfo;
import com.bonree.brfs.resource.vo.GuiNodeInfo;
import java.util.Collection;
import java.util.Map;

/**
 * gui资源管理接口
 */
public interface GuiResourceMaintainer {
    GuiNodeInfo getNodeInfo();

    void setNodeInfo(GuiNodeInfo nodeInfo);

    Collection<GuiCpuInfo> getCpuInfos(long time);

    void setCpuInfo(GuiCpuInfo cpuInfo);

    Collection<GuiMemInfo> getMemInfos(long time);

    void setMemInfo(GuiMemInfo memInfo);

    Collection<GuiLoadInfo> getLoadInfos(long time);

    void setLoadInfo(GuiLoadInfo loadInfo);

    Map<String,Collection<GuiDiskIOInfo>> getDiskIOInfos(long time);

    void setDiskIOs(Collection<GuiDiskIOInfo> iOs);

    Map<String,Collection<GuiDiskUsageInfo>> getDiskUsages(long time);

    void setDiskUsages(Collection<GuiDiskUsageInfo> usages);

    Map<String,Collection<GuiNetInfo>> getNetInfos(long time);

    void setNetInfos(Collection<GuiNetInfo> netInfos);
}
