package com.bonree.brfs.gui.server.resource;

import com.bonree.brfs.common.process.LifeCycle;
import com.bonree.brfs.common.resource.vo.NodeSnapshotInfo;
import com.bonree.brfs.gui.server.resource.vo.GuiCpuInfo;
import com.bonree.brfs.gui.server.resource.vo.GuiDiskIOInfo;
import com.bonree.brfs.gui.server.resource.vo.GuiDiskUsageInfo;
import com.bonree.brfs.gui.server.resource.vo.GuiLoadInfo;
import com.bonree.brfs.gui.server.resource.vo.GuiMemInfo;
import com.bonree.brfs.gui.server.resource.vo.GuiNetInfo;
import com.bonree.brfs.gui.server.resource.vo.GuiNodeInfo;
import java.util.Collection;
import java.util.Map;

/**
 * gui资源管理接口
 */
public interface GuiResourceMaintainer extends LifeCycle {

    Collection<GuiNodeInfo> getNodeInfos();

    GuiNodeInfo getNodeInfo(String node);

    void setNodeInfo(String node, GuiNodeInfo nodeInfo);

    Collection<GuiCpuInfo> getCpuInfos(String node, long time);

    void setCpuInfo(String node, GuiCpuInfo cpuInfo);

    Collection<GuiMemInfo> getMemInfos(String node, long time);

    void setMemInfo(String node, GuiMemInfo memInfo);

    Collection<GuiLoadInfo> getLoadInfos(String node, long time);

    void setLoadInfo(String node, GuiLoadInfo loadInfo);

    Map<String, Collection<GuiDiskIOInfo>> getDiskIOInfos(String node, long time);

    void setDiskIOs(String node, Collection<GuiDiskIOInfo> diskIOs);

    Map<String, Collection<GuiDiskUsageInfo>> getDiskUsages(String node, long time);

    void setDiskUsages(String node, Collection<GuiDiskUsageInfo> usages);

    Map<String, Collection<GuiNetInfo>> getNetInfos(String node, long time);

    void setNetInfos(String node, Collection<GuiNetInfo> netInfos);
}
