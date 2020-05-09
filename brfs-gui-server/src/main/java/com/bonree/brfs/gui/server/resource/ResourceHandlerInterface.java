package com.bonree.brfs.gui.server.resource;

import com.bonree.brfs.common.resource.vo.NodeSnapshotInfo;
import com.bonree.brfs.gui.server.resource.vo.GuiCpuInfo;
import com.bonree.brfs.gui.server.resource.vo.GuiDiskIOInfo;
import com.bonree.brfs.gui.server.resource.vo.GuiDiskUsageInfo;
import com.bonree.brfs.gui.server.resource.vo.GuiLoadInfo;
import com.bonree.brfs.gui.server.resource.vo.GuiMemInfo;
import com.bonree.brfs.gui.server.resource.vo.GuiNetInfo;
import com.bonree.brfs.gui.server.resource.vo.GuiNodeInfo;
import java.util.Collection;

public interface ResourceHandlerInterface {
    GuiNodeInfo gatherNodeInfo(NodeSnapshotInfo snapshot) throws Exception;

    GuiCpuInfo gatherCpuInfo(NodeSnapshotInfo snapshot) throws Exception;

    GuiMemInfo gahterMemInfo(NodeSnapshotInfo snapshot) throws Exception;

    GuiLoadInfo gatherLoadInfo(NodeSnapshotInfo snapshot) throws Exception;

    Collection<GuiNetInfo> gatherNetInfos(NodeSnapshotInfo snapshot) throws Exception;

    Collection<GuiDiskIOInfo> gatherDiskIOInfos(NodeSnapshotInfo snapshot) throws Exception;

    Collection<GuiDiskUsageInfo> gatherDiskUsageInfos(NodeSnapshotInfo snapshot) throws Exception;
}
