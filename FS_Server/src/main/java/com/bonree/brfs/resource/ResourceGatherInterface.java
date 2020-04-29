package com.bonree.brfs.resource;

import com.bonree.brfs.resource.vo.GuiCpuInfo;
import com.bonree.brfs.resource.vo.GuiDiskIOInfo;
import com.bonree.brfs.resource.vo.GuiDiskUsageInfo;
import com.bonree.brfs.resource.vo.GuiLoadInfo;
import com.bonree.brfs.resource.vo.GuiMemInfo;
import com.bonree.brfs.resource.vo.GuiNetInfo;
import com.bonree.brfs.resource.vo.GuiNodeInfo;
import com.bonree.brfs.resource.vo.ResourceModel;
import java.util.Collection;

/**
 * 资源采集接口
 */
public interface ResourceGatherInterface {
    ResourceModel gatherClusterResource() throws Exception;

    GuiNodeInfo gatherNodeInfo() throws Exception;

    GuiCpuInfo gatherCpuInfo() throws Exception;

    GuiMemInfo gahterMemInfo() throws Exception;

    GuiLoadInfo gatherLoadInfo() throws Exception;

    Collection<GuiNetInfo> gatherNetInfos() throws Exception;

    Collection<GuiDiskIOInfo> gatherDiskIOInfos() throws Exception;

    Collection<GuiDiskUsageInfo> gatherDiskUsageInfos() throws Exception;
}
