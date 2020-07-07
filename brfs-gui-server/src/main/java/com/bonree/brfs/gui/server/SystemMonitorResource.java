/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bonree.brfs.gui.server;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

import com.bonree.brfs.gui.server.metrics.CpuMetric;
import com.bonree.brfs.gui.server.metrics.DiskUsageMetric;
import com.bonree.brfs.gui.server.metrics.LoadAvgMetric;
import com.bonree.brfs.gui.server.metrics.MemMetric;
import com.bonree.brfs.gui.server.metrics.NetMetric;
import com.bonree.brfs.gui.server.node.MonitorNode;
import com.bonree.brfs.gui.server.resource.GuiResourceMaintainer;
import com.bonree.brfs.gui.server.resource.vo.GuiCpuInfo;
import com.bonree.brfs.gui.server.resource.vo.GuiDiskIOInfo;
import com.bonree.brfs.gui.server.resource.vo.GuiDiskUsageInfo;
import com.bonree.brfs.gui.server.resource.vo.GuiLoadInfo;
import com.bonree.brfs.gui.server.resource.vo.GuiMemInfo;
import com.bonree.brfs.gui.server.resource.vo.GuiNetInfo;
import com.bonree.brfs.gui.server.resource.vo.GuiNodeInfo;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;

@Path("/sys")
public class SystemMonitorResource {
    private GuiResourceMaintainer maintainer;

    @Inject
    public SystemMonitorResource(GuiResourceMaintainer maintainer) {
        this.maintainer = maintainer;
    }

    private long convertTime(long time, int minute) {
        return time - 60 * 1000 * ((long) minute);
    }

    @GET
    @Path("nodes")
    @Produces(APPLICATION_JSON)
    public List<MonitorNode> getMonitorNodes() {
        Collection<GuiNodeInfo> guis = maintainer.getNodeInfos();
        if (guis == null || guis.isEmpty()) {
            return ImmutableList.of();
        }
        List<MonitorNode> metrics = new ArrayList<>();
        guis.stream().forEach(x -> {
            MonitorNode metric =
                new MonitorNode(x.getId(), x.getHost(), x.getCpuCores(), x.getCpuBrand(), x.getTotalMemSize(), x.getOs());
            metrics.add(metric);
        });
        return metrics;
    }

    @GET
    @Path("cpu/{nodeId}")
    @Produces(APPLICATION_JSON)
    public List<CpuMetric> getCpuInfo(
        @PathParam("nodeId") String nodeId,
        @QueryParam("minutes") int minutes) {
        long time = convertTime(System.currentTimeMillis(), minutes);
        Collection<GuiCpuInfo> guis = maintainer.getCpuInfos(nodeId, time);
        if (guis == null || guis.isEmpty()) {
            return ImmutableList.of();
        }
        List<CpuMetric> cpuMetrics = new ArrayList<>();
        guis.stream().forEach(x -> {
            CpuMetric cpu = new CpuMetric(x.getTime(), x.getTotal(), x.getSystem(), x.getUser(), x.getSteal(), x.getIowait());
            cpuMetrics.add(cpu);
        });
        cpuMetrics.sort(new Comparator<CpuMetric>() {
            @Override
            public int compare(CpuMetric o1, CpuMetric o2) {
                return (int) (o1.getTime() - o2.getTime());
            }
        });
        return cpuMetrics;
    }

    @GET
    @Path("mem/{nodeId}")
    @Produces(APPLICATION_JSON)
    public List<MemMetric> getMemInfo(
        @PathParam("nodeId") String nodeId,
        @QueryParam("minutes") int minutes) {
        long time = convertTime(System.currentTimeMillis(), minutes);
        Collection<GuiMemInfo> guis = maintainer.getMemInfos(nodeId, time);
        if (guis == null || guis.isEmpty()) {
            return ImmutableList.of();
        }
        List<MemMetric> metrics = new ArrayList<>();
        guis.stream().forEach(x -> {
            MemMetric metric = new MemMetric(x.getTime(), x.getTotalUsed(), x.getSwapUsed());
            metrics.add(metric);
        });
        metrics.sort(new Comparator<MemMetric>() {
            @Override
            public int compare(MemMetric o1, MemMetric o2) {
                return (int) (o1.getTime() - o2.getTime());
            }
        });
        return metrics;
    }

    @GET
    @Path("diskusage/{nodeId}")
    @Produces(APPLICATION_JSON)
    public Map<String, List<DiskUsageMetric>> getDiskUsageInfo(
        @PathParam("nodeId") String nodeId,
        @QueryParam("minutes") int minutes) {
        long time = convertTime(System.currentTimeMillis(), minutes);
        Map<String, Collection<GuiDiskUsageInfo>> guis = maintainer.getDiskUsages(nodeId, time);
        if (guis == null || guis.isEmpty()) {
            return ImmutableMap.of();
        }
        Map<String, List<DiskUsageMetric>> metrics = new HashMap<>();
        guis.entrySet().stream().forEach(x -> {
            Collection<GuiDiskUsageInfo> usages = x.getValue();
            if (usages == null || usages.isEmpty()) {
                return;
            }
            List<DiskUsageMetric> list = new ArrayList<>();
            usages.stream().forEach(y -> {
                DiskUsageMetric metric = new DiskUsageMetric(y.getTime(), y.getUsage());
                list.add(metric);
            });
            if (list == null || list.isEmpty()) {
                return;
            }
            String key = x.getKey();
            if (metrics.get(key) == null) {
                metrics.put(key, list);
            } else {
                metrics.get(key).addAll(list);
            }

        });
        for (Map.Entry<String, List<DiskUsageMetric>> entry : metrics.entrySet()) {
            if (entry.getValue() != null) {
                entry.getValue().sort(new Comparator<DiskUsageMetric>() {
                    @Override
                    public int compare(DiskUsageMetric o1, DiskUsageMetric o2) {
                        return (int) (o1.getTime() - o2.getTime());
                    }
                });
            }
        }
        return metrics;
    }

    @GET
    @Path("diskio/{nodeId}")
    @Produces(APPLICATION_JSON)
    public Map<String, List<DiskUsageMetric>> getDiskIOInfo(
        @PathParam("nodeId") String nodeId,
        @QueryParam("minutes") int minutes) {
        long time = convertTime(System.currentTimeMillis(), minutes);
        Map<String, Collection<GuiDiskIOInfo>> guis = maintainer.getDiskIOInfos(nodeId, time);
        if (guis == null || guis.isEmpty()) {
            return ImmutableMap.of();
        }
        Map<String, List<DiskUsageMetric>> metrics = new HashMap<>();
        guis.entrySet().stream().forEach(x -> {
            Collection<GuiDiskIOInfo> usages = x.getValue();
            if (usages == null || usages.isEmpty()) {
                return;
            }
            List<DiskUsageMetric> list = new ArrayList<>();
            usages.stream().forEach(y -> {
                DiskUsageMetric metric = new DiskUsageMetric(y.getTime(), y.getUsage() < 0 ? 0 : y.getUsage());
                list.add(metric);
            });
            if (list == null || list.isEmpty()) {
                return;
            }
            String key = x.getKey();
            if (metrics.get(key) == null) {
                metrics.put(key, list);
            } else {
                metrics.get(key).addAll(list);
            }

        });
        for (Map.Entry<String, List<DiskUsageMetric>> entry : metrics.entrySet()) {
            if (entry.getValue() != null) {
                entry.getValue().sort(new Comparator<DiskUsageMetric>() {
                    @Override
                    public int compare(DiskUsageMetric o1, DiskUsageMetric o2) {
                        return (int) (o1.getTime() - o2.getTime());
                    }
                });
            }
        }
        return metrics;
    }

    @GET
    @Path("net/{nodeId}")
    @Produces(APPLICATION_JSON)
    public Map<String, List<NetMetric>> getNetInfo(
        @PathParam("nodeId") String nodeId,
        @QueryParam("minutes") int minutes) {
        long time = convertTime(System.currentTimeMillis(), minutes);
        Map<String, Collection<GuiNetInfo>> guis = maintainer.getNetInfos(nodeId, time);
        if (guis == null || guis.isEmpty()) {
            return ImmutableMap.of();
        }
        Map<String, List<NetMetric>> metrics = new HashMap<>();
        guis.entrySet().stream().forEach(x -> {
            Collection<GuiNetInfo> usages = x.getValue();
            if (usages == null || usages.isEmpty()) {
                return;
            }
            List<NetMetric> list = new ArrayList<>();
            usages.stream().forEach(y -> {
                NetMetric metric = new NetMetric(y.getTime(), y.getTxBytesPs(), y.getRxBytesPs());
                list.add(metric);
            });
            if (list == null || list.isEmpty()) {
                return;
            }
            String key = x.getKey();
            if (metrics.get(key) == null) {
                metrics.put(key, list);
            } else {
                metrics.get(key).addAll(list);
            }

        });
        for (Map.Entry<String, List<NetMetric>> entry : metrics.entrySet()) {
            if (entry.getValue() != null) {
                entry.getValue().sort(new Comparator<NetMetric>() {
                    @Override
                    public int compare(NetMetric o1, NetMetric o2) {
                        return (int) (o1.getTime() - o2.getTime());
                    }
                });
            }
        }
        return metrics;
    }

    @GET
    @Path("load/{nodeId}")
    @Produces(APPLICATION_JSON)
    public List<LoadAvgMetric> getLoadAvgInfo(
        @PathParam("nodeId") String nodeId,
        @QueryParam("minutes") int minutes) {
        long time = convertTime(System.currentTimeMillis(), minutes);
        Collection<GuiLoadInfo> guis = maintainer.getLoadInfos(nodeId, time);
        if (guis == null || guis.isEmpty()) {
            return ImmutableList.of();
        }
        List<LoadAvgMetric> metrics = new ArrayList<>();
        guis.stream().forEach(x -> {
            LoadAvgMetric metric = new LoadAvgMetric(x.getTime(), x.getLoad());
            metrics.add(metric);
        });
        metrics.sort(new Comparator<LoadAvgMetric>() {
            @Override
            public int compare(LoadAvgMetric o1, LoadAvgMetric o2) {
                return (int) (o1.getTime() - o2.getTime());
            }
        });
        return metrics;
    }
}
