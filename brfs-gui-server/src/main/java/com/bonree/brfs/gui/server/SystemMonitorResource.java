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

import java.util.List;
import java.util.Map;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;

import com.bonree.brfs.gui.server.metrics.CpuMetric;
import com.bonree.brfs.gui.server.metrics.DiskUsageMetric;
import com.bonree.brfs.gui.server.metrics.LoadAvgMetric;
import com.bonree.brfs.gui.server.metrics.MemMetric;
import com.bonree.brfs.gui.server.metrics.NetMetric;
import com.bonree.brfs.gui.server.node.MonitorNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

@Path("/sys")
public class SystemMonitorResource {
    
    @GET
    @Path("nodes")
    @Produces(APPLICATION_JSON)
    public List<MonitorNode> getMonitorNodes() {
        return ImmutableList.of();
    }
    
    @GET
    @Path("cpu/{nodeId}")
    @Produces(APPLICATION_JSON)
    public List<CpuMetric> getCpuInfo(
            @PathParam("nodeId") String nodeId,
            @QueryParam("minutes") int minutes) {
        return ImmutableList.of();
    }
    
    @GET
    @Path("mem/{nodeId}")
    @Produces(APPLICATION_JSON)
    public List<MemMetric> getMemInfo(
            @PathParam("nodeId") String nodeId,
            @QueryParam("minutes") int minutes) {
        return ImmutableList.of();
    }
    
    @GET
    @Path("diskusage/{nodeId}")
    @Produces(APPLICATION_JSON)
    public Map<String, List<DiskUsageMetric>> getDiskUsageInfo(
            @PathParam("nodeId") String nodeId,
            @QueryParam("minutes") int minutes) {
        return ImmutableMap.of();
    }
    
    @GET
    @Path("diskio/{nodeId}")
    @Produces(APPLICATION_JSON)
    public Map<String, List<DiskUsageMetric>> getDiskIOInfo(
            @PathParam("nodeId") String nodeId,
            @QueryParam("minutes") int minutes) {
        return ImmutableMap.of();
    }
    
    @GET
    @Path("net/{nodeId}")
    @Produces(APPLICATION_JSON)
    public Map<String, List<NetMetric>> getNetInfo(
            @PathParam("nodeId") String nodeId,
            @QueryParam("minutes") int minutes) {
        return ImmutableMap.of();
    }
    
    @GET
    @Path("load/{nodeId}")
    @Produces(APPLICATION_JSON)
    public List<LoadAvgMetric> getLoadAvgInfo(
            @PathParam("nodeId") String nodeId,
            @QueryParam("minutes") int minutes) {
        return ImmutableList.of();
    }
}
