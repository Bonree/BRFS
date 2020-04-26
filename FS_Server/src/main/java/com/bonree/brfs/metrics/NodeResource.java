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

package com.bonree.brfs.metrics;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

import com.bonree.brfs.metrics.usage.CpuUsageInfo;
import com.bonree.brfs.metrics.usage.DiskUsageInfo;
import com.bonree.brfs.metrics.usage.MemUsageInfo;
import com.bonree.brfs.metrics.usage.NetworkUsageInfo;
import com.bonree.brfs.metrics.usage.SystemLoadAvgInfo;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;

@Path("/node")
public class NodeResource {

    @GET
    @Produces(APPLICATION_JSON)
    public NodeInfo getNodeInfo() {
        return null;
    }

    @GET
    @Path("cpu")
    @Produces(APPLICATION_JSON)
    public CpuUsageInfo getCpuUsage(
        @DefaultValue("1") @QueryParam("minutes") int minutes) {
        return null;
    }

    @GET
    @Path("mem")
    @Produces(APPLICATION_JSON)
    public MemUsageInfo getMemUsage(
        @DefaultValue("1") @QueryParam("minutes") int minutes) {
        return null;
    }

    @GET
    @Path("disk/{device}")
    @Produces(APPLICATION_JSON)
    public DiskUsageInfo getDiskUsageInfo(
        @PathParam("device") String device,
        @DefaultValue("1") @QueryParam("minutes") int minutes) {
        return null;
    }

    @GET
    @Path("network/{iface}")
    @Produces(APPLICATION_JSON)
    public NetworkUsageInfo getNetworkUsageInfo(
        @PathParam("iface") String iface,
        @DefaultValue("1") @QueryParam("minutes") int minutes) {
        return null;
    }

    @GET
    @Path("load")
    @Produces(APPLICATION_JSON)
    public SystemLoadAvgInfo getSystemLoadAvgInfo(
        @DefaultValue("1") @QueryParam("minutes") int minutes) {
        return null;
    }
}
