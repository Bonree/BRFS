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

import com.bonree.brfs.gui.server.mock.DashboardMock;
import com.bonree.brfs.gui.server.mock.DashboardWorker;
import com.bonree.brfs.gui.server.node.NodeSummaryInfo;
import com.bonree.brfs.gui.server.stats.BusinessStats;
import com.bonree.brfs.gui.server.stats.StatReportor;
import com.google.inject.Inject;
import java.util.List;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;

@Path("/dashboard")
public class DashBoardResource {

    private DashboardWorker mock;
    private StatReportor statReportor;

    @Inject
    public DashBoardResource(StatReportor statReportor, DashboardWorker mock) {
        this.mock = mock;
        this.statReportor = statReportor;
    }

    @GET
    @Path("summary")
    @Produces(APPLICATION_JSON)
    public List<NodeSummaryInfo> getNodeSummaries() {
        return mock.getNodeSummaries();
    }

    @GET
    @Path("diskusage")
    @Produces(APPLICATION_JSON)
    public TotalDiskUsage getTotalDiskUsage() {
        return mock.getTotalDiskUsage();
    }

    @GET
    @Path("business")
    @Produces(APPLICATION_JSON)
    public List<String> getBusinesses() {
        return mock.getBusinesses();
    }

    @GET
    @Path("stats/{business}")
    @Produces(APPLICATION_JSON)
    public BusinessStats getBusinessStats(
        @PathParam("business") String business,
        @QueryParam("minutes") int minutes) {
        return statReportor.getCountByPrefix(business, minutes);
    }

    @GET
    @Path("stats")
    @Produces(APPLICATION_JSON)
    public BusinessStats getBusinessStats(
        @QueryParam("minutes") int minutes) {
        return statReportor.getCount("", minutes, false);
    }
}
