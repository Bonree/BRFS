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

package com.bonree.brfs.metrics.data;

import com.bonree.brfs.metrics.TimedData;
import java.util.List;
import javax.inject.Inject;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

@Path("/stats")
public class DataMetricsResource {

    private final DataStatisticReporter reporter;

    @Inject
    public DataMetricsResource(DataStatisticReporter reporter) {
        this.reporter = reporter;
    }

    @GET
    @Path("/write/{srName}")
    public List<TimedData<Long>> getWriteStatistics(
        @PathParam("srName") String srName,
        @DefaultValue("1") @QueryParam("minutes") int minutes) {
        return reporter.getWriteStatistics(srName, minutes);
    }

    @GET
    @Path("/read/{srName}")
    public List<TimedData<Long>> getReadStatistics(
        @PathParam("srName") String srName,
        @DefaultValue("1") @QueryParam("minutes") int minutes) {
        return reporter.getReadStatistics(srName, minutes);
    }
}
