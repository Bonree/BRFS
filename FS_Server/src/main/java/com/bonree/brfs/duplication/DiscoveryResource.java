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

package com.bonree.brfs.duplication;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

import com.bonree.brfs.client.discovery.ServerNode;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.utils.StringUtils;
import com.bonree.brfs.guice.ClusterConfig;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Streams;
import java.util.List;
import java.util.Locale;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;

@Path("/servers")
public class DiscoveryResource {

    private final ServiceManager serviceManager;
    private final ClusterConfig clusterConfig;

    @Inject
    public DiscoveryResource(
        ClusterConfig clusterConfig,
        ServiceManager serviceManager) {
        this.clusterConfig = clusterConfig;
        this.serviceManager = serviceManager;
    }

    private enum ServerType {
        REGION,
        DATA
    }

    @GET
    @Produces(APPLICATION_JSON)
    public List<ServerNode> getAllServers() {
        return Streams.concat(
            serviceManager.getServiceListByGroup(clusterConfig.getRegionNodeGroup()).stream(),
            serviceManager.getServiceListByGroup(clusterConfig.getDataNodeGroup()).stream())
            .map(DiscoveryResource::toServerNode)
            .collect(toImmutableList());
    }

    @GET
    @Path("{serverType}")
    @Produces(APPLICATION_JSON)
    public List<ServerNode> getServers(@PathParam("serverType") String serverType) {
        ImmutableList.Builder<ServerNode> nodes = ImmutableList.builder();
        switch (ServerType.valueOf(serverType.toUpperCase(Locale.ENGLISH))) {
        case REGION:
            nodes.addAll(
                Lists.transform(serviceManager.getServiceListByGroup(clusterConfig.getRegionNodeGroup()),
                                DiscoveryResource::toServerNode));
            break;
        case DATA:
            nodes.addAll(
                Lists.transform(serviceManager.getServiceListByGroup(clusterConfig.getDataNodeGroup()),
                                DiscoveryResource::toServerNode));
            break;
        default:
            throw new RuntimeException(StringUtils.format("unknown server type[%s]", serverType));
        }

        return nodes.build();
    }

    private static ServerNode toServerNode(Service service) {
        return new ServerNode(
            service.getServiceGroup(),
            service.getServiceId(),
            service.getHost(),
            service.getPort(),
            service.getExtraPort());
    }
}
