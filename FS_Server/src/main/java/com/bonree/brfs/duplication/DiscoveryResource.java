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
import com.bonree.brfs.common.jackson.Json;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.utils.StringUtils;
import com.bonree.brfs.duplication.configuration.AccessConfig;
import com.bonree.brfs.guice.ClusterConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/servers")
public class DiscoveryResource {
    private static final Logger log = LoggerFactory.getLogger(DiscoveryResource.class);

    private final ServiceManager serviceManager;
    private final ClusterConfig clusterConfig;
    private final ObjectMapper jsonMapper;

    @Inject
    public DiscoveryResource(
        ClusterConfig clusterConfig,
        ServiceManager serviceManager,
        @Json ObjectMapper jsonMapper) {
        this.clusterConfig = clusterConfig;
        this.serviceManager = serviceManager;
        this.jsonMapper = jsonMapper;
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
                      .map(DiscoveryResource.this::toServerNode)
                      .filter(Objects::nonNull)
                      .collect(toImmutableList());
    }

    @GET
    @Path("{serverType}")
    @Produces(APPLICATION_JSON)
    public List<ServerNode> getServers(@PathParam("serverType") String serverType) {
        switch (ServerType.valueOf(serverType.toUpperCase(Locale.ENGLISH))) {
        case REGION:
            return serviceManager.getServiceListByGroup(clusterConfig.getRegionNodeGroup())
                .stream()
                .map(DiscoveryResource.this::toServerNode)
                .filter(Objects::nonNull)
                .collect(ImmutableList.toImmutableList());
        case DATA:
            return serviceManager.getServiceListByGroup(clusterConfig.getDataNodeGroup())
                .stream()
                .map(DiscoveryResource.this::toServerNode)
                .filter(Objects::nonNull)
                .collect(ImmutableList.toImmutableList());
        default:
            throw new RuntimeException(StringUtils.format("unknown server type[%s]", serverType));
        }
    }

    private ServerNode toServerNode(Service service) {
        AccessConfig accessConfig = null;
        try {
            accessConfig = service.getPayload() == null
                ? null : jsonMapper.readValue(service.getPayload(), AccessConfig.class);
        } catch (IOException e) {
            log.error("load payload error", e);
            return null;
        }

        return new ServerNode(
            service.getServiceGroup(),
            service.getServiceId(),
            service.getHost(),
            service.getPort(),
            service.getExtraPort(),
            accessConfig == null ? null : accessConfig.getAllowed(),
            accessConfig == null ? null : accessConfig.getForbidden());
    }
}
