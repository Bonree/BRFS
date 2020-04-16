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

import java.util.List;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.client.route.NormalRouterNode;
import com.bonree.brfs.client.route.RouterNode;
import com.bonree.brfs.client.route.SecondServerID;
import com.bonree.brfs.client.route.VirtualRouterNode;
import com.bonree.brfs.client.utils.Strings;
import com.bonree.brfs.common.rebalance.route.NormalRouteInterface;
import com.bonree.brfs.common.rebalance.route.VirtualRoute;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.duplication.storageregion.StorageRegion;
import com.bonree.brfs.duplication.storageregion.StorageRegionManager;
import com.bonree.brfs.duplication.storageregion.exception.StorageRegionNonexistentException;
import com.bonree.brfs.guice.ClusterConfig;
import com.bonree.brfs.identification.PartitionInterface;
import com.bonree.brfs.identification.SecondIdsInterface;
import com.bonree.brfs.rebalance.route.RouteLoader;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

@Path("/router")
public class RouterResource {
    private static final Logger log = LoggerFactory.getLogger(RouterResource.class);
    
    private final SecondIdsInterface secondIds;
    private final PartitionInterface partitions;
    private final RouteLoader routerLoader;
    
    private final StorageRegionManager storageRegionManager;
    private final ServiceManager serviceManager;
    
    private final ClusterConfig clusterConfig;
    
    @Inject
    public RouterResource(
            ClusterConfig clusterConfig,
            SecondIdsInterface secondIds,
            PartitionInterface partitions,
            RouteLoader routerLoader,
            StorageRegionManager storageRegionManager,
            ServiceManager serviceManager) {
        this.clusterConfig = clusterConfig;
        this.secondIds = secondIds;
        this.partitions = partitions;
        this.routerLoader = routerLoader;
        this.storageRegionManager = storageRegionManager;
        this.serviceManager = serviceManager;
    }

    @GET
    @Path("secondServerID/{srName}")
    public List<SecondServerID> getAllSecondServerID(@PathParam("srName") String srName) throws StorageRegionNonexistentException {
        StorageRegion storageRegion = storageRegionManager.findStorageRegionByName(srName);
        if(storageRegion == null) {
            throw new StorageRegionNonexistentException(srName);
        }
        
        return serviceManager.getServiceListByGroup(clusterConfig.getDataNodeGroup())
                .stream()
                .map(node -> getSecondServerID(node, storageRegion.getId()))
                .flatMap(List::stream)
                .collect(toImmutableList());
    }
    
    @GET
    @Path("secondServerID/{srName}/{dataNodeId}")
    public List<SecondServerID> getSercondServerID(
            @PathParam("srName") String srName,
            @PathParam("dataNodeId") String dataNodeId) throws StorageRegionNonexistentException {
        StorageRegion storageRegion = storageRegionManager.findStorageRegionByName(srName);
        if(storageRegion == null) {
            throw new StorageRegionNonexistentException(srName);
        }
        
        Service node = serviceManager.getServiceById(clusterConfig.getDataNodeGroup(), dataNodeId);
        if(node == null) {
            return ImmutableList.of();
        }
        
        return getSecondServerID(node, storageRegion.getId());
    }
    
    @GET
    @Path("update/{srName}")
    public List<RouterNode> getUpdate(@PathParam("srName") String srName) throws StorageRegionNonexistentException {
        StorageRegion storageRegion = storageRegionManager.findStorageRegionByName(srName);
        if(storageRegion == null) {
            throw new StorageRegionNonexistentException(srName);
        }
        
        return Lists.newArrayList(
                Iterables.concat(
                        getVirtualUpdate(srName),
                        getNormalUpdate(srName)));
    }
    
    @GET
    @Path("update/virtual/{srName}")
    public List<VirtualRouterNode> getVirtualUpdate(@PathParam("srName") String srName) throws StorageRegionNonexistentException {
        StorageRegion storageRegion = storageRegionManager.findStorageRegionByName(srName);
        if(storageRegion == null) {
            throw new StorageRegionNonexistentException(srName);
        }
        
        ImmutableList.Builder<VirtualRouterNode> builder = ImmutableList.builder();
        try {
            for(VirtualRoute vr : routerLoader.loadVirtualRoutes(storageRegion.getId())) {
                builder.add(new VirtualRouterNode(
                        vr.getChangeID(),
                        vr.getStorageIndex(),
                        vr.getVirtualID(),
                        vr.getNewSecondID(),
                        vr.getVersion().name()));
            }
        } catch (Exception e) {
            log.error(Strings.format("fetch virtual router info of storage region[%s] error", srName), e);
        }
        
        return builder.build();
    }
    
    @GET
    @Path("update/normal/{srName}")
    public List<NormalRouterNode> getNormalUpdate(@PathParam("srName") String srName) throws StorageRegionNonexistentException {
        StorageRegion storageRegion = storageRegionManager.findStorageRegionByName(srName);
        if(storageRegion == null) {
            throw new StorageRegionNonexistentException(srName);
        }
        
        ImmutableList.Builder<NormalRouterNode> builder = ImmutableList.builder();
        try {
            for(NormalRouteInterface nr : routerLoader.loadNormalRoutes(storageRegion.getId())) {
                builder.add(new NormalRouterNode(
                        nr.getChangeId(),
                        nr.getStorageRegionIndex(),
                        nr.getBaseSecondId(),
                        nr.getRoutes(),
                        nr.getRouteVersion().name()));
            }
        } catch (Exception e) {
            log.error(Strings.format("fetch normal router info of storage region[%s] error", srName), e);
        }
        
        return builder.build();
    }
    
    private List<SecondServerID> getSecondServerID(Service service, int srId) {
        ImmutableList.Builder<SecondServerID> builder = ImmutableList.builder();
        for(String secondId : secondIds.getSecondIds(service.getServiceId(), srId)) {
            builder.add(new SecondServerID(
                    service.getServiceId(),
                    service.getHost(),
                    service.getPort(),
                    service.getExtraPort(),
                    srId,
                    secondId,
                    partitions.getDataDir(secondId, srId)));
        }
        
        return builder.build();
    }
}
