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

package com.bonree.brfs.duplication.storageregion;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

import com.bonree.brfs.client.storageregion.StorageRegionAttributes;
import com.bonree.brfs.client.storageregion.StorageRegionID;
import com.bonree.brfs.client.storageregion.StorageRegionInfo;
import com.bonree.brfs.common.ReturnCode;
import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.rocksdb.RocksDBManager;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.utils.StringUtils;
import com.bonree.brfs.guice.ClusterConfig;
import com.bonree.brfs.schedulers.utils.TasksUtils;
import com.google.common.base.Throwables;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.HEAD;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/sr/v2")
public class StorageRegionResource {
    private static final Logger log = LoggerFactory.getLogger(StorageRegionResource.class);

    private final ClusterConfig clusterConfig;
    private final StorageRegionManager storageRegionManager;
    private final ServiceManager serviceManager;

    private final ZookeeperPaths zkPaths;
    private final RocksDBManager rocksDBManager;
    private final CuratorFramework client;

    @Inject
    public StorageRegionResource(
        ClusterConfig clusterConfig,
        StorageRegionManager storageRegionManager,
        ServiceManager serviceManager,
        ZookeeperPaths zkPaths,
        RocksDBManager rocksDBManager,
        CuratorFramework client) {
        this.clusterConfig = clusterConfig;
        this.storageRegionManager = storageRegionManager;
        this.serviceManager = serviceManager;
        this.zkPaths = zkPaths;
        this.rocksDBManager = rocksDBManager;
        this.client = client;
    }

    @PUT
    @Path("{srName}")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON)
    public Response create(
        @PathParam("srName") String name,
        Properties attributes) {
        if (storageRegionManager.exists(name)) {
            return Response.status(Status.CONFLICT)
                           .entity(StringUtils.format("Storage Region[%s] has been existed", name))
                           .build();
        }

        try {
            StorageRegion storageRegion = storageRegionManager.createStorageRegion(
                name,
                StorageRegionProperties.withDefault().override(attributes));
            rocksDBManager.createColumnFamilyWithTtl(name, -1);
            return Response.ok(new StorageRegionID(storageRegion.getName(), storageRegion.getId())).build();
        } catch (Exception e) {
            log.error(StringUtils.format("can not create storage region[%s]", name), e);
            return Response.serverError().entity(Throwables.getStackTraceAsString(e)).build();
        }
    }

    @POST
    @Path("{srName}")
    @Produces(APPLICATION_JSON)
    public Response update(
        @PathParam("srName") String name,
        Properties attributes) {
        if (!storageRegionManager.exists(name)) {
            return Response.status(Status.NOT_FOUND).build();
        }

        try {
            storageRegionManager.updateStorageRegion(name, attributes);
            return Response.ok().build();
        } catch (Exception e) {
            log.error(StringUtils.format("can not update storage region[%s]", name), e);
            return Response.serverError().entity(Throwables.getStackTraceAsString(e)).build();
        }
    }

    @HEAD
    @Path("{srName}")
    public Response doesStorageRegionExists(
        @PathParam("srName") String name) {
        StorageRegion node = storageRegionManager.findStorageRegionByName(name);
        if (node == null) {
            return Response.status(Status.NOT_FOUND).build();
        }

        return Response.ok().build();
    }

    @GET
    @Path("id/{srName}")
    @Produces(APPLICATION_JSON)
    public Response getStorageRegionID(@PathParam("srName") String name) {
        StorageRegion node = storageRegionManager.findStorageRegionByName(name);
        if (node == null) {
            return Response.status(Status.NOT_FOUND).build();
        }

        return Response.ok().entity(new StorageRegionID(node.getName(), node.getId())).build();
    }

    @GET
    @Path("{srName}")
    @Produces(APPLICATION_JSON)
    public Response getStoargeRegion(
        @PathParam("srName") String name) {
        StorageRegion node = storageRegionManager.findStorageRegionByName(name);
        if (node == null) {
            return Response.status(Status.NOT_FOUND).build();
        }

        StorageRegionInfo info = new StorageRegionInfo(
            new StorageRegionID(node.getName(), node.getId()),
            new StorageRegionAttributes(
                node.isEnable(),
                node.getReplicateNum(),
                node.getDataTtl(),
                node.getFileCapacity(),
                node.getFilePartitionDuration()));
        return Response.ok(info).build();
    }

    @GET
    @Path("list")
    @Produces(APPLICATION_JSON)
    public List<String> listStorageRegions(
        @QueryParam("prefix") String prefix,
        @DefaultValue("true") @QueryParam("disableAllowed") boolean disableAllowed,
        @DefaultValue("2147483647") @QueryParam("maxKeys") int maxKeys) {
        List<StorageRegion> storageRegionList = storageRegionManager.getStorageRegionList();
        return storageRegionList
            .subList(0, Math.min(maxKeys, storageRegionList.size()))
            .stream()
            .filter(sr -> disableAllowed || sr.isEnable())
            .map(StorageRegion::getName)
            .filter(name -> prefix == null || name.startsWith(prefix))

            .collect(toImmutableList());
    }

    @DELETE
    @Path("{srName}")
    public Response delete(@PathParam("srName") String name) {
        StorageRegion region = storageRegionManager.findStorageRegionByName(name);
        if (region == null) {
            return Response.status(Status.NOT_FOUND).build();
        }

        if (region.isEnable()) {
            return Response.status(Status.FORBIDDEN).build();
        }

        List<Service> services = serviceManager.getServiceListByGroup(clusterConfig.getDataNodeGroup());
        ReturnCode code = TasksUtils.createUserDeleteTask(client,
                                                          services,
                                                          zkPaths,
                                                          region,
                                                          region.getCreateTime(),
                                                          System.currentTimeMillis(),
                                                          true);
        log.info("create user delete task status{}", code.name());
        if (!ReturnCode.SUCCESS.equals(code)) {
            return Response.serverError()
                           .entity(BrStringUtils.toUtf8Bytes(code.name()))
                           .build();
        }

        try {
            rocksDBManager.deleteColumnFamily(name);
            if (storageRegionManager.removeStorageRegion(name)) {
                return Response.ok().build();
            }
        } catch (Exception e) {
            log.error(StringUtils.format("can not delete storage region[%s] from manager", name), e);
        }

        return Response.serverError()
                       .entity(BrStringUtils.toUtf8Bytes(ReturnCode.STORAGE_REMOVE_ERROR.name()))
                       .build();
    }
}
