package com.bonree.brfs.duplication.storageregion;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

import com.bonree.brfs.client.storageregion.StorageRegionID;
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
import com.google.common.primitives.Ints;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Deprecated
@Path("/sr")
public class LegacyStorageRegionResouce {
    private static final Logger log = LoggerFactory.getLogger(StorageRegionResource.class);

    private final ClusterConfig clusterConfig;
    private final StorageRegionManager storageRegionManager;
    private final ServiceManager serviceManager;

    private final ZookeeperPaths zkPaths;
    private final RocksDBManager rocksDBManager;
    private final CuratorFramework client;

    @Inject
    public LegacyStorageRegionResouce(
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
        @Context UriInfo uriInfo) {
        if (storageRegionManager.exists(name)) {
            return Response.status(Status.CONFLICT)
                           .entity(StringUtils.format("Storage Region[%s] has been existed", name))
                           .build();
        }

        Properties properties = new Properties();
        MultivaluedMap<String, String> queryParams = uriInfo.getQueryParameters();
        queryParams.forEach((key, values) -> {
            properties.setProperty(key, values.get(0));
        });

        try {
            StorageRegion storageRegion = storageRegionManager.createStorageRegion(
                name,
                StorageRegionProperties.withDefault().override(properties));
            rocksDBManager.createColumnFamilyWithTtl(name, (int) Duration
                .parse(StorageRegionProperties.withDefault().override(properties).getDataTtl()).getSeconds());
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
        @Context UriInfo uriInfo) {
        if (!storageRegionManager.exists(name)) {
            return Response.status(Status.NOT_FOUND).build();
        }

        Properties properties = new Properties();
        MultivaluedMap<String, String> queryParams = uriInfo.getQueryParameters();
        queryParams.forEach((key, values) -> {
            properties.setProperty(key, values.get(0));
        });

        try {
            storageRegionManager.updateStorageRegion(name, properties);
            return Response.ok().build();
        } catch (Exception e) {
            log.error(StringUtils.format("can not update storage region[%s]", name), e);
            return Response.serverError().entity(Throwables.getStackTraceAsString(e)).build();
        }
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

        return Response.ok(Ints.toByteArray(node.getId())).build();
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
        ReturnCode code = TasksUtils.createUserDeleteTask(
            client,
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
