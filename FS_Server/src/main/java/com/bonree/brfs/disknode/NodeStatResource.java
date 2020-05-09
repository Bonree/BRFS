package com.bonree.brfs.disknode;

import com.bonree.brfs.common.resource.vo.NodeSnapshotInfo;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.resource.ResourceGatherInterface;
import com.google.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/resource")
public class NodeStatResource {
    private static final Logger LOG = LoggerFactory.getLogger(NodeStatResource.class);
    private ResourceGatherInterface gather;

    @Inject
    public NodeStatResource(ResourceGatherInterface gather) {
        this.gather = gather;
    }

    @GET
    public Response collectGetStat() {
        try {
            NodeSnapshotInfo snapshotInfo = gather.gatherSnapshot();
            byte[] data = JsonUtils.toJsonBytes(snapshotInfo);
            if (data == null || data.length == 0) {
                return Response
                    .status(Response.Status.INTERNAL_SERVER_ERROR)
                    .encoding("utf-8")
                    .entity("gather resource is empty")
                    .build();
            }
            return Response.status(Response.Status.OK)
                           .entity(data)
                           .encoding("utf-8")
                           .build();
        } catch (Exception e) {
            LOG.error("gui gather resource happen error ", e);
        }
        return Response
            .status(Response.Status.INTERNAL_SERVER_ERROR)
            .encoding("utf-8")
            .entity("gather resource happen exception")
            .build();
    }

    @POST
    public Response collectPostStat() {
        try {
            NodeSnapshotInfo snapshotInfo = gather.gatherSnapshot();
            byte[] data = JsonUtils.toJsonBytes(snapshotInfo);
            if (data == null || data.length == 0) {
                return Response
                    .status(Response.Status.INTERNAL_SERVER_ERROR)
                    .encoding("utf-8")
                    .entity("gather resource is empty")
                    .build();
            }
            return Response.status(Response.Status.OK)
                           .entity(data)
                           .encoding("utf-8")
                           .build();
        } catch (Exception e) {
            LOG.error("gui gather resource happen error ", e);
        }
        return Response
            .status(Response.Status.INTERNAL_SERVER_ERROR)
            .encoding("utf-8")
            .entity("gather resource happen exception")
            .build();
    }

}
