package com.bonree.brfs.duplication;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.MediaType.APPLICATION_OCTET_STREAM;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;

import com.bonree.brfs.common.proto.DataTransferProtos.WriteBatch;
import com.bonree.brfs.common.serialize.ProtoStuffUtils;
import com.bonree.brfs.common.write.data.WriteDataMessage;
import com.bonree.brfs.duplication.storageregion.StorageRegion;
import com.bonree.brfs.duplication.storageregion.StorageRegionManager;
import com.bonree.brfs.duplication.storageregion.exception.StorageRegionNonexistentException;
import java.io.InputStream;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Response;

@Deprecated
@Path("/data")
public class LegacyDataResource {

    private final DataResource dataResource;
    private final StorageRegionManager storageRegionManager;

    @Inject
    public LegacyDataResource(DataResource dataResource,
                              StorageRegionManager storageRegionManager) {
        this.dataResource = dataResource;
        this.storageRegionManager = storageRegionManager;
    }

    @POST
    @Consumes(APPLICATION_OCTET_STREAM)
    @Produces(APPLICATION_JSON)
    public void write(
        InputStream input,
        @Suspended AsyncResponse response) {
        WriteDataMessage writeDatas = ProtoStuffUtils.readFrom(input, WriteDataMessage.class);
        StorageRegion storageRegion = storageRegionManager.findStorageRegionById(writeDatas.getStorageNameId());
        if (storageRegion == null) {
            response.resume(new StorageRegionNonexistentException("id=" + writeDatas.getStorageNameId()));
            return;
        }

        WriteBatch.Builder builder = WriteBatch.newBuilder();

        dataResource.writeBatch(storageRegion.getName(), builder.build(), response);
    }

    @DELETE
    @Path("{srId}/{interval}")
    public Response deleteData(
        @PathParam("srId") String srId,
        @PathParam("interval") String interval) throws Exception {
        StorageRegion storageRegion = storageRegionManager.findStorageRegionById(Integer.parseInt(srId));
        if (storageRegion == null) {
            throw new StorageRegionNonexistentException("id=" + Integer.parseInt(srId));
        }

        String[] times = interval.split("_");
        if (times.length != 2) {
            return Response.status(BAD_REQUEST).build();
        }

        return dataResource.deleteData(storageRegion.getName(), times[0], times[1]);
    }
}
