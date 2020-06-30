package com.bonree.brfs.duplication;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.MediaType.APPLICATION_OCTET_STREAM;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;

import com.bonree.brfs.common.serialize.ProtoStuffUtils;
import com.bonree.brfs.common.write.data.WriteDataMessage;
import com.bonree.brfs.duplication.datastream.writer.StorageRegionWriteCallback;
import com.bonree.brfs.duplication.datastream.writer.StorageRegionWriter;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Deprecated
@Path("/data")
public class LegacyDataResource {
    private static final Logger log = LoggerFactory.getLogger(LegacyDataResource.class);

    private final DataResource dataResource;
    private final StorageRegionManager storageRegionManager;
    private final StorageRegionWriter storageRegionWriter;

    @Inject
    public LegacyDataResource(DataResource dataResource,
                              StorageRegionManager storageRegionManager,
                              StorageRegionWriter storageRegionWriter) {
        this.dataResource = dataResource;
        this.storageRegionManager = storageRegionManager;
        this.storageRegionWriter = storageRegionWriter;
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

        storageRegionWriter.write(
            storageRegion.getName(),
            writeDatas.getItems(),
            new StorageRegionWriteCallback() {

                @Override
                public void complete(String[] fids) {
                    response.resume(fids);
                }

                @Override
                public void complete(String fid) {
                    response.resume(Response.serverError().build());
                    throw new RuntimeException("Batch writting should not return a single fid");
                }

                @Override
                public void error(Throwable cause) {
                    log.error("write data error", cause);
                    response.resume(cause);
                }
            });
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
