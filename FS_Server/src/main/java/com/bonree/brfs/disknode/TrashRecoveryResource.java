package com.bonree.brfs.disknode;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.MediaType.APPLICATION_OCTET_STREAM;

import com.bonree.brfs.client.utils.HttpStatus;
import com.bonree.brfs.disknode.trash.recovery.RecoveryFileFromTrashManager;
import com.bonree.brfs.disknode.trash.recovery.TrashRecoveryCallBack;
import com.bonree.brfs.duplication.storageregion.StorageRegion;
import com.bonree.brfs.duplication.storageregion.StorageRegionManager;
import java.util.List;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @ClassName TrashRecoveryResource
 * @Description
 * @Author Tang Daqian
 * @Date 2021/1/4 19:20
 **/
@Path("/trash")
public class TrashRecoveryResource {
    private static final Logger log = LoggerFactory.getLogger(TrashRecoveryResource.class);
    private StorageRegionManager srManager;
    private RecoveryFileFromTrashManager recoveryTrashManager;

    @Inject
    public TrashRecoveryResource(StorageRegionManager srManager,
                                 RecoveryFileFromTrashManager recoveryTrashManager) {
        this.srManager = srManager;
        this.recoveryTrashManager = recoveryTrashManager;
    }

    @GET
    @Path("allRecovery")
    public void allDatanodeRecovery() {
        recoveryTrashManager.recoveryFileForAllNode();
    }

    @GET
    @Path("allRecovery/{srName}")
    public void allDatanodeRecovery(@PathParam("srName") String srName) {
        recoveryTrashManager.recoveyFileForAllNodeBySrName(srName);
    }

    @GET
    @Path("fullRecovery")
    public void reoveryAllTrash(@Suspended AsyncResponse response) {
        List<StorageRegion> storageRegionList = srManager.getStorageRegionList();
        for (StorageRegion storageRegion : storageRegionList) {
            String srName = storageRegion.getName();
            reoveryAllTrashFilesForStorageRegion(srName, 0, response);
        }
    }

    @GET
    @Path("fullRecovery/{srName}")
    @Consumes(APPLICATION_OCTET_STREAM)
    @Produces(APPLICATION_JSON)
    public void reoveryAllTrashFilesForStorageRegion(@PathParam("srName") String srName,
                                                     @QueryParam("timeStamp") long timeStamp,
                                                     @Suspended AsyncResponse response) {
        TrashRecoveryCallBack callBack = getCallBack(srName, response);
        recoveryTrashManager.recovery(() -> recoveryTrashManager.reoveryAllTrashFilesForStorageRegion(srName,
                                                                                                      timeStamp,
                                                                                                      callBack), callBack);
    }

    @GET
    @Path("intervalDirRecovery/{srName}")
    @Consumes(APPLICATION_OCTET_STREAM)
    @Produces(APPLICATION_JSON)
    public void reoveryTrashFilesWithTimeInterval(@PathParam("srName") String srName,
                                                  @QueryParam("lowTimeBoundary") long lowTimeBoundary,
                                                  @QueryParam("highTimeBoundary") long highTimeBoundary,
                                                  @Suspended AsyncResponse response) {
        TrashRecoveryCallBack callBack = getCallBack(srName, response);
        recoveryTrashManager.recovery(() -> recoveryTrashManager.reoveryTrashFilesWithTimeInterval(srName,
                                                                                                   lowTimeBoundary,
                                                                                                   highTimeBoundary,
                                                                                                   callBack), callBack);
    }

    private TrashRecoveryCallBack getCallBack(String srName,
                                                AsyncResponse response) {
        if (!srManager.exists(srName)) {
            log.info("storage [{}] is not exists.", srName);
            throw new WebApplicationException("storage:" + srName + "is not exist!", HttpStatus.CODE_STORAGE_NOT_EXIST);
        }

        return new TrashRecoveryCallBack() {
            @Override
            public void complete() {
                response.resume(Response.ok()
                                        .encoding("utf-8")
                                        .entity("recovery from trash can success"));
            }

            @Override
            public void error(Throwable cause) {
                response.resume(cause);
            }
        };
    }
}
