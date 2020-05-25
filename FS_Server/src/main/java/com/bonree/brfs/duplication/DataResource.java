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

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.MediaType.APPLICATION_OCTET_STREAM;

import com.bonree.brfs.client.data.NextData;
import com.bonree.brfs.client.utils.HttpStatus;
import com.bonree.brfs.client.utils.Strings;
import com.bonree.brfs.common.ReturnCode;
import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.net.http.HandleResultCallback;
import com.bonree.brfs.common.net.http.data.FSPacket;
import com.bonree.brfs.common.proto.DataTransferProtos.FSPacketProto;
import com.bonree.brfs.common.proto.DataTransferProtos.WriteBatch;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.common.statistic.WriteStatCollector;
import com.bonree.brfs.common.write.data.DataItem;
import com.bonree.brfs.duplication.catalog.BrfsCatalog;
import com.bonree.brfs.duplication.datastream.blockcache.BlockManager;
import com.bonree.brfs.duplication.datastream.writer.StorageRegionWriteCallback;
import com.bonree.brfs.duplication.datastream.writer.StorageRegionWriter;
import com.bonree.brfs.duplication.storageregion.StorageRegion;
import com.bonree.brfs.duplication.storageregion.StorageRegionManager;
import com.bonree.brfs.guice.ClusterConfig;
import com.bonree.brfs.schedulers.utils.TasksUtils;
import com.google.common.collect.ImmutableList;
import java.time.Duration;
import java.util.List;
import javax.inject.Inject;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/data/v2")
public class DataResource {
    private static final Logger LOG = LoggerFactory.getLogger(DataResource.class);
    private final WriteStatCollector writeCollector;
    private final ClusterConfig clusterConfig;
    private final ServiceManager serviceManager;
    private final StorageRegionManager storageRegionManager;
    private final ZookeeperPaths zkPaths;

    private final StorageRegionWriter storageRegionWriter;
    private final BlockManager blockManager;
    private final BrfsCatalog brfsCatalog;

    @Inject
    public DataResource(
        ClusterConfig clusterConfig,
        ServiceManager serviceManager,
        StorageRegionManager storageRegionManager,
        ZookeeperPaths zkPaths,
        StorageRegionWriter storageRegionWriter,
        BlockManager blockManager,
        BrfsCatalog brfsCatalog,
        WriteStatCollector writeStatCollector) {
        this.writeCollector = writeStatCollector;
        this.clusterConfig = clusterConfig;
        this.serviceManager = serviceManager;
        this.storageRegionManager = storageRegionManager;
        this.zkPaths = zkPaths;
        this.storageRegionWriter = storageRegionWriter;
        this.blockManager = blockManager;
        this.brfsCatalog = brfsCatalog;

    }

    @POST
    @Path("batch/{srName}")
    @Consumes(APPLICATION_OCTET_STREAM)
    @Produces(APPLICATION_JSON)
    public void writeBatch(@PathParam("srName") String srName,
                           WriteBatch batch,
                           @Suspended AsyncResponse response) {
        if (!storageRegionManager.exists(srName)) {
            throw new WebApplicationException("storage:" + srName + "is not exist!", HttpStatus.CODE_STORAGE_NOT_EXIST);
        }
        final BatchDatas datas = new BatchDatas(batch.getItemsCount(), brfsCatalog.isUsable());
        batch.getItemsList().forEach(datas::add);

        storageRegionWriter.write(
            srName,
            datas.getDatas(),
            new StorageRegionWriteCallback() {

                @Override
                public void complete(String[] fids) {
                    String[] fileNames = datas.getFileNames();
                    for (int i = 0; i < fileNames.length; i++) {
                        String fileName = fileNames[i];
                        if (fileName == null) {
                            continue;
                        }

                        if (brfsCatalog.isUsable() && brfsCatalog.writeFid(srName, fileName, fids[i])) {
                            LOG.error("failed when write fid to rocksDB for file[%s].", fileName);

                            // set fid null after error
                            fids[i] = null;
                        }
                    }

                    response.resume(fids);
                }

                @Override
                public void complete(String fid) {
                    response.resume(Response.serverError().build());
                    throw new RuntimeException("Batch writting should not return a single fid");
                }

                @Override
                public void error(Throwable cause) {
                    response.resume(cause);
                }
            });
    }

    @POST
    @Path("{srName}")
    @Consumes(APPLICATION_OCTET_STREAM)
    @Produces(APPLICATION_JSON)
    public void write(
        @PathParam("srName") String srName,
        FSPacketProto data,
        @Suspended AsyncResponse response) {
        if (!storageRegionManager.exists(srName)) {
            throw new WebApplicationException("storage:" + srName + "is not exist!", HttpStatus.CODE_STORAGE_NOT_EXIST);
        }
        try {
            FSPacket packet = new FSPacket();
            packet.setProto(data);
            LOG.debug("write request data length：[{}]，prepare to append to block，", packet.getData().length);
            String file = packet.getFileName();
            if (brfsCatalog.isUsable()) {
                if (checkNotNull(file) && !brfsCatalog.validPath(file)) {
                    LOG.warn("file path [{}]is invalid.", file);
                    throw new WebApplicationException("file path " + file + "is invalid", HttpStatus.CODE_NOT_AVAILABLE_FILENAME);
                }
            } else if (!checkNotNull(file)) {
                String resp = "the rocksDB is not open, can not write with file name";
                LOG.warn(resp);
                throw new WebApplicationException(resp, HttpStatus.CODE_NOT_ALLOW_CUSTOM_FILENAME);
            }
            if (packet.getSeqno() == 1) {
                LOG.debug("file [{}] is allow to write!", packet.getWriteID());
            }
            LOG.debug("deserialize [{}]", packet);
            //如果是一个小于等于packet长度的文件，由handler直接写
            if (packet.isATinyFile()) {
                LOG.debug("writing a tiny file [{}]", packet.getFileName());
                storageRegionWriter.write(
                    srName,
                    packet.getData(),
                    new StorageRegionWriteCallback() {

                        @Override
                        public void error(Throwable cause) {
                            response.resume(cause);
                        }

                        @Override
                        public void complete(String fid) {
                            if (fid == null) {
                                response.resume(Response.serverError().build());
                                return;
                            }

                            if (brfsCatalog.isUsable() && brfsCatalog.validPath(file)) {
                                if (brfsCatalog.writeFid(srName, file, fid)) {
                                    LOG.error("failed when write fid to rocksDB.");
                                    response.resume(new Exception("write fid to rocksDB failed."));
                                    return;
                                }
                            }
                            LOG.debug("response fid:[{}]", fid);
                            writeCollector.submit(srName);
                            response.resume(Response
                                                .ok()
                                                .entity(ImmutableList.of(fid)).build());

                        }

                        @Override
                        public void complete(String[] fids) {
                            response.resume(ImmutableList.of(fids));
                            LOG.debug("response file[{}]:fid[{}]", packet.getFileName(), fids[0]);
                        }
                    });
                return;
            }
            HandleResultCallback callback = result -> {
                if (result.isToContinue()) {
                    LOG.debug("response seqno：{}", result.getNextSeqno());
                    response.resume(Response
                                        .status(HttpStatus.CODE_NEXT)
                                        .entity(new NextData(result.getNextSeqno())).build());
                } else if (result.isSuccess()) {
                    String fid = new String(result.getData());
                    LOG.debug("before sync : [{}]", fid);

                    if (brfsCatalog.isUsable() && brfsCatalog.validPath(file)) {
                        if (brfsCatalog.writeFid(srName, file, fid)) {
                            LOG.error("failed when write fid to rocksDB.");
                            response.resume(new Exception("write fid to rocksDB failed."));
                            return;
                        }
                        LOG.debug("sync catalog into rocksDB. filename:[{}]", file);
                    }
                    LOG.info("response fid:[{}]", fid);
                    writeCollector.submit(srName);
                    response.resume(Response
                                        .ok()
                                        .entity(ImmutableList.of(new String(result.getData()))).build());
                } else {
                    LOG.error("response error ");
                    response.resume(result.getCause());
                }
            };
            if (packet.isTheFirstPacketInFile()) {
                blockManager.addToWaitingPool(srName, packet, callback);
                LOG.debug("put a file [{}] into the waiting pool", packet.getFileName());
                return;
            }
            LOG.debug("append packet[{}] into block", packet);
            //===== 追加数据的到blockManager
            blockManager.appendToBlock(srName, packet, callback);
        } catch (BadRequestException e) {
            throw e;
        } catch (Exception e) {
            LOG.error("handle write data message error", e);
            response.resume(e);
        }

    }

    @DELETE
    @Path("{srName}")
    public Response deleteData(
        @PathParam("srName") String srName,
        @QueryParam("startTime") String startTime,
        @QueryParam("endTime") String endTime) {
        StorageRegion storageRegion = storageRegionManager.findStorageRegionByName(srName);
        if (storageRegion == null) {
            return Response.status(Status.BAD_REQUEST)
                           .entity(Strings.format("storage region[%s] is not existed", srName))
                           .build();
        }

        long startTimestamp;
        long endTimeStamp;
        try {
            startTimestamp = DateTime.parse(startTime).getMillis();
            endTimeStamp = DateTime.parse(endTime).getMillis();
            checkTime(startTimestamp, endTimeStamp,
                      storageRegion.getCreateTime(),
                      Duration.parse(storageRegion.getFilePartitionDuration()).toMillis());
        } catch (Exception e) {
            LOG.error("check time error", e);
            return Response.status(Status.BAD_REQUEST).entity(e.getMessage()).build();
        }

        List<Service> serviceList = serviceManager.getServiceListByGroup(clusterConfig.getDataNodeGroup());
        ReturnCode code =
            TasksUtils.createUserDeleteTask(serviceList, zkPaths, storageRegion, startTimestamp, endTimeStamp, false);
        if (ReturnCode.SUCCESS.equals(code)) {
            return Response.ok().build();
        }

        return Response.status(Status.BAD_REQUEST).entity(code.name()).build();
    }

    private void checkTime(long start, long end, long ctime, long granule) {
        if (start != (start - start % granule) || end != (end - end % granule)) {
            throw new IllegalArgumentException(Strings.format(
                "starttime and endTime granule is not match !!! startTime: [{}], endTime:[{}], granue:[{}]",
                start, end, granule));
        }

        long currentTime = System.currentTimeMillis();
        long cuGra = currentTime - currentTime % granule;
        long sgra;
        long egra;
        sgra = start;
        egra = end;
        // 2.开始时间等于结束世界
        if (sgra >= egra) {
            throw new IllegalArgumentException("param error: start time is equal to end time");
        }

        // 3.结束时间小于创建时间
        if (ctime > egra) {
            throw new IllegalArgumentException("time earlier than create error");
        }

        // 4.当前时间
        if (cuGra <= sgra || cuGra < egra) {
            throw new IllegalArgumentException("forbid delete current error");
        }
    }

    private class BatchDatas {
        private final DataItem[] items;
        private final String[] fileNames;
        private final boolean pathOn;
        private int index;

        public BatchDatas(int count, boolean pathOn) {
            this.items = new DataItem[count];
            this.fileNames = new String[count];
            this.pathOn = pathOn;
        }

        public void add(FSPacketProto data) {
            String fileName = data.getFileName();
            if (fileName != null && !fileName.isEmpty()) {
                if (!pathOn) {
                    String resp = "the rocksDB is not open, can not write with file name";
                    LOG.warn(resp);
                    throw new WebApplicationException(resp, HttpStatus.CODE_NOT_ALLOW_CUSTOM_FILENAME);
                }

                if (!brfsCatalog.validPath(fileName)) {
                    LOG.warn("file path [{}]is invalid.", fileName);
                    throw new WebApplicationException(
                        "file path " + fileName + "is invalid",
                        HttpStatus.CODE_NOT_AVAILABLE_FILENAME);
                }

                fileNames[index] = data.getFileName();
            } else if (pathOn) {
                throw new WebApplicationException(
                    "file path " + fileName + "is invalid",
                    HttpStatus.CODE_NOT_AVAILABLE_FILENAME);
            }

            items[index] = new DataItem(data.getData().toByteArray());

            index++;
        }

        public DataItem[] getDatas() {
            return items;
        }

        public String[] getFileNames() {
            return fileNames;
        }
    }

    private boolean checkNotNull(String args) {
        return args != null && !args.equals("");
    }

}
