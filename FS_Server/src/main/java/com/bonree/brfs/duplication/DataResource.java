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

import java.time.Duration;
import java.util.List;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.client.data.NextData;
import com.bonree.brfs.client.utils.HttpStatus;
import com.bonree.brfs.client.utils.Strings;
import com.bonree.brfs.common.ReturnCode;
import com.bonree.brfs.common.ZookeeperPaths;
import com.bonree.brfs.common.net.http.HandleResult;
import com.bonree.brfs.common.net.http.HandleResultCallback;
import com.bonree.brfs.common.net.http.data.FSPacket;
import com.bonree.brfs.common.proto.DataTransferProtos.FSPacketProto;
import com.bonree.brfs.common.service.Service;
import com.bonree.brfs.common.service.ServiceManager;
import com.bonree.brfs.duplication.datastream.blockcache.BlockManagerInterface;
import com.bonree.brfs.duplication.datastream.writer.StorageRegionWriteCallback;
import com.bonree.brfs.duplication.datastream.writer.StorageRegionWriter;
import com.bonree.brfs.duplication.storageregion.StorageRegion;
import com.bonree.brfs.duplication.storageregion.StorageRegionManager;
import com.bonree.brfs.guice.ClusterConfig;
import com.bonree.brfs.schedulers.utils.TasksUtils;
import com.google.common.collect.ImmutableList;

@Path("/data")
public class DataResource {
    private static final Logger LOG = LoggerFactory.getLogger(DataResource.class);

    private final ClusterConfig clusterConfig;
    private final ServiceManager serviceManager;
    private final StorageRegionManager storageRegionManager;
    private final ZookeeperPaths zkPaths;
    
    private final StorageRegionWriter storageRegionWriter;
    private final BlockManagerInterface blockManager;


    @Inject
    public DataResource(
            ClusterConfig clusterConfig,
            ServiceManager serviceManager,
            StorageRegionManager storageRegionManager,
            ZookeeperPaths zkPaths,
            StorageRegionWriter storageRegionWriter,
            BlockManagerInterface blockManager) {
        this.clusterConfig = clusterConfig;
        this.serviceManager = serviceManager;
        this.storageRegionManager = storageRegionManager;
        this.zkPaths = zkPaths;
        this.storageRegionWriter = storageRegionWriter;
        this.blockManager = blockManager;
    }

    @POST
    @Path("{srName}")
    @Consumes(APPLICATION_OCTET_STREAM)
    @Produces(APPLICATION_JSON)
    public void write(
            @PathParam("srName") String srName,
            FSPacketProto data,
            @Suspended AsyncResponse response) {
        LOG.debug("DONE decode ,从请求中取出data");
        LOG.debug("{}",data);
        try {
            FSPacket packet = new FSPacket();
            packet.setProto(data);
            LOG.debug("收到数据长度为：[{}]，尝试将其填充到block中，",packet.getData().length);
            int storage = packet.getStorageName();
            String file = packet.getFileName();
            LOG.debug("从数据中反序列化packet [{}]",packet);
            //如果是一个小于等于packet长度的文件，由handler直接写
            if(packet.isATinyFile(blockManager.getBlockSize())){
                LOG.debug("一条超小文件[{}]",packet.getFileName());
                storageRegionWriter.write(
                        packet.getStorageName(),
                        packet.getData(),
                        new StorageRegionWriteCallback() {

                            @Override
                            public void error() {
                                response.resume(new Exception());
                            }

                            @Override
                            public void complete(String fid) {
                                response.resume(ImmutableList.of(fid));
                                LOG.info("返回文件[{}]:fid[{}]",packet.getFileName(),fid);
                            }

                            @Override
                            public void complete(String[] fids) {
                                response.resume(ImmutableList.of(fids));
                                LOG.info("返回文件[{}]:fid[{}]",packet.getFileName(),fids[0]);
                            }
                        });
                return;
            }
            LOG.debug("填充内存");
            //===== 追加数据的到blockManager
            blockManager.appendToBlock(packet, new HandleResultCallback() {
                @Override
                public void completed(HandleResult result) {
                    if(result.isCONTINUE()) {
                        LOG.debug("返回seqno：{}",result.getNextSeqno());
                        response.resume(Response
                                .status(HttpStatus.CODE_NEXT)
                                .entity(new NextData(result.getNextSeqno())).build());
                    }else if(result.isSuccess()){
                        LOG.info("返回fid:[{}]",new String(result.getData()));
                        response.resume(Response
                                .ok()
                                .entity(ImmutableList.of(new String(result.getData()))).build());
                    }else{
                        LOG.debug("返回错误");
                        response.resume(result.getCause());
                    }
                }
            });
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
        if(storageRegion == null) {
            return Response.status(Status.BAD_REQUEST)
                    .entity(Strings.format("storage region[%s] is not existed", srName))
                    .build();
        }
        
        long startTimestamp = 0;
        long endTimeStamp = 0;
        try {
            startTimestamp = DateTime.parse(startTime).getMillis();
            endTimeStamp = DateTime.parse(endTime).getMillis();
            checkTime(startTimestamp, endTimeStamp, storageRegion.getCreateTime(), Duration.parse(storageRegion.getFilePartitionDuration()).toMillis());
        } catch (Exception e) {
            LOG.error("check time error", e);
            return Response.status(Status.BAD_REQUEST).entity(e.getMessage()).build();
        }
        
        List<Service> serviceList = serviceManager.getServiceListByGroup(clusterConfig.getDataNodeGroup());
        ReturnCode code = TasksUtils.createUserDeleteTask(serviceList, zkPaths, storageRegion, startTimestamp, endTimeStamp, false);
        if(ReturnCode.SUCCESS.equals(code)) {
            return Response.ok().build();
        }
        
        return Response.status(Status.BAD_REQUEST).entity(code.name()).build();
    }
    
    private void checkTime(long start, long end, long cTime, long granule) throws Exception {
        if (start != (start - start % granule) || end != (end - end % granule)) {
            throw new IllegalArgumentException(Strings.format(
                    "starttime and endTime granule is not match !!! startTime: [{}], endTime:[{}], granue:[{}]",
                    start, end, granule));
        }

        long currentTime = System.currentTimeMillis();
        long cuGra = currentTime - currentTime % granule;
        long sGra = start - start % granule;
        long eGra = end - end % granule;
        sGra = start;
        eGra = end;
        // 2.开始时间等于结束世界
        if (sGra >= eGra) {
            throw new IllegalArgumentException("param error: start time is equal to end time");
        }

        // 3.结束时间小于创建时间
        if (cTime > eGra) {
            throw new IllegalArgumentException("time earlier than create error");
        }

        // 4.当前时间
        if (cuGra <= sGra || cuGra < eGra) {
            throw new IllegalArgumentException("forbid delete current error");
        }
    }
}
