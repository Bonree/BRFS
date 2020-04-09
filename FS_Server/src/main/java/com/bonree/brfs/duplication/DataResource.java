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

import com.bonree.brfs.client.data.NextData;
import com.bonree.brfs.client.utils.HttpStatus;
import com.bonree.brfs.common.net.http.HandleResult;
import com.bonree.brfs.common.net.http.HandleResultCallback;
import com.bonree.brfs.common.net.http.data.FSPacket;
import com.bonree.brfs.common.proto.DataTransferProtos.FSPacketProto;
import com.bonree.brfs.duplication.datastream.blockcache.BlockManagerInterface;
import com.bonree.brfs.duplication.datastream.writer.StorageRegionWriteCallback;
import com.bonree.brfs.duplication.datastream.writer.StorageRegionWriter;
import com.bonree.brfs.duplication.storageregion.StorageRegionManager;
import com.bonree.brfs.rocksdb.RocksDBManager;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Response;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.MediaType.APPLICATION_OCTET_STREAM;

@Path("/data")
public class DataResource {
    private static final Logger LOG = LoggerFactory.getLogger(DataResource.class);

    private final StorageRegionWriter storageRegionWriter;
    private final BlockManagerInterface blockManager;
//    private final StorageRegionManager storageRegionManager;
    private final RocksDBManager rocksDBManager;


    @Inject
    public DataResource(
            StorageRegionWriter storageRegionWriter,
            BlockManagerInterface blockManager,
            StorageRegionManager storageRegionManager,
            RocksDBManager rocksDBManager) {
        this.storageRegionWriter = storageRegionWriter;
        this.blockManager = blockManager;
//        this.storageRegionManager = storageRegionManager;
        this.rocksDBManager = rocksDBManager;
    }

    @POST
    @Path("{srName}")
    @Consumes(APPLICATION_OCTET_STREAM)
    @Produces(APPLICATION_JSON)
    public void write(
            @PathParam("srName") String srName,
            FSPacketProto data,
            @Suspended AsyncResponse response) {
        LOG.debug("DONE decode");
        LOG.debug("{}",data);
        try {
            FSPacket packet = new FSPacket();
            packet.setProto(data);
            LOG.debug("write request data length：[{}]，prepare to append to block，",packet.getData().length);
            int storage = packet.getStorageName();
//            String storageName = storageRegionManager.findStorageRegionById(storage).getName();
            String file = packet.getFileName();
            LOG.debug("deserialize [{}]",packet);
            //如果是一个小于等于packet长度的文件，由handler直接写
            if(packet.isATinyFile(blockManager.getBlockSize())){
                LOG.debug("writing a tiny file [{}]",packet.getFileName());
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
                                LOG.info("response file :[{}]:fid[{}]",packet.getFileName(),fid);
                            }

                            @Override
                            public void complete(String[] fids) {
                                response.resume(ImmutableList.of(fids));
                                LOG.info("response file[{}]:fid[{}]",packet.getFileName(),fids[0]);
                            }
                        });
                return;
            }
            LOG.debug("append packet[{}] into block",packet);
            //===== 追加数据的到blockManager
            blockManager.appendToBlock(packet, new HandleResultCallback() {
                @Override
                public void completed(HandleResult result) {
                    if(result.isCONTINUE()) {
                        LOG.debug("response seqno：{}",result.getNextSeqno());
                        response.resume(Response
                                .status(HttpStatus.CODE_NEXT)
                                .entity(new NextData(result.getNextSeqno())).build());
                    }else if(result.isSuccess()){
                        String fid = new String (result.getData());
                        //todo rocksdb
                        LOG.info("response fid:[{}]",fid);
                        response.resume(Response
                                .ok()
                                .entity(ImmutableList.of(new String(result.getData()))).build());
                    }else{
                        LOG.debug("response error");
                        response.resume(result.getCause());
                    }
                }
            });
        } catch (Exception e) {
            LOG.error("handle write data message error", e);
            response.resume(e);
        }

    }
}
