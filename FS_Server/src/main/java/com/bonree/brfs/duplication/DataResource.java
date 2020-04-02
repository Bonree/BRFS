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

import java.util.List;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Response;

import com.bonree.brfs.client.data.NextData;
import com.bonree.brfs.common.proto.DataTransferProtos.FSPacketProto;
import com.bonree.brfs.duplication.datastream.blockcache.BlockManager;
import com.bonree.brfs.duplication.datastream.writer.StorageRegionWriteCallback;
import com.bonree.brfs.duplication.datastream.writer.StorageRegionWriter;
import com.google.common.collect.ImmutableList;

@Path("/data")
public class DataResource {
    private final StorageRegionWriter storageRegionWriter;
    private final BlockManager blockManager;
    
    @Inject
    public DataResource(
            StorageRegionWriter storageRegionWriter) {
        this.storageRegionWriter = storageRegionWriter;
        this.blockManager = null;
    }

    @POST
    @Path("{srName}")
    @Consumes(APPLICATION_OCTET_STREAM)
    @Produces(APPLICATION_JSON)
    public void write(
            @PathParam("srName") String srName,
            FSPacketProto data,
            @Suspended AsyncResponse response) {
        // example
        storageRegionWriter.write(
                data.getStorageName(),
                data.getData().toByteArray(),
                new StorageRegionWriteCallback() {
                    
                    @Override
                    public void error() {
                        response.resume(new Exception());
                    }
                    
                    @Override
                    public void complete(String fid) {
                        response.resume(ImmutableList.of(fid));
                    }
                    
                    @Override
                    public void complete(String[] fids) {
                        response.resume(fids);
                    }
                });
    }
}
