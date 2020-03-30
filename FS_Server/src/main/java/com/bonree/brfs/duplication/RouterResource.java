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

import java.util.List;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

import com.bonree.brfs.client.route.NormalRouterNode;
import com.bonree.brfs.client.route.RouterNode;
import com.bonree.brfs.client.route.SecondServerID;
import com.bonree.brfs.client.route.VirtualRouterNode;
import com.bonree.brfs.server.identification.ServerIDManager;

@Path("/router")
public class RouterResource {
    private final ServerIDManager idManager;
    
    @Inject
    public RouterResource(ServerIDManager idManager) {
        this.idManager = idManager;
    }

    @GET
    @Path("secondServerID/{srName}")
    public List<SecondServerID> getAllSecondServerID(@PathParam("srName") String srName) {
        // TODO
        return null;
    }
    
    @GET
    @Path("secondServerID/{srName}/{dataNodeId}")
    public List<SecondServerID> getSercondServerID(
            @PathParam("srName") String srName,
            @PathParam("dataNodeId") String dataNodeId) {
     // TODO
        return null;
    }
    
    @GET
    @Path("update/{srName}")
    public List<RouterNode> getUpdate(@PathParam("srName") String srName) {
     // TODO
        return null;
    }
    
    @GET
    @Path("update/virtual/{srName}")
    public List<VirtualRouterNode> getVirtualUpdate(@PathParam("srName") String srName) {
     // TODO
        return null;
    }
    
    @GET
    @Path("update/normal/{srName}")
    public List<NormalRouterNode> getNormalUpdate(@PathParam("srName") String srName) {
     // TODO
        return null;
    }
}
