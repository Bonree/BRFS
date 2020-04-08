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
package com.bonree.brfs.metrics.zookeeper;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.MediaType.TEXT_PLAIN;

import java.util.List;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;

@Path("/zookeeper")
public class ZookeeperResource {
    
    @GET
    @Path("/root")
    @Produces(TEXT_PLAIN)
    public String rootNode() {
        return null;
    }
    
    @GET
    @Path("/list/{nodePath}")
    @Produces(APPLICATION_JSON)
    public List<String> list(@PathParam("nodePath") String nodePath) {
        return null;
    }
    
    @GET
    @Path("/data/{nodePath}")
    @Produces(APPLICATION_JSON)
    public NodeData get(@PathParam("nodePath") String nodePath) {
        return null;
    }
}
