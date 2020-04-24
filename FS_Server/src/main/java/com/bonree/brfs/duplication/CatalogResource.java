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

import com.bonree.brfs.duplication.catalog.BrfsCatalog;
import com.bonree.brfs.duplication.catalog.Inode;
import java.util.List;
import javax.inject.Inject;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.ProcessingException;
import javax.ws.rs.QueryParam;
import javax.ws.rs.ServiceUnavailableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/catalog")
public class CatalogResource {
    private static final Logger LOG = LoggerFactory.getLogger(CatalogResource.class);
    private final BrfsCatalog catalog;
    @Inject
    public CatalogResource(BrfsCatalog catalog) {
        this.catalog = catalog;
    }
    @GET
    @Path("fid/{srName}")
    public String getFid (
            @PathParam("srName") String srName,
            @QueryParam("absPath") String absPath) {
        LOG.info("test for get fid,[{}],[{}]" ,srName,absPath);
        LOG.info("get fid request srName[{}],absPath[{}]",srName,absPath);
        //todo 参数检查
        if(!catalog.isUsable()){
            LOG.error("get fid error caused by the catalog is not open");
            throw new ServiceUnavailableException("get fid error caused by the catalog is not open");
        }
        if(!catalog.validPath(absPath)){
            LOG.error("invalid file path [{}]",absPath);
            throw new BadRequestException("invalid file path:"+absPath);
        }
        String fid = null;
        try {
            fid = catalog.getFid(srName, absPath);
        }catch (Exception e){
            LOG.error("error when get fid from rocksDb!");
            throw new ProcessingException("error when get fid from rocksDb");
        }

        if(fid == null) {
            LOG.error("get null from rocksDB");
            throw new ServiceUnavailableException("get null when get fid from catalog!");
        }
        return fid;
    }

    @GET
    @Path("/isFile")
    public boolean isFile (
        @PathParam("srName") String srName,
        @QueryParam("nodePath") String nodePath) {
        return catalog.isFileNode(srName,nodePath);
    }

    @GET
    @Path("/list")
    public List<Inode> list(
        @QueryParam("srName") String srName,
        @QueryParam("nodePath") String nodePath,
        @QueryParam("pageNumber") int pageNumber,
        @QueryParam("pageSize") int pageSize){
        return catalog.list(srName,nodePath,pageNumber,pageSize);
    }
}
