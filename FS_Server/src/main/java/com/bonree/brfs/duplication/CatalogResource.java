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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

@Path("/catalog")
public class CatalogResource {
    private static final Logger LOG = LoggerFactory.getLogger(CatalogResource.class);
    private final BrfsCatalog catalog;
    @Inject
    public CatalogResource(
            BrfsCatalog catalog) {
        this.catalog = catalog;

    }
    @GET
    @Path("fid/{srName}")
    public String getSercondServerID(
            @PathParam("srName") String srName,
            @QueryParam("absPath") String absPath) throws  Exception{
        LOG.info("get fid request srName[{}],absPath[{}]",srName,absPath);
        //todo 参数检查
        if(!catalog.isUsable()){
            throw new Exception("get fid error caused by the catalog is not open");
        }
        String fid = catalog.getFid(srName, absPath);
        if(fid == null) {
            throw new Exception("error when get fid from catalog!");
        }
        return fid;
    }
}
