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
import java.util.concurrent.ExecutorService;
import javax.inject.Inject;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.ProcessingException;
import javax.ws.rs.QueryParam;
import javax.ws.rs.ServiceUnavailableException;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/catalog")
public class CatalogResource {
    private static final Logger LOG = LoggerFactory.getLogger(CatalogResource.class);
    private final BrfsCatalog catalog;

    private final ExecutorService readExec;

    @Inject
    public CatalogResource(BrfsCatalog catalog,
                           @RocksDBRead ExecutorService readExec) {
        this.catalog = catalog;
        this.readExec = readExec;
    }

    @GET
    @Path("fid/{srName}")
    public void getFid(
        @PathParam("srName") String srName,
        @QueryParam("absPath") String absPath,
        @Suspended AsyncResponse asyncResponse) {
        LOG.debug("get fid request srName[{}],absPath[{}]", srName, absPath);

        if (!catalog.isUsable()) {
            LOG.error("get fid error caused by the catalog is not open");
            throw new ServiceUnavailableException("get fid error caused by the catalog is not open");
        }

        readExec.submit(() -> {
            String fid = null;
            try {
                fid = catalog.getFid(srName, absPath);
            } catch (NotFoundException nfe) {
                LOG.info("not found the path [{}], maybe it is expired.", absPath);
                asyncResponse.resume(nfe);
                return;
            } catch (Exception e) {
                LOG.error("error when get fid from rocksDb!");
                asyncResponse.resume(new ProcessingException("error when get fid from rocksDb"));
                return;
            }

            if (fid == null) {
                LOG.error("get null from rocksDB");
                asyncResponse.resume(new ServiceUnavailableException("get null when get fid from catalog!"));
                return;
            }

            asyncResponse.resume(fid);
        });
    }

    @GET
    @Path("/isFile")
    public boolean isFile(
        @QueryParam("srName") String srName,
        @QueryParam("nodePath") String nodePath) {
        return catalog.isFileNode(srName, nodePath);
    }

    @GET
    @Path("/list")
    public List<Inode> list(
        @QueryParam("srName") String srName,
        @QueryParam("nodePath") String nodePath,
        @QueryParam("pageNumber") @DefaultValue("1") int pageNumber,
        @QueryParam("pageSize") @DefaultValue("100") int pageSize) {
        return catalog.list(srName, nodePath, pageNumber, pageSize);
    }

    @GET
    @Path("/getFidsByDir")
    public List<String> getFidsByDir(
        @QueryParam("srName") String srName,
        @QueryParam("dir") String dir) {
        return catalog.getFidsByDir(srName, dir);
    }
}
