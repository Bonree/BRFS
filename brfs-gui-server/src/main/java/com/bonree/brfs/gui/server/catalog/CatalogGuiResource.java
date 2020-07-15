package com.bonree.brfs.gui.server.catalog;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

import com.bonree.brfs.client.BRFS;
import com.bonree.brfs.client.BRFSClientBuilder;
import com.bonree.brfs.client.BRFSObject;
import com.bonree.brfs.client.BRFSPath;
import com.bonree.brfs.client.ClientConfigurationBuilder;
import com.bonree.brfs.client.FidException;
import com.bonree.brfs.client.GetObjectRequest;
import com.bonree.brfs.client.discovery.Discovery;
import com.bonree.brfs.client.discovery.ServerNode;
import com.bonree.brfs.client.utils.HttpStatus;
import com.bonree.brfs.client.utils.SocketChannelSocketFactory;
import com.bonree.brfs.gui.server.BrfsConfig;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.List;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.ResponseBody;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/catalog")
public class CatalogGuiResource {
    private static final Logger LOG = LoggerFactory.getLogger(CatalogGuiResource.class);

    private Discovery httpDiscovery;
    private BrfsConfig brfsConfig;

    @Inject
    public CatalogGuiResource(Discovery httpDiscovery, BrfsConfig brfsConfig) {
        this.httpDiscovery = httpDiscovery;
        this.brfsConfig = brfsConfig;
    }

    @GET
    @Path("list")
    @Produces(APPLICATION_JSON)
    public String list(
        @QueryParam("srName") String srName,
        @QueryParam("filePath") String nodePath,
        @QueryParam("pageNumber") @DefaultValue("1") int pageNumber,
        @QueryParam("pageSize") @DefaultValue("100") int pageSize) {
        if (nodePath == null || "".equals(nodePath)) {
            throw new BadRequestException();
        }
        OkHttpClient httpClient = new OkHttpClient.Builder()
            .socketFactory(new SocketChannelSocketFactory())
            .build();
        List<ServerNode> serviceList = httpDiscovery.getServiceList(Discovery.ServiceType.REGION);
        for (ServerNode serverNode : serviceList) {
            Request httpRequest = new Request.Builder()
                .url(HttpUrl.get(URI.create("http://" + serverNode.getHost() + ":" + serverNode.getPort()))
                            .newBuilder()
                            .encodedPath("/catalog/list")
                            .addQueryParameter("srName", srName)
                            .addQueryParameter("nodePath", nodePath)
                            .addQueryParameter("pageNumber", String.valueOf(pageNumber))
                            .addQueryParameter("pageSize", String.valueOf(pageSize))
                            .build())
                .get()
                .build();
            try {
                okhttp3.Response response = httpClient.newCall(httpRequest).execute();
                if (response.code() == HttpStatus.CODE_OK) {
                    ResponseBody responseBody = response.body();
                    if (responseBody == null) {
                        continue;
                    }
                    return responseBody.string();
                } else if (response.code() == HttpStatus.CODE_NOT_FOUND) {
                    throw new NotFoundException();
                }
            } catch (IOException e) {
                throw new WebApplicationException(777);
            }

        }
        throw new NotFoundException();
    }

    @GET
    @Path("isFile")
    @Produces(APPLICATION_JSON)
    public Response isFile(
        @QueryParam("srName") String srName,
        @QueryParam("filePath") String filePath) {
        OkHttpClient httpClient = new OkHttpClient.Builder()
            .socketFactory(new SocketChannelSocketFactory())
            .build();
        List<ServerNode> serviceList = httpDiscovery.getServiceList(Discovery.ServiceType.REGION);
        for (ServerNode serverNode : serviceList) {
            Request httpRequest = new Request.Builder()
                .url(HttpUrl.get(URI.create("http://" + serverNode.getHost() + ":" + serverNode.getPort()))
                            .newBuilder()
                            .encodedPath("/catalog/isFile")
                            .addQueryParameter("srName", srName)
                            .addQueryParameter("nodePath", filePath)
                            .build())
                .get()
                .build();
            try {
                okhttp3.Response response = httpClient.newCall(httpRequest).execute();
                if (response.code() == HttpStatus.CODE_OK) {
                    ResponseBody responseBody = response.body();
                    if (responseBody == null) {
                        continue;
                    }
                    return Response.ok().entity(responseBody.string()).build();
                }
            } catch (IOException e) {
                throw new WebApplicationException(777);
            }

        }
        throw new NotFoundException();
    }

    @GET
    @Path("get")
    public Response get(@QueryParam("srName") String srName,
                        @QueryParam("filePath") String filePath,
                        @Context HttpServletResponse response,
                        @Context ServletContext ctx) {
        if ("".equals(filePath) || filePath == null) {
            throw new BadRequestException();
        }
        return Response.ok(new StreamingOutput() {
            @Override
            public void write(OutputStream output) throws IOException, WebApplicationException {
                String[] a = new String[] {};
                List<String> regionAddress = brfsConfig.getRegionAddress();
                URI[] seeds = new URI[regionAddress.size()];
                for (int i = 0; i < regionAddress.size(); i++) {
                    seeds[i] = URI.create(regionAddress.get(i));
                }
                BRFS client = new BRFSClientBuilder()
                    .config(new ClientConfigurationBuilder()
                                .setDataPackageSize(4 * 1024 * 1024)
                                .build())
                    .build(brfsConfig.getUsername(), brfsConfig.getPassword(), seeds);
                BRFSObject object = null;
                try {
                    object = client.getObject(GetObjectRequest.of(srName, BRFSPath.get(filePath)));
                    byte[] tmp = new byte[1024];
                    InputStream objectContent = object.getObjectContent();
                    int len;
                    while ((len = objectContent.read(tmp)) != -1) {
                        output.write(tmp, 0, len);
                        len = 0;
                    }
                } catch (FidException fidException) {
                    LOG.warn("data is expired of [{}]!", filePath);
                    throw new WebApplicationException(506);
                } catch (Exception e) {
                    LOG.warn("error when get the file[{}]", filePath);
                    throw new NotFoundException();
                }
            }
        }).header("Content-disposition", "attachment;filename=" + getLastNodeName(filePath))
                       .header("Cache-Control", "no-cache").build();
    }

    @GET
    @Path("sr")
    @Produces(APPLICATION_JSON)
    public List<String> listSR() {
        List<String> regionAddress = brfsConfig.getRegionAddress();
        URI[] seeds = new URI[regionAddress.size()];
        for (int i = 0; i < regionAddress.size(); i++) {
            seeds[i] = URI.create(regionAddress.get(i));
        }
        BRFS client = new BRFSClientBuilder()
            .config(new ClientConfigurationBuilder()
                        .setDataPackageSize(4 * 1024 * 1024)
                        .build())
            .build(brfsConfig.getUsername(), brfsConfig.getPassword(), seeds);
        List<String> result;
        try {
            result = client.listStorageRegions();
        } catch (Exception e) {
            result = ImmutableList.of();
            LOG.warn("can not get srs");
        }
        return result;
    }

    private String getLastNodeName(String path) {
        return path.substring(path.lastIndexOf("/") + 1);
    }
}
