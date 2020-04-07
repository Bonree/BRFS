package com.bonree.brfs.rocksdb.guice;

import com.bonree.brfs.common.utils.BrStringUtils;
import com.bonree.brfs.common.utils.FileUtils;
import com.bonree.brfs.common.utils.StringUtils;
import com.bonree.brfs.common.utils.ZipUtils;
import com.bonree.brfs.rocksdb.RocksDBDataUnit;
import com.bonree.brfs.rocksdb.RocksDBManager;
import com.bonree.brfs.rocksdb.WriteStatus;
import com.bonree.brfs.rocksdb.backup.RocksDBBackupEngine;
import com.bonree.brfs.rocksdb.file.SimpleFileSender;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.Response;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

/*******************************************************************************
 * 版权信息：北京博睿宏远数据科技股份有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @Date 2020/4/3 16:05
 * @Author: <a href=mailto:zhangqi@bonree.com>张奇</a>
 * @Description:
 ******************************************************************************/
@Path("/rocksdb")
public class RocksDBResource {

    private static final Logger LOG = LoggerFactory.getLogger(RocksDBResource.class);

    private RocksDBManager rocksDBManager;
    private RocksDBBackupEngine backupEngine;
    private RocksDBConfig rocksDBConfig;

    @Inject
    public RocksDBResource(RocksDBConfig rocksDBConfig, RocksDBManager rocksDBManager, RocksDBBackupEngine backupEngine) {
        this.rocksDBConfig = rocksDBConfig;
        this.rocksDBManager = rocksDBManager;
        this.backupEngine = backupEngine;
    }

    @GET
    @Path("ping")
    @Produces(APPLICATION_JSON)
    public Response ping() {
        return Response.ok().build();
    }

    @GET
    @Path("read/{srName}")
    @Produces(APPLICATION_JSON)
    public Response read(
            @PathParam("srName") String srName,
            @QueryParam("fileName") String fileName) {

        byte[] fid = this.rocksDBManager.read(srName, fileName.getBytes(StandardCharsets.UTF_8));
        if (fid == null) {
            return Response.status(Response.Status.NOT_FOUND).build();
        }

        return Response.ok().entity(fid).build();
    }

    @GET
    @Path("gui/{srName}")
    @Produces(APPLICATION_JSON)
    public Response gui(
            @PathParam("srName") String srName,
            @QueryParam("prefix") String prefix) {
        List<byte[]> fids = this.rocksDBManager.readByPrefix(srName, prefix.getBytes(StandardCharsets.UTF_8));

        if (fids == null || fids.isEmpty()) {
            return Response.status(Response.Status.NOT_FOUND).build();
        }

        return Response.ok().entity(fids).build();
    }

    @GET
    @Path("inner/read")
    @Produces(APPLICATION_JSON)
    public Response readInner(
            @QueryParam("srName") String srName,
            @QueryParam("fileName") String fileName) {
        byte[] fid = this.rocksDBManager.read(srName, fileName.getBytes(StandardCharsets.UTF_8), false);
        if (fid == null) {
            return Response.status(Response.Status.NOT_FOUND).build();
        }

        return Response.ok().entity(fid).build();
    }


    @POST
    @Path("inner/write")
    @Produces(APPLICATION_JSON)
    public Response writeInner(RocksDBDataUnit data) {
        try {
            WriteStatus status = this.rocksDBManager.write(data);
            return Response.ok().entity(BrStringUtils.toUtf8Bytes(status.name())).build();
        } catch (Exception e) {
            LOG.error(StringUtils.format("write data failed, data:{}", data), e);
            return Response.serverError().entity(Throwables.getStackTraceAsString(e)).build();
        }
    }


    @POST
    @Path("inner/restore")
    @Produces(APPLICATION_JSON)
    public Response restore(
            @QueryParam("transferFileName") String transferFileName,
            @QueryParam("restorePath") String restorePath,
            @QueryParam("host") String host,
            @QueryParam("port") int port) {

        String backupPath = this.rocksDBConfig.getRocksDBBackupPath();

        try {
            int backupId = this.backupEngine.createNewBackup();
            List<Integer> backupIds = this.backupEngine.getBackupIds();
            LOG.info("restore handler create new backup, this backupId:{}, all backupIds:{}", backupId, backupIds);

            String outDir = backupPath + File.separator + transferFileName;
            ZipUtils.zip(FileUtils.listFilePaths(backupPath), outDir);
            SimpleFileSender sender = new SimpleFileSender();
            sender.send(host, port, outDir, restorePath);

            if (FileUtils.deleteFile(outDir)) {
                LOG.info("socket client delete tmp transfer file :{}", outDir);
            }

            return Response.ok().entity(backupIds).build();
        } catch (Exception e) {
            LOG.error("restore request handler err, host:port {}:{}", host, port);
            return Response.serverError().build();
        }
    }

}
