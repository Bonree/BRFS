package com.bonree.brfs.rocksdb.guice;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.MediaType.APPLICATION_OCTET_STREAM;

import com.bonree.brfs.common.rocksdb.RocksDBManager;
import com.bonree.brfs.common.rocksdb.WriteStatus;
import com.bonree.brfs.common.supervisor.TimeWatcher;
import com.bonree.brfs.common.utils.JsonUtils;
import com.bonree.brfs.common.utils.PooledThreadFactory;
import com.bonree.brfs.common.utils.StringUtils;
import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.RocksDBConfigs;
import com.bonree.brfs.rocksdb.impl.RocksDBDataUnit;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Throwables;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private RocksDBConfig rocksDBConfig;
    private int syncerNum = Configs.getConfiguration().getConfig(RocksDBConfigs.ROCKSDB_SYNCER_NUM);
    private TimeWatcher watcher = new TimeWatcher();
    private ExecutorService workers = new ThreadPoolExecutor(
        syncerNum,
        syncerNum,
        0L,
        TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<>(10000),
        new PooledThreadFactory("rocksdb_handler"),
        new ThreadPoolExecutor.AbortPolicy()
    );

    @Inject
    public RocksDBResource(RocksDBConfig rocksDBConfig,
                           RocksDBManager rocksDBManager) {
        this.rocksDBConfig = rocksDBConfig;
        this.rocksDBManager = rocksDBManager;
        System.out.println("---------------------" + syncerNum);
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
        Map<byte[], byte[]> result = this.rocksDBManager.readByPrefix(srName, prefix.getBytes(StandardCharsets.UTF_8));

        if (result == null || result.isEmpty()) {
            return Response.status(Response.Status.NOT_FOUND).build();
        }

        return Response.ok().entity(result).build();
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
    public Response writeInner(
        @QueryParam("cf") String columnFamily,
        @QueryParam("key") String key,
        @QueryParam("value") String value) {
        try {
            workers.submit(() -> {
                try {
                    watcher.getElapsedTimeAndRefresh();
                    WriteStatus status = this.rocksDBManager.syncData(columnFamily, key.getBytes(), value.getBytes());
                    LOG.info("receive sync data request, cf:{}, key:{}, value:{}, write cost time:{}", columnFamily, key, value,
                             watcher.getElapsedTime());
                } catch (Exception e) {
                    LOG.error(StringUtils.format("write data failed, cf:{}, key:{}, value:{}", columnFamily, key, value), e);
                }
            });
        } catch (RejectedExecutionException e) {
            LOG.warn("sync queue now is exceed cannot accept new data");
            return Response.serverError().entity(Throwables.getStackTraceAsString(e)).build();
        }
        return Response.ok().build();
    }

    @POST
    @Path("inner/batch_write")
    @Consumes(APPLICATION_OCTET_STREAM)
    @Produces(APPLICATION_JSON)
    public Response batchWriteInner(byte[] body) {
        try {
            workers.submit(() -> {
                try {
                    //批量同步fid到rocksdb
                    List<RocksDBDataUnit> dataList = JsonUtils.toObject(body, new TypeReference<List<RocksDBDataUnit>>() {
                    });
                    watcher.getElapsedTimeAndRefresh();
                    for (RocksDBDataUnit unit : dataList) {
                        rocksDBManager.syncData(unit.getColumnFamily(), unit.getKey(), unit.getValue());
                    }
                    LOG.debug("receive sync data request, size:{}, write cost time:{}",
                              dataList.size(), watcher.getElapsedTime());
                } catch (Exception e) {
                    LOG.error("batch write data failed", e);
                }
            });
        } catch (RejectedExecutionException e) {
            LOG.warn("sync queue now is exceed cannot accept new data");
            return Response.serverError().entity(Throwables.getStackTraceAsString(e)).build();
        }
        return Response.ok().build();
    }
}
