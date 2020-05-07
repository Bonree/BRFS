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

package com.bonree.brfs.client;

import static com.bonree.brfs.client.utils.Strings.format;
import static java.util.Objects.requireNonNull;

import com.bonree.brfs.client.data.DataSplitter;
import com.bonree.brfs.client.data.FSPackageProtoMaker;
import com.bonree.brfs.client.data.FixedSizeDataSplitter;
import com.bonree.brfs.client.data.NextData;
import com.bonree.brfs.client.data.PutObjectCallMaker;
import com.bonree.brfs.client.data.compress.Compression;
import com.bonree.brfs.client.data.read.FidContentReader;
import com.bonree.brfs.client.data.read.FilePathMapper;
import com.bonree.brfs.client.data.read.SubFidParser;
import com.bonree.brfs.client.discovery.Discovery.ServiceType;
import com.bonree.brfs.client.discovery.NodeSelector;
import com.bonree.brfs.client.json.JsonCodec;
import com.bonree.brfs.client.route.Router;
import com.bonree.brfs.client.storageregion.CreateStorageRegionRequest;
import com.bonree.brfs.client.storageregion.ListStorageRegionRequest;
import com.bonree.brfs.client.storageregion.StorageRegionID;
import com.bonree.brfs.client.storageregion.StorageRegionInfo;
import com.bonree.brfs.client.storageregion.UpdateStorageRegionRequest;
import com.bonree.brfs.client.utils.HttpStatus;
import com.bonree.brfs.client.utils.IteratorUtils;
import com.bonree.brfs.client.utils.LazeAggregateInputStream;
import com.bonree.brfs.client.utils.Range;
import com.bonree.brfs.client.utils.Retrys;
import com.bonree.brfs.client.utils.Strings;
import com.bonree.brfs.client.utils.URIRetryable;
import com.bonree.brfs.client.utils.URIRetryable.TaskResult;
import com.bonree.brfs.common.proto.FileDataProtos.Fid;
import com.bonree.brfs.common.write.data.FidDecoder;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Stopwatch;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.io.Closeables;
import com.google.common.io.Closer;
import com.google.common.io.Files;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BRFSClient implements BRFS {
    private static final Logger log = LoggerFactory.getLogger(BRFSClient.class);

    public static final MediaType JSON = MediaType.get("application/json; charset=utf-8");
    public static final MediaType OCTET_STREAM = MediaType.get("application/octet-stream");

    private static final Duration DEFAULT_EXPIRE_DURATION = Duration.ofMinutes(30);
    private static final Duration DEFAULT_REFRESH_DURATION = Duration.ofMinutes(10);

    private final OkHttpClient httpClient;
    private final JsonCodec codec;
    private final NodeSelector nodeSelector;

    private final Router router;
    private final FidContentReader fidReader;
    private final FilePathMapper pathMapper;
    private final SubFidParser subFidParser;

    private final DataSplitter dataSplitter;

    private final Closer closer;

    private final LoadingCache<String, Integer> storageRegionCache;

    public BRFSClient(
        ClientConfiguration config,
        OkHttpClient httpClient,
        NodeSelector nodeSelector,
        Router router,
        FidContentReader fidReader,
        FilePathMapper pathMapper,
        SubFidParser subFidParser,
        JsonCodec codec,
        Closer closer) {
        this.closer = closer;
        this.httpClient = requireNonNull(httpClient, "http client is null");
        this.nodeSelector = requireNonNull(nodeSelector, "nodeSelector is null");
        this.codec = requireNonNull(codec, "codec is null");
        this.dataSplitter = new FixedSizeDataSplitter(config.getDataPackageSize());

        this.storageRegionCache = CacheBuilder.newBuilder()
                                              .expireAfterWrite(Optional.ofNullable(config.getDiscoveryExpiredDuration())
                                                                        .orElse(DEFAULT_EXPIRE_DURATION))
                                              .refreshAfterWrite(
                                                  Optional.ofNullable(config.getStorageRegionCacheRefreshDuration())
                                                          .orElse(DEFAULT_REFRESH_DURATION))
                                              .build(new CacheLoader<String, Integer>() {

                                                  @Override
                                                  public Integer load(String srName) throws Exception {
                                                      StorageRegionID id = getStorageRegionIDFromRemote(srName);
                                                      return id.getId();
                                                  }

                                              });

        this.router = requireNonNull(router, "router is null");
        this.fidReader = requireNonNull(fidReader, "fidReader is null");
        this.pathMapper = requireNonNull(pathMapper, "pathMapper is null");
        this.subFidParser = requireNonNull(subFidParser, "subFidParser is null");
    }

    @Override
    public StorageRegionID createStorageRegion(CreateStorageRegionRequest request) throws Exception {
        RequestBody body = RequestBody.create(JSON, codec.toJson(request.getAttributes()));

        return Retrys.execute(new URIRetryable<>(
            format("create storage region[%s]", request.getStorageRegionName()),
            nodeSelector.getNodeHttpLocations(ServiceType.REGION),
            uri -> {
                Request httpRequest = new Request.Builder()
                    .url(HttpUrl.get(uri)
                                .newBuilder()
                                .encodedPath("/sr")
                                .addEncodedPathSegment(request.getStorageRegionName())
                                .build())
                    .put(body)
                    .build();

                try (Response response = httpClient.newCall(httpRequest).execute()) {
                    if (response.code() == HttpStatus.CODE_CONFLICT) {
                        return TaskResult.fail(new BRFSException("Storage Region[%s] is existed",
                                                                 request.getStorageRegionName()));
                    }

                    if (response.code() == HttpStatus.CODE_OK) {
                        ResponseBody responseBody = response.body();
                        if (responseBody == null) {
                            return TaskResult.fail(new IllegalStateException("No response content is found"));
                        }

                        return TaskResult.success(codec.fromJsonBytes(responseBody.bytes(), StorageRegionID.class));
                    }

                    return TaskResult.fail(new IllegalStateException(format("Server error[%d]", response.code())));
                } catch (IOException e) {
                    return TaskResult.retry(e);
                }
            }));
    }

    private int getStorageRegionID(String srName) throws BRFSException {
        try {
            return storageRegionCache.get(srName);
        } catch (ExecutionException e) {
            throw new BRFSException(e, "Can not get id of storage region[%s]", srName);
        }
    }

    private StorageRegionID getStorageRegionIDFromRemote(String srName) {
        return Retrys.execute(new URIRetryable<>(
            format("get the id of storage region[%s]", srName),
            nodeSelector.getNodeHttpLocations(ServiceType.REGION),
            uri -> {
                Request httpRequest = new Request.Builder()
                    .url(HttpUrl.get(uri)
                                .newBuilder()
                                .encodedPath("/sr/id")
                                .addEncodedPathSegment(srName)
                                .build())
                    .get()
                    .build();

                try (Response response = httpClient.newCall(httpRequest).execute()) {
                    if (response.code() == HttpStatus.CODE_NOT_FOUND) {
                        return TaskResult.fail(new BRFSException("Storage Region[%s] is not existed", srName));
                    }

                    if (response.code() == HttpStatus.CODE_OK) {
                        ResponseBody responseBody = response.body();
                        if (responseBody == null) {
                            return TaskResult.fail(new IllegalStateException("No response content is found"));
                        }

                        return TaskResult.success(codec.fromJsonBytes(responseBody.bytes(), StorageRegionID.class));
                    }

                    return TaskResult.fail(new IllegalStateException(format("Server error[%d]", response.code())));
                } catch (IOException e) {
                    return TaskResult.retry(e);
                }
            }));
    }

    @Override
    public boolean doesStorageRegionExists(String srName) {
        return Retrys.execute(new URIRetryable<>(
            format("check the existance of storage region[%s]", srName),
            nodeSelector.getNodeHttpLocations(ServiceType.REGION),
            uri -> {
                Request httpRequest = new Request.Builder()
                    .url(HttpUrl.get(uri)
                                .newBuilder()
                                .encodedPath("/sr")
                                .addEncodedPathSegment(srName)
                                .build())
                    .head()
                    .build();

                try {
                    Response response = httpClient.newCall(httpRequest).execute();
                    if (response.code() == HttpStatus.CODE_NOT_FOUND) {
                        return TaskResult.success(false);
                    }

                    if (response.code() == HttpStatus.CODE_OK) {
                        return TaskResult.success(true);
                    }

                    return TaskResult.fail(new IllegalStateException(format("Server error[%d]", response.code())));
                } catch (IOException e) {
                    return TaskResult.retry(e);
                }
            }));
    }

    @Override
    public List<String> listStorageRegions() {
        return listStorageRegions(ListStorageRegionRequest.newBuilder().build());
    }

    @Override
    public List<String> listStorageRegions(ListStorageRegionRequest request) {
        return Retrys.execute(new URIRetryable<>(
            "list storage region names",
            nodeSelector.getNodeHttpLocations(ServiceType.REGION),
            uri -> {
                Request httpRequest = new Request.Builder()
                    .url(HttpUrl.get(uri)
                                .newBuilder()
                                .encodedPath("/sr")
                                .addEncodedPathSegment("list")
                                .addEncodedQueryParameter("disableAllowed", String.valueOf(request.disableAllowed()))
                                .addEncodedQueryParameter("prefix", request.getPrefix())
                                .addEncodedQueryParameter("maxKeys", String.valueOf(request.getMaxKeys()))
                                .build())
                    .get()
                    .build();

                try (Response response = httpClient.newCall(httpRequest).execute()) {
                    if (response.code() == HttpStatus.CODE_OK) {
                        ResponseBody responseBody = response.body();
                        if (responseBody == null) {
                            return TaskResult.fail(new IllegalStateException("No response content is found"));
                        }

                        return TaskResult.success(codec.fromJsonBytes(responseBody.bytes(), new TypeReference<List<String>>() {
                        }));
                    }

                    return TaskResult.fail(new IllegalStateException(format("Server error[%d]", response.code())));
                } catch (IOException e) {
                    return TaskResult.retry(e);
                }
            }));
    }

    @Override
    public boolean updateStorageRegion(String srName, UpdateStorageRegionRequest request) throws Exception {
        RequestBody body = RequestBody.create(JSON, codec.toJson(request.getAttributes()));

        return Retrys.execute(new URIRetryable<>(
            format("update storage region[%s]", srName),
            nodeSelector.getNodeHttpLocations(ServiceType.REGION),
            uri -> {
                Request httpRequest = new Request.Builder()
                    .url(HttpUrl.get(uri)
                                .newBuilder()
                                .encodedPath("/sr")
                                .addEncodedPathSegment(srName)
                                .build())
                    .post(body)
                    .build();

                try {
                    Response response = httpClient.newCall(httpRequest).execute();
                    if (response.code() == HttpStatus.CODE_NOT_FOUND) {
                        return TaskResult.fail(new BRFSException("Storage Region[%s] is not existed", srName));
                    }

                    if (response.code() == HttpStatus.CODE_OK) {
                        return TaskResult.success(true);
                    }

                    return TaskResult.fail(new IllegalStateException(format("Server error[%d]", response.code())));
                } catch (IOException e) {
                    return TaskResult.retry(e);
                }
            }));

    }

    @Override
    public StorageRegionInfo getStorageRegionInfo(String srName) {
        return Retrys.execute(new URIRetryable<>(
            format("get storage region[%s] info", srName),
            nodeSelector.getNodeHttpLocations(ServiceType.REGION),
            uri -> {
                Request httpRequest = new Request.Builder()
                    .url(HttpUrl.get(uri)
                                .newBuilder()
                                .encodedPath("/sr")
                                .addEncodedPathSegment(srName)
                                .build())
                    .get()
                    .build();

                try (Response response = httpClient.newCall(httpRequest).execute()) {
                    if (response.code() == HttpStatus.CODE_NOT_FOUND) {
                        return TaskResult.fail(new BRFSException("Storage Region[%s] is not existed", srName));
                    }

                    if (response.code() == HttpStatus.CODE_OK) {
                        ResponseBody responseBody = response.body();
                        if (responseBody == null) {
                            return TaskResult.fail(new IllegalStateException("No response content is found"));
                        }

                        return TaskResult.success(codec.fromJsonBytes(responseBody.bytes(), StorageRegionInfo.class));
                    }

                    return TaskResult.fail(new IllegalStateException(format("Server error[%d]", response.code())));
                } catch (IOException e) {
                    return TaskResult.retry(e);
                }
            }));
    }

    @Override
    public void deleteStorageRegion(String srName) {
        Retrys.execute(new URIRetryable<Void>(
            format("delete storage region[%s]", srName),
            nodeSelector.getNodeHttpLocations(ServiceType.REGION),
            uri -> {
                Request httpRequest = new Request.Builder()
                    .url(HttpUrl.get(uri)
                                .newBuilder()
                                .encodedPath("/sr")
                                .addEncodedPathSegment(srName)
                                .build())
                    .delete()
                    .build();

                try {
                    Response response = httpClient.newCall(httpRequest).execute();
                    if (response.code() == HttpStatus.CODE_NOT_FOUND) {
                        return TaskResult.fail(new BRFSException("Storage Region[%s] is not existed", srName));
                    }

                    if (response.code() == HttpStatus.CODE_FORBIDDEN) {
                        return TaskResult.fail(new BRFSException("Storage Region[%s] is not disabled", srName));
                    }

                    if (response.code() == HttpStatus.CODE_OK) {
                        return TaskResult.success(null);
                    }

                    return TaskResult.fail(new IllegalStateException(format("Server error[%d]", response.code())));
                } catch (IOException e) {
                    return TaskResult.retry(e);
                }
            }));
    }

    @Override
    public PutObjectResult putObject(String srName, byte[] bytes) throws Exception {
        return putObject(srName, dataSplitter.split(bytes), Optional.empty());
    }

    @Override
    public PutObjectResult putObject(String srName, File file) throws Exception {
        try (FileInputStream input = new FileInputStream(file)) {
            return putObject(srName, input);
        } catch (IOException e) {
            throw new ClientException(e, "Write file[%s] failed", file.getAbsolutePath());
        }
    }

    @Override
    public PutObjectResult putObject(String srName, InputStream input) throws Exception {
        return putObject(srName, dataSplitter.split(input), Optional.empty());
    }

    @Override
    public PutObjectResult putObject(String srName, BRFSPath objectPath, byte[] bytes) throws Exception {
        return putObject(srName, dataSplitter.split(bytes), Optional.of(objectPath));
    }

    @Override
    public PutObjectResult putObject(String srName, BRFSPath objectPath, File file) throws Exception {
        try (FileInputStream input = new FileInputStream(file)) {
            return putObject(srName, objectPath, input);
        } catch (IOException e) {
            throw new ClientException(e, "Write file[%s] failed", file.getAbsolutePath());
        }
    }

    @Override
    public PutObjectResult putObject(String srName, BRFSPath objectPath, InputStream input) throws Exception {
        return putObject(srName, dataSplitter.split(input), Optional.of(objectPath));
    }

    private PutObjectResult putObject(String srName, Iterator<ByteBuffer> buffers, Optional<BRFSPath> objectPath)
        throws Exception {
        AtomicInteger sequenceIDs = new AtomicInteger();
        Iterator<Function<URI, Call>> callProvider = IteratorUtils.from(buffers)
                                                                  .map(new FSPackageProtoMaker(
                                                                      () -> sequenceIDs.getAndIncrement(),
                                                                      getStorageRegionID(srName),
                                                                      UUID.randomUUID().toString(),
                                                                      objectPath.map(BRFSPath::getPath),
                                                                      false,
                                                                      Compression.NONE))
                                                                  .map(new PutObjectCallMaker(httpClient, OCTET_STREAM, srName))
                                                                  .iterator();

        if (!callProvider.hasNext()) {
            throw new IllegalArgumentException(Strings.format("No content to write for sr[%s]", srName));
        }

        Function<URI, Call> firstCall = callProvider.next();
        SettableFuture<PutObjectResult> resultFuture = SettableFuture.create();

        return Retrys.execute(new URIRetryable<>(
            format("put object to storage region[%s]", srName),
            nodeSelector.getNodeHttpLocations(ServiceType.REGION),
            uri -> {
                SettableFuture<PutObjectCallback> retryFuture = SettableFuture.create();

                firstCall.apply(uri).enqueue(new Callback() {

                    @Override
                    public void onResponse(Call call, Response response) throws IOException {
                        PutObjectCallback callback = new PutObjectCallback(
                            uri,
                            () -> sequenceIDs.get(),
                            callProvider);

                        retryFuture.set(callback);

                        callback.onResponse(call, response);
                    }

                    @Override
                    public void onFailure(Call call, IOException cause) {
                        if (!retryFuture.isDone()) {
                            retryFuture.setException(cause);
                        }

                        resultFuture.setException(cause);
                    }
                });

                try {
                    return TaskResult.success(retryFuture.get());
                } catch (Exception e) {
                    return TaskResult.retry(e);
                }
            })).get();
    }

    @Override
    public BRFSObject getObject(GetObjectRequest request) throws Exception {
        Stopwatch stopwatch = Stopwatch.createStarted();
        String fid = null;
        if (request.getPath() != null) {
            fid = pathMapper.getFidByPath(request.getStorageRegionName(), request.getPath());
            if (fid == null) {
                throw new FileNotFoundException(request.getPath().toString());
            }
        }
        log.info("get fid of [{}] cost [{}]ms", request.getPath(), stopwatch.elapsed(TimeUnit.MILLISECONDS));
        stopwatch.stop();
        if (request.getFID() != null) {
            if (fid != null && !fid.equals(request.getFID())) {
                throw new IllegalArgumentException(
                    Strings.format("filePath[%s] has a diffrent fid[%s] from specified fid[%s]",
                                   request.getPath(),
                                   fid,
                                   request.getFID()));
            }

            fid = request.getFID();
        }

        if (fid == null) {
            throw new IllegalArgumentException("either fid or file path should be supplied");
        }
        return getObject(request.getStorageRegionName(), fid, request.getRange());
    }

    private BRFSObject getObject(String srName, String fid, Range range) throws Exception {
        Stopwatch started = Stopwatch.createStarted();
        if (range != null) {
            throw new ClientException("range is not supported now.");
        }

        Fid fidObj;
        try {
            fidObj = FidDecoder.build(fid);
        } catch (Exception e) {
            throw new IllegalArgumentException(Strings.format("Invalid FID: %s", fid));
        }

        if (fidObj.getStorageNameCode() != getStorageRegionID(srName)) {
            throw new IllegalStateException(
                Strings.format("fid[%s] is not belong to sr[%s]",
                               fid,
                               srName));
        }

        if (!fidObj.getIsBigFile()) {
            InputStream content = getActualContentofFile(srName, fidObj, range);
            log.info("get content of a small file which fid is :[] cost [{}]", fid, started.elapsed(TimeUnit.MILLISECONDS));
            return BRFSObject.from(content);
        }

        InputStream content = getActualContentofFile(srName, fidObj, null);
        List<Fid> subFids = subFidParser.readFids(content);
        ImmutableList.Builder<Supplier<InputStream>> streamBuilder = ImmutableList.builderWithExpectedSize(subFids.size());
        long accumulatedOffset = 0;
        for (Fid subFid : subFids) {
            if (range == null) {
                streamBuilder.add(() -> getActualContentofFile(srName, subFid, null));
                continue;
            }

            try {
                if (accumulatedOffset + subFid.getSize() <= range.getOffset()) {
                    continue;
                }

                long actualOffset = Math.max(accumulatedOffset, range.getOffset());
                long actualSize = Math.min(subFid.getSize(), range.getEndOffset()) - actualOffset;

                streamBuilder.add(() -> getActualContentofFile(srName, subFid, new Range(actualOffset, actualSize)));
            } finally {
                accumulatedOffset += subFid.getSize();
                if (accumulatedOffset >= range.getEndOffset()) {
                    break;
                }
            }
        }

        log.info("get content of a big file which fid is :[] cost [{}]", fid, started.elapsed(TimeUnit.MILLISECONDS));
        started.stop();
        return BRFSObject.from(new LazeAggregateInputStream(streamBuilder.build().iterator()));
    }

    private InputStream getActualContentofFile(String srName, Fid fidObj, Range range) {
        long offset = range == null ? fidObj.getOffset() : fidObj.getOffset() + range.getOffset();
        long size = range == null ? fidObj.getSize() : Math.min(fidObj.getSize(), range.getSize());

        Map<URI, Integer> idIndex = new HashMap<>();
        return Retrys.execute(new URIRetryable<>(
            format("read content of fid[%s]", fidObj),
            router.getServerLocation(srName, fidObj.getUuid(), fidObj.getServerIdList(), idIndex),
            uri -> {
                try {
                    return TaskResult.success(fidReader.read(uri, srName, fidObj, offset, size, idIndex.get(uri)));
                } catch (IOException e) {
                    return TaskResult.retry(e);
                } catch (Exception cause) {
                    return TaskResult.fail(cause);
                }
            }));
    }

    @Override
    public void getObject(GetObjectRequest request, File outputFile) throws Exception {
        getObject(request, outputFile, r -> r.run()).get();
    }

    @Override
    public ListenableFuture<?> getObject(GetObjectRequest request, File outputFile, Executor executor) {
        SettableFuture<?> future = SettableFuture.create();
        executor.execute(() -> {
            Stopwatch started = Stopwatch.createStarted();
            try (InputStream stream = getObject(request).getObjectContent()) {
                if (stream == null) {
                    future.setException(new ClientException("No content is responsed"));
                    return;
                }

                Files.asByteSink(outputFile).writeFrom(stream);
                future.set(null);
            } catch (Exception e) {
                future.setException(e);
            }
            log.info("get the future of [{}] cost [{}]ms", request.getPath(), started.elapsed(TimeUnit.MILLISECONDS));
        });

        return future;
    }

    @Override
    public boolean doesObjectExists(String srName, BRFSPath path) {
        return pathMapper.getFidByPath(srName, path) != null;
    }

    @Override
    public void deleteObjects(String srName, long startTime, long endTime) {
        Retrys.execute(new URIRetryable<Void>(
            format("delete data of storage region[%s]", srName),
            nodeSelector.getNodeHttpLocations(ServiceType.REGION),
            uri -> {
                Request httpRequest = new Request.Builder()
                    .url(HttpUrl.get(uri)
                                .newBuilder()
                                .encodedPath("/data")
                                .addQueryParameter("startTime", formatTime(startTime))
                                .addQueryParameter("endTime", formatTime(endTime))
                                .build())
                    .delete()
                    .build();

                try {
                    Response response = httpClient.newCall(httpRequest).execute();

                    if (response.code() == HttpStatus.CODE_OK) {
                        return TaskResult.success(null);
                    }

                    return TaskResult.fail(new IllegalStateException(format("Server error[%d]", response.code())));
                } catch (IOException e) {
                    return TaskResult.retry(e);
                }
            }));
    }

    private static String formatTime(long time) {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        return format.format(new Date(time));
    }

    private class PutObjectCallback implements Callback {
        private final URI uri;
        private final LongSupplier seqences;
        private final Iterator<Function<URI, Call>> callIterator;

        private final SettableFuture<PutObjectResult> resultFuture = SettableFuture.create();

        public PutObjectCallback(URI uri, LongSupplier seqences, Iterator<Function<URI, Call>> callIterator) {
            this.uri = uri;
            this.seqences = seqences;
            this.callIterator = callIterator;
        }

        public PutObjectResult get() throws InterruptedException, ExecutionException {
            return resultFuture.get();
        }

        @Override
        public void onResponse(Call call, Response response) throws IOException {
            try {
                if (response.code() == HttpStatus.CODE_NEXT) {
                    ResponseBody body = response.body();
                    if (body == null) {
                        resultFuture.setException(
                            new IllegalStateException("No response content is found in writting"));
                        return;
                    }

                    NextData nextData = codec.fromJsonBytes(body.bytes(), NextData.class);
                    if (nextData.getNextSequence() != seqences.getAsLong()) {
                        resultFuture.setException(
                            new IllegalStateException(
                                Strings.format("Expected next seq[%d] but get [%d]",
                                               seqences.getAsLong(),
                                               nextData.getNextSequence())));
                        return;
                    }

                    if (!callIterator.hasNext()) {
                        resultFuture.setException(
                            new IllegalStateException(
                                Strings.format("Expected fid is returned, but get NEXT code, current seq[%d]",
                                               seqences.getAsLong() - 1)));
                    }

                    log.info("writer block[%d] to url[%s] successfully", seqences.getAsLong() - 1, call.request().url());
                    callIterator.next().apply(uri).enqueue(this);
                    return;
                }

                if (response.code() == HttpStatus.CODE_OK) {
                    ResponseBody body = response.body();
                    if (body == null) {
                        resultFuture.setException(new IllegalStateException("No response content is found"));
                        return;
                    }

                    resultFuture.set(PutObjectResult.of(
                        Iterables.getOnlyElement(
                            codec.fromJsonBytes(
                                body.bytes(),
                                new TypeReference<List<String>>() {
                                }))));

                    return;
                }

                if (response.code() == HttpStatus.CODE_NOT_ALLOW_CUSTOM_FILENAME) {
                    resultFuture.setException(new IllegalArgumentException(
                        Strings.format("the catalog is not opened, cannot use the custom file name!"))
                    );
                    return;
                }

                if (response.code() == HttpStatus.CODE_NOT_AVAILABLE_FILENAME) {
                    resultFuture.setException(new IllegalArgumentException(
                        Strings.format("the custom file name is not pattern the regex [^(/+(\\.*[\\w,\\-]+\\.*)+)+$]!"))
                    );
                    return;
                }

                resultFuture.setException(new IllegalStateException(
                    Strings.format("Unexpected response code is returned[%d]", response.code())));
            } finally {
                Closeables.close(response, true);
            }
        }

        @Override
        public void onFailure(Call call, IOException cause) {
            resultFuture.setException(cause);
        }

    }

    @Override
    public void shutdown() {
        closeQuietly(closer);
    }

    private void closeQuietly(Closeable closeable) {
        try {
            closeable.close();
        } catch (IOException e) {
            log.warn(Strings.format("close [%s] failed", closeable), e);
        }
    }
}
