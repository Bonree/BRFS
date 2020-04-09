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

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.LongSupplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonree.brfs.client.data.DataSplitter;
import com.bonree.brfs.client.data.FSPackageProtoMaker;
import com.bonree.brfs.client.data.FixedSizeDataSplitter;
import com.bonree.brfs.client.data.NextData;
import com.bonree.brfs.client.data.PutObjectCallMaker;
import com.bonree.brfs.client.data.compress.Compression;
import com.bonree.brfs.client.discovery.Discovery;
import com.bonree.brfs.client.discovery.Discovery.ServiceType;
import com.bonree.brfs.client.discovery.ServerNode;
import com.bonree.brfs.client.json.JsonCodec;
import com.bonree.brfs.client.ranker.Ranker;
import com.bonree.brfs.client.ranker.ShiftRanker;
import com.bonree.brfs.client.storageregion.CreateStorageRegionRequest;
import com.bonree.brfs.client.storageregion.ListStorageRegionRequest;
import com.bonree.brfs.client.storageregion.StorageRegionID;
import com.bonree.brfs.client.storageregion.StorageRegionInfo;
import com.bonree.brfs.client.storageregion.UpdateStorageRegionRequest;
import com.bonree.brfs.client.utils.HttpStatus;
import com.bonree.brfs.client.utils.IteratorUtils;
import com.bonree.brfs.client.utils.Retrys;
import com.bonree.brfs.client.utils.Strings;
import com.bonree.brfs.client.utils.URIRetryable;
import com.bonree.brfs.client.utils.URIRetryable.TaskResult;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;

public class BRFSClient implements BRFS {
    private static final Logger log = LoggerFactory.getLogger(BRFSClient.class);
    
    public static final MediaType JSON = MediaType.get("application/json; charset=utf-8");
    public static final MediaType OCTET_STREAM = MediaType.get("application/octet-stream");
    
    private static final Duration DEFAULT_EXPIRE_DURATION = Duration.ofMinutes(30);
    private static final Duration DEFAULT_REFRESH_DURATION = Duration.ofMinutes(10);
    
    private final OkHttpClient httpClient;
    private final Discovery discovery;
    private final JsonCodec codec;
    
    private final DataSplitter dataSplitter;
    
    private final LoadingCache<String, Integer> storageRegionCache;
    private final Ranker<ServerNode> nodeRanker;
    
    public BRFSClient(ClientConfiguration config, OkHttpClient httpClient, Discovery discovery, JsonCodec codec) {
        this.httpClient = requireNonNull(httpClient, "http client is null");
        this.discovery = requireNonNull(discovery, "discovery is null");
        this.codec = requireNonNull(codec, "codec is null");
        this.dataSplitter = new FixedSizeDataSplitter(config.getDataPackageSize());
        
        this.storageRegionCache = CacheBuilder.newBuilder()
                .expireAfterWrite(Optional.ofNullable(config.getDiscoveryExpiredDuration()).orElse(DEFAULT_EXPIRE_DURATION))
                .refreshAfterWrite(Optional.ofNullable(config.getStorageRegionCacheRefreshDuration()).orElse(DEFAULT_REFRESH_DURATION))
                .build(new CacheLoader<String, Integer>() {

                    @Override
                    public Integer load(String srName) throws Exception {
                        StorageRegionID id = getStorageRegionIDFromRemote(srName);
                        return id.getId();
                    }
                    
                });
        
        this.nodeRanker = new ShiftRanker<>();
    }

    public StorageRegionID createStorageRegion(CreateStorageRegionRequest request) throws Exception {
        RequestBody body = RequestBody.create(JSON, codec.toJson(request.getAttributes()));
        
        return Retrys.execute(new URIRetryable<StorageRegionID> (
                format("create storage region[%s]", request.getStorageRegionName()),
                getNodeHttpLocations(ServiceType.REGION),
                uri -> {
                    Request httpRequest = new Request.Builder()
                            .url(HttpUrl.get(uri)
                                    .newBuilder()
                                    .encodedPath("/sr")
                                    .addEncodedPathSegment(request.getStorageRegionName())
                                    .build())
                            .put(body)
                            .build();
                    
                    try {
                        Response response = httpClient.newCall(httpRequest).execute();
                        if(response.code() == HttpStatus.CODE_CONFLICT) {
                            return TaskResult.fail(new BRFSException("Storage Region[%s] is existed",
                                    request.getStorageRegionName()));
                        }
                        
                        if(response.code() == HttpStatus.CODE_OK) {
                            ResponseBody responseBody = response.body();
                            if(responseBody == null) {
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
        return Retrys.execute(new URIRetryable<StorageRegionID> (
                format("get the id of storage region[%s]", srName),
                getNodeHttpLocations(ServiceType.REGION),
                uri -> {
                    Request httpRequest = new Request.Builder()
                            .url(HttpUrl.get(uri)
                                    .newBuilder()
                                    .encodedPath("/sr/id")
                                    .addEncodedPathSegment(srName)
                                    .build())
                            .get()
                            .build();
                    
                    try {
                        Response response = httpClient.newCall(httpRequest).execute();
                        if(response.code() == HttpStatus.CODE_NOT_FOUND) {
                            return TaskResult.fail(new BRFSException("Storage Region[%s] is not existed", srName));
                        }
                        
                        if(response.code() == HttpStatus.CODE_OK) {
                            ResponseBody responseBody = response.body();
                            if(responseBody == null) {
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

    public boolean doesStorageRegionExists(String srName) {
        return Retrys.execute(new URIRetryable<Boolean> (
                format("check the existance of storage region[%s]", srName),
                getNodeHttpLocations(ServiceType.REGION),
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
                        if(response.code() == HttpStatus.CODE_NOT_FOUND) {
                            return TaskResult.success(false);
                        }
                        
                        if(response.code() == HttpStatus.CODE_OK) {
                            return TaskResult.success(true);
                        }
                        
                        return TaskResult.fail(new IllegalStateException(format("Server error[%d]", response.code())));
                    } catch (IOException e) {
                        return TaskResult.retry(e);
                    }
                }));
    }

    public List<String> listStorageRegions() {
        return listStorageRegions(ListStorageRegionRequest.newBuilder().build());
    }

    public List<String> listStorageRegions(ListStorageRegionRequest request) {
        return Retrys.execute(new URIRetryable<List<String>> (
                "list storage region names",
                getNodeHttpLocations(ServiceType.REGION),
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
                    
                    try {
                        Response response = httpClient.newCall(httpRequest).execute();
                        if(response.code() == HttpStatus.CODE_OK) {
                            ResponseBody responseBody = response.body();
                            if(responseBody == null) {
                                return TaskResult.fail(new IllegalStateException("No response content is found"));
                            }
                            
                            return TaskResult.success(codec.fromJsonBytes(responseBody.bytes(), new TypeReference<List<String>>() {}));
                        }
                        
                        return TaskResult.fail(new IllegalStateException(format("Server error[%d]", response.code())));
                    } catch (IOException e) {
                        return TaskResult.retry(e);
                    }
                }));
    }

    public boolean updateStorageRegion(String srName, UpdateStorageRegionRequest request) throws Exception {
        RequestBody body = RequestBody.create(JSON, codec.toJson(request.getAttributes()));
        
        return Retrys.execute(new URIRetryable<Boolean> (
                format("update storage region[%s]", srName),
                getNodeHttpLocations(ServiceType.REGION),
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
                        if(response.code() == HttpStatus.CODE_NOT_FOUND) {
                            return TaskResult.fail(new BRFSException("Storage Region[%s] is not existed", srName));
                        }
                        
                        if(response.code() == HttpStatus.CODE_OK) {
                            return TaskResult.success(true);
                        }
                        
                        return TaskResult.fail(new IllegalStateException(format("Server error[%d]", response.code())));
                    } catch (IOException e) {
                        return TaskResult.retry(e);
                    }
                }));

    }

    public StorageRegionInfo getStorageRegionInfo(String srName) {
        return Retrys.execute(new URIRetryable<StorageRegionInfo> (
                format("get storage region[%s] info", srName),
                getNodeHttpLocations(ServiceType.REGION),
                uri -> {
                    Request httpRequest = new Request.Builder()
                            .url(HttpUrl.get(uri)
                                    .newBuilder()
                                    .encodedPath("/sr")
                                    .addEncodedPathSegment(srName)
                                    .build())
                            .get()
                            .build();
                    
                    try {
                        Response response = httpClient.newCall(httpRequest).execute();
                        if(response.code() == HttpStatus.CODE_NOT_FOUND) {
                            return TaskResult.fail(new BRFSException("Storage Region[%s] is not existed", srName));
                        }
                        
                        if(response.code() == HttpStatus.CODE_OK) {
                            ResponseBody responseBody = response.body();
                            if(responseBody == null) {
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

    public void deleteStorageRegion(String srName) {
        Retrys.execute(new URIRetryable<Void> (
                format("delete storage region[%s]", srName),
                getNodeHttpLocations(ServiceType.REGION),
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
                        if(response.code() == HttpStatus.CODE_NOT_FOUND) {
                            return TaskResult.fail(new BRFSException("Storage Region[%s] is not existed", srName));
                        }
                        
                        if(response.code() == HttpStatus.CODE_FORBIDDEN) {
                            return TaskResult.fail(new BRFSException("Storage Region[%s] is not disabled", srName));
                        }
                        
                        if(response.code() == HttpStatus.CODE_OK) {
                            return TaskResult.success(null);
                        }
                        
                        return TaskResult.fail(new IllegalStateException(format("Server error[%d]", response.code())));
                    } catch (IOException e) {
                        return TaskResult.retry(e);
                    }
                }));
    }

    public PutObjectResult putObject(String srName, byte[] bytes) throws Exception {
        return putObject(srName, dataSplitter.split(bytes), Optional.of(Paths.get(UUID.randomUUID().toString())));
    }

    public PutObjectResult putObject(String srName, File file) throws Exception {
        try(FileInputStream input = new FileInputStream(file)) {
            return putObject(srName, input);
        } catch (IOException e) {
            throw new ClientException(e, "Write file[%s] failed", file.getAbsolutePath());
        }
    }

    public PutObjectResult putObject(String srName, InputStream input) throws Exception {
        return putObject(srName, dataSplitter.split(input), Optional.of(Paths.get(UUID.randomUUID().toString())));
    }

    public PutObjectResult putObject(String srName, Path objectPath, byte[] bytes) throws Exception {
        return putObject(srName, dataSplitter.split(bytes), Optional.of(objectPath));
    }

    public PutObjectResult putObject(String srName, Path objectPath, File file) throws Exception {
        try(FileInputStream input = new FileInputStream(file)) {
            return putObject(srName, objectPath, input);
        } catch (IOException e) {
            throw new ClientException(e, "Write file[%s] failed", file.getAbsolutePath());
        }
    }

    public PutObjectResult putObject(String srName, Path objectPath, InputStream input) throws Exception {
        return putObject(srName, dataSplitter.split(input), Optional.of(objectPath));
    }
    
    private PutObjectResult putObject(String srName, Iterator<ByteBuffer> buffers, Optional<Path> objectPath) throws Exception {
        AtomicInteger sequenceIDs = new AtomicInteger();
        Iterator<Function<URI, Call>> callProvider = IteratorUtils.from(buffers)
                .map(new FSPackageProtoMaker(
                        () -> sequenceIDs.getAndIncrement(),
                        getStorageRegionID(srName),
                        objectPath.map(Path::toString),
                        false,
                        Compression.NONE))
                .map(new PutObjectCallMaker(httpClient, OCTET_STREAM, srName))
                .iterator();
        
        if(!callProvider.hasNext()) {
            throw new IllegalArgumentException(Strings.format("No content to write for sr[%s]", srName));
        }
        
        Function<URI, Call> firstCall = callProvider.next();
        SettableFuture<PutObjectResult> resultFuture = SettableFuture.create();
        
        return Retrys.execute(new URIRetryable<PutObjectCallback> (
                format("put object to storage region[%s]", srName),
                getNodeHttpLocations(ServiceType.REGION),
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
                            resultFuture.setException(cause);
                        }
                    });
                    
                    try {
                        return TaskResult.success(retryFuture.get());
                    } catch(Exception e) {
                        return TaskResult.retry(e);
                    }
                })).get();
    }

    public BRFSObject getObject(GetObjectRequest request) {
        // TODO Auto-generated method stub
        return null;
    }

    public ListenableFuture<?> getObject(GetObjectRequest request, File outputFile) {
        // TODO Auto-generated method stub
        return null;
    }

    public boolean doesObjectExists(String srName, Path path) {
        // TODO Auto-generated method stub
        return false;
    }

    public void deleteObjects(String srName, long startTime, long endTime) {
        Retrys.execute(new URIRetryable<Void> (
                format("delete data of storage region[%s]", srName),
                getNodeHttpLocations(ServiceType.REGION),
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
                        
                        if(response.code() == HttpStatus.CODE_OK) {
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
    
    private URI buildUri(String scheme, String host, int port) {
        try {
            return new URI(scheme, null, host, port, null, null, null);
        }
        catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }
    
    private Iterable<URI> getNodeHttpLocations(ServiceType type) {
        return getNodeLocations(type, "http");
    }
    
    private Iterable<URI> getNodeLocations(ServiceType type, String scheme) {
        return Iterables.transform(
                nodeRanker.rank(discovery.getServiceList(type)),
                node -> buildUri(scheme, node.getHost(), node.getPort()));
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
            if(response.code() == HttpStatus.CODE_NEXT) {
                ResponseBody body = response.body();
                if(body == null) {
                    resultFuture.setException(
                            new IllegalStateException("No response content is found in writting"));
                    return;
                }
                
                NextData nextData = codec.fromJsonBytes(body.bytes(), NextData.class);
                if(nextData.getNextSequence() != seqences.getAsLong()) {
                    resultFuture.setException(
                            new IllegalStateException(
                                    Strings.format("Expected next seq[%d] but get [%d]",
                                            seqences.getAsLong(),
                                            nextData.getNextSequence())));
                    return;
                }
                
                if(!callIterator.hasNext()) {
                    resultFuture.setException(
                            new IllegalStateException(
                                    Strings.format("Expected fid is returned, but get NEXT code, current seq[%d]",
                                            seqences.getAsLong() - 1)));
                }
                
                log.info("writer block[%d] to url[%s] successfully", seqences.getAsLong() - 1, call.request().url());
                callIterator.next().apply(uri).enqueue(this);
                return;
            }
            
            if(response.code() == HttpStatus.CODE_OK) {
                ResponseBody body = response.body();
                if(body == null) {
                    resultFuture.setException(new IllegalStateException("No response content is found"));
                    return;
                }
                
                resultFuture.set(new SimplePutObjectResult(
                        Iterables.getOnlyElement(
                                codec.fromJsonBytes(
                                        body.bytes(),
                                        new TypeReference<List<String>>() {}))));
                
                return;
            }
            
            resultFuture.setException(new IllegalStateException(
                    Strings.format("Unexpected response code is returned[%d]", response.code())));
        }
        
        @Override
        public void onFailure(Call call, IOException cause) {
            resultFuture.setException(cause);
        }
        
    }

    @Override
    public void shutdown() {
        httpClient.dispatcher().executorService().shutdown();
        httpClient.connectionPool().evictAll();
        
        closeQuietly(discovery);
    }
    
    private void closeQuietly(Closeable closeable) {
        try {
            closeable.close();
        } catch (IOException e) {
            log.warn(Strings.format("close [%s] failed", closeable) , e);
        }
    }
}
