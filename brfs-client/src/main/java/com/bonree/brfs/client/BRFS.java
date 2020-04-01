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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import com.bonree.brfs.client.data.DataSplitter;
import com.bonree.brfs.client.data.FixedSizeDataSplitter;
import com.bonree.brfs.client.discovery.Discovery;
import com.bonree.brfs.client.discovery.Discovery.ServiceType;
import com.bonree.brfs.client.json.JsonCodec;
import com.bonree.brfs.client.storageregion.CreateStorageRegionRequest;
import com.bonree.brfs.client.storageregion.ListStorageRegionRequest;
import com.bonree.brfs.client.storageregion.StorageRegionID;
import com.bonree.brfs.client.storageregion.StorageRegionInfo;
import com.bonree.brfs.client.storageregion.UpdateStorageRegionRequest;
import com.bonree.brfs.client.utils.HttpStatus;
import com.bonree.brfs.client.utils.Retrys;
import com.bonree.brfs.client.utils.Strings;
import com.bonree.brfs.client.utils.URIRetryable;
import com.bonree.brfs.client.utils.URIRetryable.TaskResult;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.ListenableFuture;

import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;

public class BRFS implements BRFSClient {
    
    public static final MediaType JSON = MediaType.get("application/json; charset=utf-8");
    public static final MediaType OCTET_STREAM = MediaType.get("application/octet-stream");
    
    private static final Duration DEFAULT_EXPIRE_DURATION = Duration.ofMinutes(30);
    private static final Duration DEFAULT_REFRESH_DURATION = Duration.ofMinutes(10);
    
    private final OkHttpClient httpClient;
    private final Discovery discovery;
    private final JsonCodec codec;
    
    private final DataSplitter dataSplitter;
    
    private final LoadingCache<String, Integer> storageRegionCache;
    
    public BRFS(ClientConfiguration config, OkHttpClient httpClient, Discovery discovery, JsonCodec codec) {
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
    }

    public StorageRegionID createStorageRegion(CreateStorageRegionRequest request) throws Exception {
        RequestBody body = RequestBody.create(codec.toJson(request.getAttributes()), JSON);
        
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
                            
                            TaskResult.success(codec.fromJsonBytes(responseBody.bytes(), new TypeReference<List<String>>() {}));
                        }
                        
                        return TaskResult.fail(new IllegalStateException(format("Server error[%d]", response.code())));
                    } catch (IOException e) {
                        return TaskResult.retry(e);
                    }
                }));
    }

    public boolean updateStorageRegion(UpdateStorageRegionRequest request) throws Exception {
        RequestBody body = RequestBody.create(codec.toJson(request.getAttributes()), JSON);
        
        return Retrys.execute(new URIRetryable<Boolean> (
                format("update storage region[%s]", request.getStorageRegionName()),
                getNodeHttpLocations(ServiceType.REGION),
                uri -> {
                    Request httpRequest = new Request.Builder()
                            .url(HttpUrl.get(uri)
                                    .newBuilder()
                                    .encodedPath("/sr")
                                    .addEncodedPathSegment(request.getStorageRegionName())
                                    .build())
                            .post(body)
                            .build();
                    
                    try {
                        Response response = httpClient.newCall(httpRequest).execute();
                        if(response.code() == HttpStatus.CODE_NOT_FOUND) {
                            return TaskResult.fail(new BRFSException("Storage Region[%s] is not existed",
                                    request.getStorageRegionName()));
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
                            
                            TaskResult.success(codec.fromJsonBytes(responseBody.bytes(), StorageRegionInfo.class));
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
                            TaskResult.success(null);
                        }
                        
                        return TaskResult.fail(new IllegalStateException(format("Server error[%d]", response.code())));
                    } catch (IOException e) {
                        return TaskResult.retry(e);
                    }
                }));
    }

    public PutObjectResult putObject(String srName, byte[] bytes) {
        return putObject(srName, new ByteArrayInputStream(bytes));
    }

    public PutObjectResult putObject(String srName, File file) {
        try(FileInputStream input = new FileInputStream(file)) {
            return putObject(srName, input);
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        return null;
    }

    public PutObjectResult putObject(String srName, InputStream input) {
        Iterator<RequestBody> requestContents = Iterators.transform(
                dataSplitter.split(input),
                buffer -> buildRequestBody(buffer, OCTET_STREAM));
        
        if(!requestContents.hasNext()) {
            throw new IllegalArgumentException(Strings.format("No content to write for sr[%s]", srName));
        }
        
        RequestBody firstContent = requestContents.next();
        return Retrys.execute(new URIRetryable<PutObjectResult> (
                format("put object to storage region[%s]", srName),
                getNodeHttpLocations(ServiceType.REGION),
                uri -> {
                    Request httpRequest = new Request.Builder()
                            .url(HttpUrl.get(uri)
                                    .newBuilder()
                                    .encodedPath("/data")
                                    .addEncodedPathSegment(srName)
                                    .build())
                            .post(null)
                            .build();
                    
                    try {
                        Response response = httpClient.newCall(httpRequest).execute();
                        if(response.code() == HttpStatus.CODE_NOT_FOUND) {
                            return TaskResult.fail(new BRFSException("Storage Region[%s] is not existed", srName));
                        }
                        
                        if(response.code() == HttpStatus.CODE_FORBIDDEN) {
                            return TaskResult.fail(new BRFSException("Storage Region[%s] is not enabled", srName));
                        }
                        
                        if(response.code() == HttpStatus.CODE_OK) {
                            ResponseBody body = response.body();
                            if(body == null) {
//                                throw new BRFSException("No result is return by Server when put object to [%s]", srName);
                            }
                            
                            // TODO get fid
                            TaskResult.success(null);
                        }
                        
                        return TaskResult.fail(new IllegalStateException(format("Server error[%d]", response.code())));
                    } catch (IOException e) {
                        return TaskResult.retry(e);
                    }
                }));
    }

    public PutObjectResult putObject(String srName, Path objectPath, byte[] bytes) {
        // TODO Auto-generated method stub
        return null;
    }

    public PutObjectResult putObject(String srName, Path objectPath, File file) {
        // TODO Auto-generated method stub
        return null;
    }

    public PutObjectResult putObject(String srName, Path objectPath, InputStream input) {
        // TODO Auto-generated method stub
        return null;
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

    public void deleteObjects(long startTime, long endTime) {
        // TODO Auto-generated method stub

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
        return Iterables.transform(discovery.getServiceList(type),
                node -> buildUri(scheme, node.getHost(), node.getPort()));
    }

    @Override
    public void close() throws IOException {
        httpClient.dispatcher().executorService().shutdown();
        httpClient.connectionPool().evictAll();
        
        discovery.close();
    }
    
    private static RequestBody buildRequestBody(ByteBuffer buffer, MediaType contentType) {
        if(buffer.hasArray()) {
            return RequestBody.create(
                    buffer.array(),
                    contentType,
                    buffer.arrayOffset() + buffer.position(),
                    buffer.remaining());
        }
        
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        
        return RequestBody.create(bytes, contentType);
    }
}
