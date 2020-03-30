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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import com.bonree.brfs.client.Retrys.MultiObjectRetryable;
import com.bonree.brfs.client.discovery.Discovery;
import com.bonree.brfs.client.discovery.Discovery.ServiceType;
import com.bonree.brfs.client.json.JsonCodec;
import com.bonree.brfs.client.storageregion.CreateStorageRegionRequest;
import com.bonree.brfs.client.storageregion.ListStorageRegionRequest;
import com.bonree.brfs.client.storageregion.StorageRegionID;
import com.bonree.brfs.client.storageregion.StorageRegionInfo;
import com.bonree.brfs.client.storageregion.UpdateStorageRegionRequest;
import com.bonree.brfs.client.utils.HttpStatus;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Iterables;
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
    
    private final OkHttpClient httpClient;
    private final Discovery discovery;
    private final JsonCodec codec;
    
    public BRFS(OkHttpClient httpClient, Discovery discovery, JsonCodec codec) {
        this.httpClient = requireNonNull(httpClient, "http client is null");
        this.discovery = requireNonNull(discovery, "discovery is null");
        this.codec = requireNonNull(codec, "codec is null");
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
        // TODO Auto-generated method stub
        return null;
    }

    public PutObjectResult putObject(String srName, File file) {
        // TODO Auto-generated method stub
        return null;
    }

    public PutObjectResult putObject(String srName, InputStream input) {
        // TODO Auto-generated method stub
        return null;
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
    }
    
    private class URIRetryable<T> extends MultiObjectRetryable<T, URI> {
        private final AtomicBoolean retryable = new AtomicBoolean(true);
        
        private final Task<T> task;
        
        public URIRetryable(String description, Iterable<URI> iterable, Task<T> task) {
            this(description, iterable.iterator(), task);
        }

        public URIRetryable(String description, Iterator<URI> iter, Task<T> task) {
            super(description, iter);
            this.task = requireNonNull(task, "task is null");
        }

        @Override
        protected Throwable noMoreObjectError() {
            return new NoNodeException("no node is found for task[%s]!", getDescription());
        }

        @Override
        protected boolean continueRetry() {
            return retryable.get();
        }

        @Override
        protected T execute(URI uri) throws Exception {
            TaskResult<T> result = task.execute(uri);
            if(result.isCompleted()) {
                retryable.set(false);
                if(result.getCause() != null) {
                    throw result.getCause();
                }
                
                return result.getResult();
            }
            
            throw requireNonNull(result.getCause());
        }
        
    }
    
    private static interface Task<T> {
        TaskResult<T> execute(URI uri);
    }
    
    private static interface TaskResult<T> {
        boolean isCompleted();
        
        Exception getCause();
        
        T getResult();
        
        static <T> TaskResult<T> success(T result) {
            return new TaskResult<T>() {

                @Override
                public boolean isCompleted() {
                    return true;
                }

                @Override
                public Exception getCause() {
                    return null;
                }

                @Override
                public T getResult() {
                    return result;
                }
            };
        }
        
        static <T> TaskResult<T> fail(Exception cause) {
            return new TaskResult<T>() {

                @Override
                public boolean isCompleted() {
                    return true;
                }

                @Override
                public Exception getCause() {
                    return cause;
                }

                @Override
                public T getResult() {
                    return null;
                }
            };
        }
        
        static <T> TaskResult<T> retry(Exception cause) {
            return new TaskResult<T>() {

                @Override
                public boolean isCompleted() {
                    return false;
                }

                @Override
                public Exception getCause() {
                    return cause;
                }

                @Override
                public T getResult() {
                    return null;
                }
            };
        }
    }
}
