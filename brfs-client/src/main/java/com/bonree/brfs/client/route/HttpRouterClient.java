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

package com.bonree.brfs.client.route;

import static com.bonree.brfs.client.utils.Strings.format;
import static java.util.Objects.requireNonNull;

import com.bonree.brfs.client.discovery.Discovery.ServiceType;
import com.bonree.brfs.client.discovery.NodeSelector;
import com.bonree.brfs.client.json.JsonCodec;
import com.bonree.brfs.client.utils.HttpStatus;
import com.bonree.brfs.client.utils.Retrys;
import com.bonree.brfs.client.utils.URIRetryable;
import com.bonree.brfs.client.utils.URIRetryable.TaskResult;
import com.fasterxml.jackson.core.type.TypeReference;
import java.io.IOException;
import java.util.List;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;

public class HttpRouterClient implements RouterClient {
    private final OkHttpClient httpClient;
    private final NodeSelector nodeSelector;
    private final JsonCodec jsonCodec;

    public HttpRouterClient(
        OkHttpClient httpClient,
        NodeSelector nodeSelector,
        JsonCodec jsonCodec) {
        this.httpClient = requireNonNull(httpClient);
        this.nodeSelector = requireNonNull(nodeSelector);
        this.jsonCodec = requireNonNull(jsonCodec);
    }

    @Override
    public List<SecondServerID> getSecondServerId(String srName) {
        return Retrys.execute(new URIRetryable<List<SecondServerID>>(
            format("get second server id of sr[%s]", srName),
            nodeSelector.getNodeHttpLocations(ServiceType.REGION),
            uri -> {
                Request httpRequest = new Request.Builder()
                    .url(HttpUrl.get(uri)
                                .newBuilder()
                                .encodedPath("/router/secondServerID")
                                .addEncodedPathSegment(srName)
                                .build())
                    .get()
                    .build();

                try {
                    Response response = httpClient.newCall(httpRequest).execute();
                    if (response.code() == HttpStatus.CODE_OK) {
                        ResponseBody responseBody = response.body();
                        if (responseBody == null) {
                            return TaskResult.fail(new IllegalStateException("No response content is found"));
                        }

                        return TaskResult.success(jsonCodec.fromJsonBytes(
                            responseBody.bytes(),
                            new TypeReference<List<SecondServerID>>() {
                            }));
                    }

                    return TaskResult.fail(new IllegalStateException(format("Server error[%d]", response.code())));
                } catch (IOException e) {
                    return TaskResult.retry(e);
                }
            }));
    }

    @Override
    public List<NormalRouterNode> getNormalRouter(String srName) {
        return Retrys.execute(new URIRetryable<List<NormalRouterNode>>(
            format("get normal update router of sr[%s]", srName),
            nodeSelector.getNodeHttpLocations(ServiceType.REGION),
            uri -> {
                Request httpRequest = new Request.Builder()
                    .url(HttpUrl.get(uri)
                                .newBuilder()
                                .encodedPath("/router/update/normal")
                                .addEncodedPathSegment(srName)
                                .build())
                    .get()
                    .build();

                try {
                    Response response = httpClient.newCall(httpRequest).execute();
                    if (response.code() == HttpStatus.CODE_OK) {
                        ResponseBody responseBody = response.body();
                        if (responseBody == null) {
                            return TaskResult.fail(new IllegalStateException("No response content is found"));
                        }

                        return TaskResult.success(jsonCodec.fromJsonBytes(
                            responseBody.bytes(),
                            new TypeReference<List<NormalRouterNode>>() {
                            }));
                    }

                    return TaskResult.fail(new IllegalStateException(format("Server error[%d]", response.code())));
                } catch (IOException e) {
                    return TaskResult.retry(e);
                }
            }));
    }

    @Override
    public List<VirtualRouterNode> getVirtualRouter(String srName) {
        return Retrys.execute(new URIRetryable<List<VirtualRouterNode>>(
            format("get virtual update router of sr[%s]", srName),
            nodeSelector.getNodeHttpLocations(ServiceType.REGION),
            uri -> {
                Request httpRequest = new Request.Builder()
                    .url(HttpUrl.get(uri)
                                .newBuilder()
                                .encodedPath("/router/update/virtual")
                                .addEncodedPathSegment(srName)
                                .build())
                    .get()
                    .build();

                try {
                    Response response = httpClient.newCall(httpRequest).execute();
                    if (response.code() == HttpStatus.CODE_OK) {
                        ResponseBody responseBody = response.body();
                        if (responseBody == null) {
                            return TaskResult.fail(new IllegalStateException("No response content is found"));
                        }

                        return TaskResult.success(jsonCodec.fromJsonBytes(
                            responseBody.bytes(),
                            new TypeReference<List<VirtualRouterNode>>() {
                            }));
                    }

                    return TaskResult.fail(new IllegalStateException(format("Server error[%d]", response.code())));
                } catch (IOException e) {
                    return TaskResult.retry(e);
                }
            }));
    }
}
