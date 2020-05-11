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

package com.bonree.brfs.gui.server;

import static com.bonree.brfs.client.utils.Strings.format;
import static com.google.common.net.HttpHeaders.ACCEPT;
import static java.util.Objects.requireNonNull;

import com.bonree.brfs.client.ClientException;
import com.bonree.brfs.client.discovery.Discovery;
import com.bonree.brfs.client.discovery.ServerNode;
import com.bonree.brfs.client.json.JsonCodec;
import com.bonree.brfs.client.utils.Retrys;
import com.bonree.brfs.client.utils.URIRetryable;
import com.bonree.brfs.client.utils.URIRetryable.TaskResult;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.net.MediaType;
import com.google.inject.Inject;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GuiInnerClient implements Discovery {
    private static final Logger LOG = LoggerFactory.getLogger(GuiInnerClient.class);
    private final OkHttpClient httpClient;
    private final List<URI> seedUris;
    private final JsonCodec jsonCodec;

    @Inject
    public GuiInnerClient(OkHttpClient httpClient, BrfsConfig config) {
        this.httpClient = requireNonNull(httpClient, "http client is null");
        this.seedUris = Arrays.asList(requireNonNull(collectRegionURI(config.getRegionAddress())));
        LOG.info("region uris {} ", config.getRegionAddress());
        this.jsonCodec = requireNonNull(new JsonCodec(new ObjectMapper()), "jsonCodec is null");
    }

    public URI[] collectRegionURI(List<String> regionAddress) {
        if (regionAddress == null || regionAddress.isEmpty()) {
            return new URI[0];
        }
        List<URI> array = new ArrayList<>();
        regionAddress.stream().forEach(
            x -> {
                try {
                    URI uri = new URI(x);
                    array.add(uri);
                    LOG.info(uri.toString());
                } catch (URISyntaxException e) {
                    throw new RuntimeException(x + " find invalid region node address");
                }
            }
        );
        return array.toArray(new URI[array.size()]);
    }

    public List<ServerNode> getServiceList(ServiceType type) {
        LOG.info("get {} server nodes {}", type, seedUris);
        return Retrys.execute(new URIRetryable<List<ServerNode>>(
            format("get service list of [%s]", type),
            seedUris,
            uri -> {
                HttpUrl url = HttpUrl.get(uri);
                if (url == null) {
                    throw new ClientException("Invalid seed URL: %s", uri);
                }

                Request request = new Request.Builder()
                    .url(url.newBuilder()
                            .encodedPath("/servers")
                            .addEncodedPathSegment(type.name().toLowerCase(Locale.ENGLISH))
                            .build())
                    .addHeader(ACCEPT, MediaType.JSON_UTF_8.toString())
                    .build();

                try (Response response = httpClient.newCall(request).execute()) {
                    if (response.code() != 200) {
                        return TaskResult.fail(
                            new ClientException("fetch service nodes[%s] failed with code[%d]",
                                                type,
                                                response.code()));
                    }

                    ResponseBody body = response.body();
                    if (body == null) {
                        return TaskResult.fail(new NullPointerException("No content from get-service response"));
                    }

                    return TaskResult.success(jsonCodec.fromJsonBytes(body.bytes(), new TypeReference<List<ServerNode>>() {
                    }));
                } catch (IOException e) {
                    return TaskResult.retry(e);
                }
            }));
    }

    @Override
    public void close() throws IOException {
        // Nothing to close
    }

}
