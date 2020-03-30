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
package com.bonree.brfs.client.discovery;

import static com.google.common.net.HttpHeaders.ACCEPT;
import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.List;
import java.util.Locale;

import com.bonree.brfs.client.ClientException;
import com.bonree.brfs.client.json.JsonCodec;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.net.MediaType;

import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;

public class HttpDiscovery implements Discovery {
    private final OkHttpClient httpClient;
    private final URI seedUri;
    private final JsonCodec jsonCodec;
    
    public HttpDiscovery(
            OkHttpClient httpClient,
            URI seedUri,
            JsonCodec jsonCodec) {
        this.httpClient = requireNonNull(httpClient, "http client is null");
        this.seedUri = requireNonNull(seedUri, "seed Uri is null");
        this.jsonCodec = requireNonNull(jsonCodec, "jsonCodec is null");
    }

    public List<ServerNode> getServiceList(ServiceType type) {
        HttpUrl url = HttpUrl.get(seedUri);
        if (url == null) {
            throw new ClientException("Invalid seed URL: " + seedUri);
        }
        
        Request request = new Request.Builder()
                .url(url.newBuilder()
                        .encodedPath("/servers")
                        .addEncodedPathSegment(type.name().toLowerCase(Locale.ENGLISH))
                        .build())
                .addHeader(ACCEPT, MediaType.JSON_UTF_8.toString())
                .build();
        
        try(Response response = httpClient.newCall(request).execute()) {
            if(response.code() != 200) {
                throw new ClientException("fetch service nodes[%s] failed with code[%d]", type, response.code());
            }
            
            ResponseBody body = response.body();
            if(body == null) {
                throw new NullPointerException("No content from get-service response");
            }
            
            return jsonCodec.fromJsonBytes(body.bytes(), new TypeReference<List<ServerNode>>() {});
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

}
