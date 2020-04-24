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

package com.bonree.brfs.client.data;

import com.bonree.brfs.client.utils.IteratorUtils.Transformer;
import com.bonree.brfs.client.utils.RequestBodys;
import com.bonree.brfs.common.proto.DataTransferProtos.FSPacketProto;
import java.net.URI;
import java.util.function.Function;
import okhttp3.Call;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;

public class PutObjectCallMaker implements Transformer<FSPacketProto, Function<URI, Call>> {
    private final OkHttpClient httpClient;
    private final MediaType contentType;
    private final String srName;

    public PutObjectCallMaker(
        OkHttpClient httpClient,
        MediaType contentType,
        String srName) {
        this.httpClient = httpClient;
        this.contentType = contentType;
        this.srName = srName;
    }

    @Override
    public Function<URI, Call> apply(FSPacketProto data, boolean noMoreElement) {
        return uri -> httpClient.newCall(
            new Request.Builder()
                .url(HttpUrl.get(uri).newBuilder()
                            .encodedPath("/data")
                            .addEncodedPathSegment(srName)
                            .build())
                .post(RequestBodys.create(contentType, data))
                .build());
    }

}
