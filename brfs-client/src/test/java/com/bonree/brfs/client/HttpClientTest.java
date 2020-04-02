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

import java.io.IOException;

import com.bonree.brfs.client.BRFS;
import com.bonree.brfs.client.BRFSClientBuilder.AuthorizationIterceptor;
import com.bonree.brfs.client.utils.RequestBodys;
import com.bonree.brfs.client.utils.SocketChannelSocketFactory;
import com.bonree.brfs.common.proto.DataTransferProtos.FSPacketProto;
import com.google.protobuf.ByteString;

import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

public class HttpClientTest {

    /**
     * @param args
     * @throws IOException 
     */
    public static void main(String[] args) throws IOException {
        OkHttpClient httpClient = new OkHttpClient.Builder()
                .addNetworkInterceptor(chain -> chain.proceed(chain.request().newBuilder().addHeader("Expect", "100-continue").build()))
                .socketFactory(new SocketChannelSocketFactory())
                .build();
        
        FSPacketProto.Builder b = FSPacketProto.newBuilder();
        b.setCompress(0)
        .setCrcCheckCode(0)
        .setCrcFlag(false)
        .setData(ByteString.copyFromUtf8("123"))
        .setFileName("a")
        .setLastPacketInFile(false)
        .setOffsetInFile(0);
        
        Request httpRequest = new Request.Builder()
                .url(HttpUrl.get("http://localhost:8100")
                        .newBuilder()
                        .encodedPath("/data")
                        .build())
                .post(RequestBodys.create(BRFS.OCTET_STREAM, b.build()))
                .build();
        
        for(int i = 0; i < 3; i++) {
            System.out.println("send : "+ i);
            Response r = httpClient.newCall(httpRequest).execute();
            System.out.println(r.code());
            System.out.println(r.body().string());
        }
    }

}
