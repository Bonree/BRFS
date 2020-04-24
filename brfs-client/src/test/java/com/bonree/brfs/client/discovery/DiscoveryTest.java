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

import com.bonree.brfs.client.BRFSClientBuilder.AuthorizationIterceptor;
import com.bonree.brfs.client.discovery.Discovery.ServiceType;
import com.bonree.brfs.client.json.JsonCodec;
import com.bonree.brfs.client.utils.SocketChannelSocketFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import okhttp3.OkHttpClient;

public class DiscoveryTest {

    /**
     * @param args
     *
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        OkHttpClient httpClient = new OkHttpClient.Builder()
            .addNetworkInterceptor(new AuthorizationIterceptor("root", "12345"))
            .socketFactory(new SocketChannelSocketFactory())
            .build();

        JsonCodec codec = new JsonCodec(new ObjectMapper());

        Discovery discovery = new HttpDiscovery(httpClient, new URI[] {URI.create("http://localhost:8100")}, codec);

        List<ServerNode> nodes = discovery.getServiceList(ServiceType.REGION);
        System.out.println(nodes);

        discovery.close();
    }

}
