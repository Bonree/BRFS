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

package com.bonree.brfs.client.write;

import com.bonree.brfs.client.BRFS;
import com.bonree.brfs.client.BRFSClientBuilder;
import com.bonree.brfs.client.ClientConfigurationBuilder;
import com.bonree.brfs.client.PutObjectResult;
import java.net.URI;

public class WriteTest {

    /**
     * @param args
     *
     * @throws Exception
     */
    public static void main(String[] args) {
        BRFS client = new BRFSClientBuilder()
            .config(new ClientConfigurationBuilder()
                        .setDataPackageSize(300)
                        .build())
            .build("root", "12345", new URI[] {URI.create("http://localhost:8200")});

        try {
            PutObjectResult r = client.putObject("guice_test", "1234567890abcd".getBytes());
            System.out.println(r.getFID());
        } catch (Exception e) {
            e.printStackTrace();
        }

        client.shutdown();
    }

}
