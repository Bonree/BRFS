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
package com.bonree.brfs.client.read;

import java.net.URI;

import com.bonree.brfs.client.BRFS;
import com.bonree.brfs.client.BRFSClientBuilder;
import com.bonree.brfs.client.BRFSObject;
import com.bonree.brfs.client.ClientConfigurationBuilder;
import com.bonree.brfs.client.GetObjectRequest;

public class ReadTest {
    
    public static void main(String[] args) {
        BRFS client = new BRFSClientBuilder()
                .config(new ClientConfigurationBuilder()
                        .setDataPackageSize(100)
                        .build())
                .build("root", "12345", new URI[] {URI.create("http://localhost:8200")});
//        CAAQABgCIiA5ZGUxNTk4MzhmZDc0Nzk2YmY1YTkzN2E4ODA3ZTc0NyiK/7a5mC4wgN3bAToCMjI6AjMwQP4ESDo=
        try {
            BRFSObject obj = client.getObject(GetObjectRequest.of(
                    "guice_test",
                    "CAAQABgCIiAzYTJkNmZkY2RlNjE0ZjhjYTgyNzIwZmVlM2E5YmE2YSjysPe6mC4wgN3bAToCMjI6AjMwQDpIOg=="));
            
            System.out.println("[" + obj.string() + "]");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
