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
package com.bonree.brfs.client.sr;

import java.net.URI;
import java.util.List;

import com.bonree.brfs.client.BRFS;
import com.bonree.brfs.client.BRFSClientBuilder;

public class ListTest {

    /**
     * @param args
     * @throws Exception 
     */
    public static void main(String[] args) throws Exception {
        BRFS client = new BRFSClientBuilder().build("root", "12345", new URI[] {URI.create("http://localhost:8200")});
        
        List<String> srs = client.listStorageRegions();
        System.out.println("list : " + srs);
    }

}
