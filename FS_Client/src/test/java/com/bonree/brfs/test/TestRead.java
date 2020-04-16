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
package com.bonree.brfs.test;

import com.bonree.brfs.client.BRFileSystem;
import com.bonree.brfs.client.InputItem;
import com.bonree.brfs.client.StorageNameStick;
import com.bonree.brfs.client.impl.DefaultBRFileSystem;
import com.bonree.brfs.client.impl.FileSystemConfig;

public class TestRead {

    /**
     * @param args
     * @throws Exception 
     */
    public static void main(String[] args) throws Exception {
        String fid = "CAAQABgNIiAxY2EwOTE4N2JkNjE0YTJiYTU1M2VlZDc2ZGRiMTU2ZCit7Nqe5i0wgN3bAToDMjI3OgMyMjZAAEhX";
        
        FileSystemConfig config = FileSystemConfig.newBuilder()
                .setClusterName("brfs_sdk2")
                .setUsername("root")
                .setPasswd("12345")
                .setConnectionPoolSize(10)
                .setZkAddresses("192.168.13.91:2181")
                .build();
        BRFileSystem fileSystem = new DefaultBRFileSystem(config);
        StorageNameStick stick = fileSystem.openStorageName("T_WINSDK_STAT_ERROR_SNAPSHOT");
        
        InputItem input = stick.readData(fid);
        System.out.println(input);
    }

}
