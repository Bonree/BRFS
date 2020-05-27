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
     *
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        //CAAQABgJIiA0ZjRmNjM5ZWUyN2I0MGI1YjNhMWM4YWIzYTNjNzA5YiiAoLSnpS4wgN3bAToDMjEyOgIzMUAySDI=
        String fid = "CAAQABgJIiA0ZjRmNjM5ZWUyN2I0MGI1YjNhMWM4YWIzYTNjNzA5YiiAoLSnpS4wgN3bAToDMjEyOgIzMUAASDI=";

        BRFileSystem fs = new DefaultBRFileSystem(FileSystemConfig.newBuilder()
                                                      .setZkAddresses("192.168.101.87:2181")
                                                      .setClusterName("guice_test")
                                                      .setUsername("root")
                                                      .setPasswd("12345")
                                                      .build());
        StorageNameStick stick = fs.openStorageName("sr_v1");

        InputItem input = stick.readData(fid);
        System.out.println(new String(input.getBytes(), "utf-8"));
    }

}
