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
import com.bonree.brfs.client.storageregion.CreateStorageRegionRequest;
import com.bonree.brfs.client.storageregion.StorageRegionID;
import com.bonree.brfs.client.storageregion.StorageRegionInfo;
import com.bonree.brfs.client.storageregion.UpdateStorageRegionRequest;

public class storageRegionTest {

    /**
     * @param args
     */
    public static void main(String[] args) {
        BRFS client = new BRFSClientBuilder().build("root", "12345", new URI[] {URI.create("http://localhost:8100")});
        
        String rn = "new_region1";
        try {
          StorageRegionID sr = client.createStorageRegion(CreateStorageRegionRequest.newBuilder(rn)
                  .build());
          System.out.println("create : " + sr);
          
          boolean exist = client.doesStorageRegionExists(rn);
          System.out.println("exist: " + exist);
          
          List<String> srs = client.listStorageRegions();
          System.out.println("list : " + srs);
          
          StorageRegionInfo info = client.getStorageRegionInfo(rn);
          System.out.println("info : " + info);
          
          boolean updated = client.updateStorageRegion(rn, UpdateStorageRegionRequest.newBuilder()
                  .setEnabled(false)
                  .setDataTTL("P20D")
                  .setFileCapacity(100)
                  .setFilePartition("PT2H")
                  .setReplicateNum(4)
                  .build());
          System.out.println("updated: " + updated);
          
          StorageRegionInfo info1 = client.getStorageRegionInfo(rn);
          System.out.println("updated info : " + info1);
          
//          client.deleteStorageRegion("new_test");
          System.out.println(sr);
      } catch (Exception e) {
          e.printStackTrace();
      }
        
    }

}
