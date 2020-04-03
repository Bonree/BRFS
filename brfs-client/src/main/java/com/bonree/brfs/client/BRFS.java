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

import java.io.File;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.List;

import com.bonree.brfs.client.storageregion.CreateStorageRegionRequest;
import com.bonree.brfs.client.storageregion.ListStorageRegionRequest;
import com.bonree.brfs.client.storageregion.StorageRegionID;
import com.bonree.brfs.client.storageregion.StorageRegionInfo;
import com.bonree.brfs.client.storageregion.UpdateStorageRegionRequest;
import com.google.common.util.concurrent.ListenableFuture;

public interface BRFS {
    StorageRegionID createStorageRegion(CreateStorageRegionRequest request) throws Exception;
    
    boolean doesStorageRegionExists(String srName) throws Exception;
    
    List<String> listStorageRegions() throws Exception;
    
    List<String> listStorageRegions(ListStorageRegionRequest request) throws Exception;
    
    boolean updateStorageRegion(String srName, UpdateStorageRegionRequest request) throws Exception;
    
    StorageRegionInfo getStorageRegionInfo(String srName) throws Exception;
    
    void deleteStorageRegion(String srName) throws Exception;
    
    PutObjectResult putObject(String srName, byte[] bytes) throws Exception;
    
    PutObjectResult putObject(String srName, File file) throws Exception;
    
    PutObjectResult putObject(String srName, InputStream input) throws Exception;
    
    PutObjectResult putObject(String srName, Path objectPath, byte[] bytes) throws Exception;
    
    PutObjectResult putObject(String srName, Path objectPath, File file) throws Exception;
    
    PutObjectResult putObject(String srName, Path objectPath, InputStream input) throws Exception;
    
    BRFSObject getObject(GetObjectRequest request) throws Exception;
    
    ListenableFuture<?> getObject(GetObjectRequest request, File outputFile);
    
    boolean doesObjectExists(String srName, Path path) throws Exception;
    
    void deleteObjects(long startTime, long endTime) throws Exception;
    
    void shutdown();
}
