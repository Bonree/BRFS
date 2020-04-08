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
package com.bonree.brfs.duplication.storageregion;

import com.fasterxml.jackson.annotation.JsonProperty;

public class StorageRegionConfig {
    @JsonProperty("data.ttl")
    private String ttl = "P30D";
    
    @JsonProperty("replicate.count")
    private int replicateNum = 2;
    
    @JsonProperty("data.file.capacity")
    private long fileCapacity = 64 * 1024 * 1024;
    
    @JsonProperty("data.file.patition.duration")
    private String partitionDuration = "PT1H";

    public String getTtl() {
        return ttl;
    }

    public void setTtl(String ttl) {
        this.ttl = ttl;
    }

    public int getReplicateNum() {
        return replicateNum;
    }

    public void setReplicateNum(int replicateNum) {
        this.replicateNum = replicateNum;
    }

    public long getFileCapacity() {
        return fileCapacity;
    }

    public void setFileCapacity(long fileCapacity) {
        this.fileCapacity = fileCapacity;
    }

    public String getPartitionDuration() {
        return partitionDuration;
    }

    public void setPartitionDuration(String partitionDuration) {
        this.partitionDuration = partitionDuration;
    }

}
