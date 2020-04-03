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
package com.bonree.brfs.client.storageregion;

import static com.google.common.base.MoreObjects.toStringHelper;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class StorageRegionAttributes {
    private final boolean enabled;
    private final int replicateNum;
    private final String dataTTL;
    private final long fileCapacity;
    private final String filePartition;
    
    @JsonCreator
    public StorageRegionAttributes(
            @JsonProperty("enabled") boolean enabled,
            @JsonProperty("replicates") int replicateNum,
            @JsonProperty("ttl") String dataTTL,
            @JsonProperty("capacity") long fileCapacity,
            @JsonProperty("partition") String filePartition) {
        this.enabled = enabled;
        this.replicateNum = replicateNum;
        this.dataTTL = dataTTL;
        this.fileCapacity = fileCapacity;
        this.filePartition = filePartition;
    }

    @JsonProperty("enabled")
    public boolean isEnabled() {
        return enabled;
    }

    @JsonProperty("replicates")
    public int getReplicateNum() {
        return replicateNum;
    }

    @JsonProperty("ttl")
    public String getDataTTL() {
        return dataTTL;
    }

    @JsonProperty("capacity")
    public long getFileCapacity() {
        return fileCapacity;
    }

    @JsonProperty("partition")
    public String getFilePartition() {
        return filePartition;
    }
    
    @Override
    public String toString() {
        return toStringHelper(getClass())
                .add("enabled", enabled)
                .add("replicateNum", replicateNum)
                .add("dataTTL", dataTTL)
                .add("fileCapacity", fileCapacity)
                .add("filePartition", filePartition)
                .toString();
    }
}
