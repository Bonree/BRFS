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

import static com.google.common.base.MoreObjects.toStringHelper;

import java.util.Map;

import com.bonree.brfs.client.storageregion.StorageRegionPropertyNames;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class StorageRegionProperties {
    private final boolean enable;
    private final int replicateNum;
    private final String dataTtl;
    private final long fileCapacity;
    private final String filePartitionDuration;
    
    public static StorageRegionProperties withDefault() {
        return new StorageRegionProperties(true, 2, "P30D", 64 * 1024 * 1024, "PT1H");
    }

    @JsonCreator
    public StorageRegionProperties(
            @JsonProperty("enable") boolean enable,
            @JsonProperty("replicate_num") int replicateNum,
            @JsonProperty("data_ttl") String dataTtl,
            @JsonProperty("file_capacity") long fileCapacity,
            @JsonProperty("patition_duration") String filePartitionDuration) {
        this.enable = enable;
        this.replicateNum = replicateNum;
        this.dataTtl = dataTtl;
        this.fileCapacity = fileCapacity;
        this.filePartitionDuration = filePartitionDuration;
    }

    @JsonProperty("enable")
    public boolean isEnable() {
        return enable;
    }

    @JsonProperty("replicate_num")
    public int getReplicateNum() {
        return replicateNum;
    }

    @JsonProperty("data_ttl")
    public String getDataTtl() {
        return dataTtl;
    }

    @JsonProperty("file_capacity")
    public long getFileCapacity() {
        return fileCapacity;
    }

    @JsonProperty("patition_duration")
    public String getFilePartitionDuration() {
        return filePartitionDuration;
    }
    
    public StorageRegionProperties override(Map<String, Object> props) {
        return new StorageRegionProperties(
                (boolean) props.getOrDefault(StorageRegionPropertyNames.PROP_ENABLED, enable),
                (int) props.getOrDefault(StorageRegionPropertyNames.PROP_REPLICATE_NUM, replicateNum),
                (String) props.getOrDefault(StorageRegionPropertyNames.PROP_DATATTL, dataTtl),
                (long) props.getOrDefault(StorageRegionPropertyNames.PROP_FILE_CAPACITY, fileCapacity),
                (String) props.getOrDefault(StorageRegionPropertyNames.PROP_FILE_PARTITION, filePartitionDuration));
    }
    
    @Override
    public String toString() {
        return toStringHelper(getClass())
                .add("enable", enable)
                .add("replicateNum", replicateNum)
                .add("dataTtl", dataTtl)
                .add("fileCapacity", fileCapacity)
                .add("filePartitionDuration", filePartitionDuration)
                .toString();
    }
}
