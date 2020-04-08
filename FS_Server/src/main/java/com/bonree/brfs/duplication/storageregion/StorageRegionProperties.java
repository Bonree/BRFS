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

import static com.bonree.brfs.client.storageregion.StorageRegionPropertyNames.PROP_DATATTL;
import static com.bonree.brfs.client.storageregion.StorageRegionPropertyNames.PROP_ENABLED;
import static com.bonree.brfs.client.storageregion.StorageRegionPropertyNames.PROP_FILE_CAPACITY;
import static com.bonree.brfs.client.storageregion.StorageRegionPropertyNames.PROP_FILE_PARTITION;
import static com.bonree.brfs.client.storageregion.StorageRegionPropertyNames.PROP_REPLICATE_NUM;
import static com.google.common.base.MoreObjects.toStringHelper;

import java.util.Optional;
import java.util.Properties;

import javax.inject.Inject;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class StorageRegionProperties {
    private final boolean enable;
    private final int replicateNum;
    private final String dataTtl;
    private final long fileCapacity;
    private final String filePartitionDuration;
    
    private static StorageRegionConfig defaultConfig;
    
    @Inject
    public static void setDefaultConfig(StorageRegionConfig config) {
        defaultConfig = config;
    }
    
    public static StorageRegionProperties withDefault() {
        return new StorageRegionProperties(
                true,
                defaultConfig.getReplicateNum(),
                defaultConfig.getTtl(),
                defaultConfig.getFileCapacity(),
                defaultConfig.getPartitionDuration());
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
    
    public StorageRegionProperties override(Properties props) {
        return new StorageRegionProperties(
                Optional.ofNullable(props.getProperty(PROP_ENABLED)).map(Boolean::parseBoolean).orElse(enable),
                Optional.ofNullable(props.getProperty(PROP_REPLICATE_NUM)).map(Integer::parseInt).orElse(replicateNum),
                props.getProperty(PROP_DATATTL, dataTtl),
                Optional.ofNullable(props.getProperty(PROP_FILE_CAPACITY)).map(Long::parseLong).orElse(fileCapacity),
                props.getProperty(PROP_FILE_PARTITION, filePartitionDuration));
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
