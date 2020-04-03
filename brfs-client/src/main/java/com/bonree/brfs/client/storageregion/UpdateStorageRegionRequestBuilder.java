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

import java.time.Duration;
import java.util.Properties;

public class UpdateStorageRegionRequestBuilder {
    private final Properties props = new Properties();

    UpdateStorageRegionRequestBuilder() {}

    public UpdateStorageRegionRequestBuilder setEnabled(boolean enabled) {
        this.props.put(StorageRegionPropertyNames.PROP_ENABLED, String.valueOf(enabled));
        return this;
    }

    public UpdateStorageRegionRequestBuilder setReplicateNum(int replicateNum) {
        this.props.put(StorageRegionPropertyNames.PROP_REPLICATE_NUM, String.valueOf(replicateNum));
        return this;
    }

    public UpdateStorageRegionRequestBuilder setDataTTL(String dataTTL) {
        Duration.parse(dataTTL);
        this.props.put(StorageRegionPropertyNames.PROP_DATATTL, dataTTL);
        return this;
    }

    public UpdateStorageRegionRequestBuilder setFileCapacity(long fileCapacity) {
        this.props.put(StorageRegionPropertyNames.PROP_FILE_CAPACITY, String.valueOf(fileCapacity));
        return this;
    }

    public UpdateStorageRegionRequestBuilder setFilePartition(String filePartition) {
        Duration.parse(filePartition);
        this.props.put(StorageRegionPropertyNames.PROP_FILE_PARTITION, filePartition);
        return this;
    }

    public UpdateStorageRegionRequest build() {
        return new UpdateStorageRegionRequest() {
            
            @Override
            public Properties getAttributes() {
                return props;
            }
        };
    }
}
