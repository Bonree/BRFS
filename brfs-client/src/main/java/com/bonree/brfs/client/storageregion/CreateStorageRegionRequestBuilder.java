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

import static java.util.Objects.requireNonNull;

import java.time.Period;

public class CreateStorageRegionRequestBuilder {
    private String name;
    private Boolean enabled;
    private Integer replicateNum;
    private String dataTTL;
    private Long fileCapacity;
    private String filePartition;

    CreateStorageRegionRequestBuilder() {
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public void setReplicateNum(int replicateNum) {
        this.replicateNum = replicateNum;
    }

    public void setDataTTL(String dataTTL) {
        Period.parse(dataTTL);
        this.dataTTL = dataTTL;
    }

    public void setFileCapacity(long fileCapacity) {
        this.fileCapacity = fileCapacity;
    }

    public void setFilePartition(String filePartition) {
        Period.parse(dataTTL);
        this.filePartition = filePartition;
    }

    public CreateStorageRegionRequest build() {
        String storageRegionName = requireNonNull(name);
        StorageRegionAttributes attributes = new StorageRegionAttributes(
                enabled,
                replicateNum,
                dataTTL,
                fileCapacity,
                filePartition);
        
        return new CreateStorageRegionRequest() {
            
            @Override
            public String getStorageRegionName() {
                return storageRegionName;
            }
            
            @Override
            public StorageRegionAttributes getAttributes() {
                return attributes;
            }
        };
    }
}
