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

import com.fasterxml.jackson.annotation.JsonProperty;

public class StorageRegionInfo {
    private final StorageRegionID id;
    private final StorageRegionAttributes attributes;
    
    public StorageRegionInfo(
            @JsonProperty("id") StorageRegionID id,
            @JsonProperty("attributes") StorageRegionAttributes attributes) {
        this.id = requireNonNull(id);
        this.attributes = requireNonNull(attributes);
    }
    
    @JsonProperty("id")
    public StorageRegionID getStorageRegion() {
        return id;
    }
    
    @JsonProperty("attributes")
    public StorageRegionAttributes getAttributes() {
        return attributes;
    }
}
