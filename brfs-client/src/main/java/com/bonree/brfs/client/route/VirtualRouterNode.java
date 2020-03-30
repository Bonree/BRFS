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
package com.bonree.brfs.client.route;

import static com.google.common.base.MoreObjects.toStringHelper;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class VirtualRouterNode implements RouterNode {
    private final String changeId;
    private final int storageRegionIndex;
    private final String virtualId;
    private final String newSecondId;
    private final String version;
    
    @JsonCreator
    public VirtualRouterNode(
            @JsonProperty("changeId") String changeId,
            @JsonProperty("storageRegionIndex") int storageRegionIndex,
            @JsonProperty("virtualId") String virtualId,
            @JsonProperty("newSecondId") String newSecondId,
            @JsonProperty("version") String version) {
        this.changeId = changeId;
        this.storageRegionIndex = storageRegionIndex;
        this.virtualId = virtualId;
        this.newSecondId = newSecondId;
        this.version = version;
    }

    @JsonProperty("changeId")
    public String getChangeId() {
        return changeId;
    }

    @JsonProperty("storageRegionIndex")
    public int getStorageRegionIndex() {
        return storageRegionIndex;
    }

    @JsonProperty("virtualId")
    public String getVirtualId() {
        return virtualId;
    }

    @JsonProperty("newSecondId")
    public String getNewSecondId() {
        return newSecondId;
    }

    @JsonProperty("version")
    public String getVersion() {
        return version;
    }
    
    @Override
    public String toString() {
        return toStringHelper(getClass())
                .add("type", RouterNode.VIRTUAL)
                .add("changeId", changeId)
                .add("storageRegionIndex", storageRegionIndex)
                .add("virtualId", virtualId)
                .add("newSecondId", newSecondId)
                .add("version", version)
                .toString();
    }
}
