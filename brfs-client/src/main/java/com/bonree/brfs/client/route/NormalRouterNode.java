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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class NormalRouterNode implements RouterNode {
    private final String changeId;
    private final int storageRegionIndex;
    private final String baseSecondId;
    private final Map<String, Integer> newSecondIDs;
    private final Map<String, String> secondFirstShip;
    private final String version;
    @JsonIgnore
    private Map<String, Collection<String>> firstSeconds;

    @JsonCreator
    public NormalRouterNode(
        @JsonProperty("changeId") String changeId,
        @JsonProperty("storageRegionIndex") int storageRegionIndex,
        @JsonProperty("baseSecondId") String baseSecondId,
        @JsonProperty("newSecondIDs") Map<String, Integer> newSecondIDs,
        @JsonProperty("secondFirstShip") Map<String, String> secondFirstShip,
        @JsonProperty("version") String version) {
        this.changeId = changeId;
        this.storageRegionIndex = storageRegionIndex;
        this.baseSecondId = baseSecondId;
        this.newSecondIDs = newSecondIDs;
        this.secondFirstShip = secondFirstShip;
        this.version = version;
        convertToShip(this.secondFirstShip);
    }

    private void convertToShip(Map<String, String> map) {
        if (map == null || map.isEmpty()) {
            return;
        }
        Map<String, Collection<String>> cache = new HashMap<>();
        map.forEach(
            (key, value) -> {
                if (key == null || value == null) {
                    return;
                }
                if (cache.get(value) == null) {
                    cache.put(value, new HashSet<>());
                }
                cache.get(value).add(key);
            });
        this.firstSeconds = cache;
    }

    @JsonProperty("changeId")
    public String getChangeId() {
        return changeId;
    }

    @JsonProperty("storageRegionIndex")
    public int getStorageRegionIndex() {
        return storageRegionIndex;
    }

    @JsonProperty("baseSecondId")
    public String getBaseSecondId() {
        return baseSecondId;
    }

    @JsonProperty("newSecondIDs")
    public Map<String, Integer> getNewSecondIDs() {
        return newSecondIDs;
    }

    @JsonProperty("secondFirstShip")
    public Map<String, String> getSecondFirstShip() {
        return secondFirstShip;
    }

    @JsonProperty("version")
    public String getVersion() {
        return version;
    }

    public Map<String, Collection<String>> getFirstSeconds() {
        return firstSeconds;
    }

    @Override
    public String toString() {
        return toStringHelper(getClass())
            .add("type", RouterNode.NORMAL)
            .add("changeId", changeId)
            .add("storageRegionIndex", storageRegionIndex)
            .add("baseSecondId", baseSecondId)
            .add("newSecondIDs", newSecondIDs)
            .add("version", version)
            .toString();
    }
}
