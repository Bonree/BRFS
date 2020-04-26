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

package com.bonree.brfs.gui.server.zookeeper;

import static com.google.common.base.MoreObjects.toStringHelper;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ZookeeperNode {
    private final String name;
    private final boolean hasValue;

    @JsonCreator
    public ZookeeperNode(
        @JsonProperty("name") String name,
        @JsonProperty("hasValue") boolean hasValue) {
        this.name = name;
        this.hasValue = hasValue;
    }

    @JsonProperty("name")
    public String getName() {
        return name;
    }

    @JsonProperty("hasValue")
    public boolean isHasValue() {
        return hasValue;
    }

    @Override
    public String toString() {
        return toStringHelper(getClass())
            .add("name", name)
            .add("hasValue", hasValue)
            .toString();
    }
}
