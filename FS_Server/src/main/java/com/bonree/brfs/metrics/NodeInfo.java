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

package com.bonree.brfs.metrics;

import static com.google.common.base.MoreObjects.toStringHelper;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class NodeInfo {
    private final String hostName;
    private final HardWareInfo hardWare;

    @JsonCreator
    public NodeInfo(
        @JsonProperty("hostName") String hostName,
        @JsonProperty("hardWare") HardWareInfo hardWare) {
        this.hostName = hostName;
        this.hardWare = hardWare;
    }

    @JsonProperty("hostName")
    public String getHostName() {
        return hostName;
    }

    @JsonProperty("hardWare")
    public HardWareInfo getHardWare() {
        return hardWare;
    }

    @Override
    public String toString() {
        return toStringHelper(getClass())
            .add("hostName", hostName)
            .add("hardWare", hardWare)
            .toString();
    }
}
