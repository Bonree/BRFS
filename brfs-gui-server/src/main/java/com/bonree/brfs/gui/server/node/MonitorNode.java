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

package com.bonree.brfs.gui.server.node;

import static com.google.common.base.MoreObjects.toStringHelper;

import com.bonree.brfs.common.jackson.Json;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class MonitorNode {
    private final String id;
    private final String host;
    private final int cpuCores;
    private final String cpuBrand;
    private final long totalMemSize;
    private final String os;

    @JsonCreator
    public MonitorNode(
        @JsonProperty("id") String id,
        @JsonProperty("host") String host,
        @JsonProperty("cpuCores") int cpuCores,
        @JsonProperty("cpuBrand") String cpuBrand,
        @JsonProperty("totalMemSize") long totalMemSize,
        @JsonProperty("os") String os) {
        this.id = id;
        this.host = host;
        this.cpuCores = cpuCores;
        this.cpuBrand = cpuBrand;
        this.totalMemSize = totalMemSize;
        this.os = os;
    }

    @JsonProperty("id")
    public String getId() {
        return id;
    }

    @JsonProperty("host")
    public String getHost() {
        return host;
    }

    @JsonProperty("cpuCores")
    public int getCpuCores() {
        return cpuCores;
    }

    @JsonProperty("cpuBrand")
    public String getCpuBrand() {
        return cpuBrand;
    }

    @JsonProperty("totalMemSize")
    public long getTotalMemSize() {
        return totalMemSize;
    }

    @JsonProperty("os")
    public String getOs() {
        return os;
    }

    @Override
    public String toString() {
        return toStringHelper(this)
            .add("id", id)
            .add("host", host)
            .add("cpuCores", cpuCores)
            .add("cpuBrand", cpuBrand)
            .add("totalMemSize", totalMemSize)
            .add("os", os)
            .toString();
    }
}
