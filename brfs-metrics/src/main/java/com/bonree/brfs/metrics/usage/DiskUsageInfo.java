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
package com.bonree.brfs.metrics.usage;

import static com.google.common.base.MoreObjects.toStringHelper;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class DiskUsageInfo {
    private final long usedBytes;
    private final long freeBytes;
    private final int ioUsagePercent;
    
    @JsonCreator
    public DiskUsageInfo(
            @JsonProperty("usedBytes") long usedBytes,
            @JsonProperty("freeBytes") long freeBytes,
            @JsonProperty("ioUsage") int ioUsagePercent) {
        this.usedBytes = usedBytes;
        this.freeBytes = freeBytes;
        this.ioUsagePercent = ioUsagePercent;
    }

    @JsonProperty("usedBytes")
    public long getUsedBytes() {
        return usedBytes;
    }

    @JsonProperty("freeBytes")
    public long getFreeBytes() {
        return freeBytes;
    }

    @JsonProperty("ioUsage")
    public int getIoUsagePercent() {
        return ioUsagePercent;
    }
    
    @Override
    public String toString() {
        return toStringHelper(getClass())
                .add("usedBytes", usedBytes)
                .add("freeBytes", freeBytes)
                .add("ioUsagePercent", ioUsagePercent)
                .toString();
    }
}
