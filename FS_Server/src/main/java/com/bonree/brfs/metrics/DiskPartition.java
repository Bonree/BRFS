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

public class DiskPartition {
    private final String mountPoint;
    private final String device;
    private final long capacity;
    private final boolean brfsUsed;

    @JsonCreator
    public DiskPartition(
        @JsonProperty("mountPoint") String mountPoint,
        @JsonProperty("device") String device,
        @JsonProperty("capacity") long capacity,
        @JsonProperty("brfsUsed") boolean brfsUsed) {
        this.mountPoint = mountPoint;
        this.device = device;
        this.capacity = capacity;
        this.brfsUsed = brfsUsed;
    }

    @JsonProperty("mountPoint")
    public String getMountPoint() {
        return mountPoint;
    }

    @JsonProperty("device")
    public String getDevice() {
        return device;
    }

    @JsonProperty("capacity")
    public long getCapacity() {
        return capacity;
    }

    @JsonProperty("brfsUsed")
    public boolean isBrfsUsed() {
        return brfsUsed;
    }

    @Override
    public String toString() {
        return toStringHelper(getClass())
            .add("mountPoint", mountPoint)
            .add("device", device)
            .add("capacity", capacity)
            .add("brfsUsed", brfsUsed)
            .toString();
    }
}
