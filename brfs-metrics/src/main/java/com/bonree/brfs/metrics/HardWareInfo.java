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

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class HardWareInfo {
    private final int cpuCores;
    private final String cpuBrand;
    private final long memoryTotalBytes;
    private final String operatingSystemDesc;
    private final List<DiskPartition> diskPartitions;
    private final List<NetWorkInterfaceInfo> netInterfaces;
    
    @JsonCreator
    public HardWareInfo(
            @JsonProperty("cpuCores") int cpuCores,
            @JsonProperty("cpuBrand") String cpuBrand,
            @JsonProperty("memoryTotalBytes") long memoryTotalBytes,
            @JsonProperty("osDesc") String operatingSystemDesc,
            @JsonProperty("partitions") List<DiskPartition> diskPartitions,
            @JsonProperty("ifaces") List<NetWorkInterfaceInfo> netInterfaces) {
        this.cpuCores = cpuCores;
        this.cpuBrand = cpuBrand;
        this.memoryTotalBytes = memoryTotalBytes;
        this.operatingSystemDesc = operatingSystemDesc;
        this.diskPartitions = diskPartitions;
        this.netInterfaces = netInterfaces;
    }

    @JsonProperty("cpuCores")
    public int getCpuCores() {
        return cpuCores;
    }

    @JsonProperty("cpuBrand")
    public String getCpuBrand() {
        return cpuBrand;
    }

    @JsonProperty("memoryTotalBytes")
    public long getMemoryTotalBytes() {
        return memoryTotalBytes;
    }

    @JsonProperty("osDesc")
    public String getOperatingSystemDesc() {
        return operatingSystemDesc;
    }
    
    @JsonProperty("partitions")
    public List<DiskPartition> getDiskPartitions() {
        return diskPartitions;
    }
    
    @JsonProperty("ifaces")
    public List<NetWorkInterfaceInfo> getNetInterfaces() {
        return netInterfaces;
    }
    
    @Override
    public String toString() {
        return toStringHelper(getClass())
                .add("cpuCores", cpuCores)
                .add("cpuBrand", cpuBrand)
                .add("memoryTotalBytes", memoryTotalBytes)
                .add("operatingSystemDesc", operatingSystemDesc)
                .add("diskPartitions", diskPartitions)
                .add("netInterfaces", netInterfaces)
                .toString();
    }
}
