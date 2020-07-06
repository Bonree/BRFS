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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class NodeSummaryInfo {

    private final ServerState state;
    private final NodeState regionNodeState;
    private final NodeState dataNodeState;
    private final String hostName;
    private final String ip;
    private final double cpuUsage;
    private final double memUsage;
    private final double brfsDiskUsage;
    private final double systemDiskUsage;

    @JsonCreator
    public NodeSummaryInfo(
        @JsonProperty("state") ServerState state,
        @JsonProperty("regionNodeState") NodeState regionNodeState,
        @JsonProperty("dataNodeState") NodeState dataNodeState,
        @JsonProperty("hostName") String hostName,
        @JsonProperty("ip") String ip,
        @JsonProperty("cpuUsage") double cpuUsage,
        @JsonProperty("memUsage") double memUsage,
        @JsonProperty("brfsDiskUsage") double brfsDiskUsage,
        @JsonProperty("systemDiskUsage") double systemDiskUsage) {
        this.state = state;
        this.regionNodeState = regionNodeState;
        this.dataNodeState = dataNodeState;
        this.hostName = hostName;
        this.ip = ip;
        this.cpuUsage = cpuUsage;
        this.memUsage = memUsage;
        this.brfsDiskUsage = brfsDiskUsage;
        this.systemDiskUsage = systemDiskUsage;
    }

    @JsonProperty("state")
    public ServerState getState() {
        return state;
    }

    @JsonProperty("regionNodeState")
    public NodeState getRegionNodeState() {
        return regionNodeState;
    }

    @JsonProperty("dataNodeState")
    public NodeState getDataNodeState() {
        return dataNodeState;
    }

    @JsonProperty("hostName")
    public String getHostName() {
        return hostName;
    }

    @JsonProperty("ip")
    public String getIp() {
        return ip;
    }

    @JsonProperty("cpuUsage")
    public double getCpuUsage() {
        return cpuUsage;
    }

    @JsonProperty("memUsage")
    public double getMemUsage() {
        return memUsage;
    }

    @JsonProperty("brfsDiskUsage")
    public double getBrfsDiskUsage() {
        return brfsDiskUsage;
    }

    @JsonProperty("systemDiskUsage")
    public double getSystemDiskUsage() {
        return systemDiskUsage;
    }

    @Override
    public String toString() {
        return toStringHelper(this)
            .add("regionNodeState", regionNodeState)
            .add("dataNodeState", dataNodeState)
            .add("hostName", hostName)
            .add("ip", ip)
            .add("cpuUsage", cpuUsage)
            .add("memUsage", memUsage)
            .add("brfsDiskUsage", brfsDiskUsage)
            .add("systemDiskUsage", systemDiskUsage)
            .toString();
    }
}
