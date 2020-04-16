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

public class SecondServerID {
    private final String serviceId;
    private final String host;
    private final int port;
    private final int readPort;
    private final int storageRegionIndex;
    private final String secondServerId;
    private final String storagePath;
    
    @JsonCreator
    public SecondServerID(
            @JsonProperty("serviceId") String serviceId,
            @JsonProperty("host") String host,
            @JsonProperty("port") int port,
            @JsonProperty("readPort") int readPort,
            @JsonProperty("storageRegionIndex") int storageRegionIndex,
            @JsonProperty("secondServerId") String secondServerId,
            @JsonProperty("storagePath") String storagePath) {
        this.serviceId = serviceId;
        this.host = host;
        this.port = port;
        this.readPort = readPort;
        this.storageRegionIndex = storageRegionIndex;
        this.secondServerId = secondServerId;
        this.storagePath = storagePath;
    }

    @JsonProperty("serviceId")
    public String getServiceId() {
        return serviceId;
    }
    
    @JsonProperty("host")
    public String getHost() {
        return host;
    }
    
    @JsonProperty("port")
    public int getPort() {
        return port;
    }
    
    @JsonProperty("readPort")
    public int getReadPort() {
        return readPort;
    }

    @JsonProperty("storageRegionIndex")
    public int getStorageRegionIndex() {
        return storageRegionIndex;
    }

    @JsonProperty("secondServerId")
    public String getSecondServerId() {
        return secondServerId;
    }

    @JsonProperty("storagePath")
    public String getStoragePath() {
        return storagePath;
    }
    
    @Override
    public String toString() {
        return toStringHelper(getClass())
                .add("serviceId", serviceId)
                .add("host", host)
                .add("port", port)
                .add("readPort", readPort)
                .add("storageRegionIndex", storageRegionIndex)
                .add("secondServerId", secondServerId)
                .add("storagePath", storagePath)
                .toString();
    }
}
