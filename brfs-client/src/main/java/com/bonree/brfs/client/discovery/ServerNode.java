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
package com.bonree.brfs.client.discovery;

import static com.google.common.base.MoreObjects.toStringHelper;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ServerNode {
    private final String serviceGroup;
    private final String serviceId;
    private final String host;
    private final int port;
    private final int extraPort;
    
    @JsonCreator
    public ServerNode(
            @JsonProperty("serviceGroup") String serviceGroup,
            @JsonProperty("serviceId") String serviceId,
            @JsonProperty("host") String host,
            @JsonProperty("port") int port,
            @JsonProperty("extraPort") int extraPort) {
        this.serviceGroup = serviceGroup;
        this.serviceId = serviceId;
        this.host = host;
        this.port = port;
        this.extraPort = extraPort;
    }

    @JsonProperty("serviceGroup")
    public String getServiceGroup() {
        return serviceGroup;
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

    @JsonProperty("extraPort")
    public int getExtraPort() {
        return extraPort;
    }

    @Override
    public boolean equals(Object obj) {
        if(obj == this) {
            return true;
        }
        
        if(!(obj instanceof ServerNode)) {
            return false;
        }
        
        ServerNode node = (ServerNode) obj;
        return Objects.equals(serviceGroup, node.serviceGroup)
                && Objects.equals(serviceId, node.serviceId)
                && Objects.equals(host, node.host)
                && port == node.port
                && extraPort == node.extraPort;
    }

    @Override
    public String toString() {
        return toStringHelper(getClass())
                .add("serviceGroup", serviceGroup)
                .add("serviceId", serviceId)
                .add("host", host)
                .add("port", port)
                .add("extraPort", extraPort)
                .toString();
    }
}
