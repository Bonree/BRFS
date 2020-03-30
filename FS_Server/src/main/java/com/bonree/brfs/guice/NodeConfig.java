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
package com.bonree.brfs.guice;

import com.bonree.brfs.configuration.Configs;
import com.bonree.brfs.configuration.units.DataNodeConfigs;
import com.bonree.brfs.configuration.units.RegionNodeConfigs;
import com.fasterxml.jackson.annotation.JsonProperty;

public class NodeConfig {
    @JsonProperty("service.host")
    public String host = Configs.getConfiguration().GetConfig(RegionNodeConfigs.CONFIG_HOST);
    
    @JsonProperty("service.port")
    public int port = Configs.getConfiguration().GetConfig(RegionNodeConfigs.CONFIG_PORT);
    
    @JsonProperty("file.server.port")
    public int sslPort = Configs.getConfiguration().GetConfig(DataNodeConfigs.CONFIG_FILE_PORT);

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getSslPort() {
        return sslPort;
    }

    public void setSslPort(int sslPort) {
        this.sslPort = sslPort;
    }
}
